"""请求级指标注册表（带来源维度与来源基数上限）。

按 request_id 幂等记录每次请求的耗时、状态码、错误信息与来源，
支持采样开关；同一 request_id 重复上报不会重复计数。

口径约定（详见 docs/observability.md）：
- total：去重后的请求总数（无论是否采样）；
- sampled：其中被采样、进入统计口径的请求数；
- error_count / sum_duration_ms / by_status / avg_duration_ms
  均只统计 sampled=True 的记录，未采样记录不污染统计口径；
- 同一 request_id 以第一次上报为准（first-write-wins），
  之后即使采样开关或字段发生变化也不会引起指标漂移。

来源维度：
- 每条记录归属且仅归属一个有效来源；未带来源的上报落入 ``DEFAULT_SOURCE``；
- 全局快照 == 各有效来源快照之和（由“每条记录恰好一个来源”保证，
  不会重复计数也不会漏计）；
- 不同来源的数量受 ``max_sources`` 上限约束：超限的新来源被归并到
  ``OVERFLOW_SOURCE`` 桶，已统计数字不受影响，归并情况可通过
  :meth:`MetricsRegistry.cardinality` 观察。
"""

import threading
import time
from dataclasses import dataclass, field

#: 未携带来源时的默认归属。
DEFAULT_SOURCE = "default"
#: 来源数量超限后，新来源统一归并到该桶。
OVERFLOW_SOURCE = "__overflow__"
#: 来源名最大长度（超出部分截断，保证单个键的内存有界）。
MAX_SOURCE_LEN = 64
#: 溢出桶中保留的不同原始来源名样本数上限（仅用于观测，不影响统计）。
_OVERFLOW_SAMPLE_CAP = 64


def normalize_source(raw: str | None) -> str:
    """把外部传入的来源名归一化为注册表内部使用的键。

    None / 空白 -> 默认来源；其余去首尾空白并截断到 ``MAX_SOURCE_LEN``。
    """
    if raw is None:
        return DEFAULT_SOURCE
    name = raw.strip()
    if not name:
        return DEFAULT_SOURCE
    return name[:MAX_SOURCE_LEN]


@dataclass(frozen=True)
class RequestRecord:
    """单次请求的指标记录（不可变，防止发布后被外部改动）。"""

    request_id: str
    trace_id: str
    duration_ms: float
    status_code: int
    error: str | None
    sampled: bool
    recorded_at: float
    source: str = DEFAULT_SOURCE


@dataclass(frozen=True)
class MetricSnapshot:
    """某一时刻的指标快照（一致视图）。

    ``source`` 为 None 表示全局汇总快照，否则为对应来源的快照。
    """

    total: int
    sampled: int
    error_count: int
    sum_duration_ms: float
    by_status: dict[int, int] = field(default_factory=dict)
    source: str | None = None

    @property
    def avg_duration_ms(self) -> float:
        if self.sampled == 0:
            return 0.0
        return self.sum_duration_ms / self.sampled

    @property
    def error_rate(self) -> float:
        if self.sampled == 0:
            return 0.0
        return self.error_count / self.sampled


@dataclass(frozen=True)
class CardinalityInfo:
    """来源基数观测信息。"""

    max_sources: int
    active_sources: int
    overflowed: bool
    overflow_records: int
    overflow_sources_sample: tuple[str, ...] = ()


class MetricsRegistry:
    """线程安全的指标注册表（带来源维度与来源基数上限）。"""

    def __init__(self, max_sources: int = 128) -> None:
        if max_sources < 1:
            raise ValueError("max_sources 必须 >= 1")
        self._max_sources = int(max_sources)
        self._lock = threading.RLock()
        self._records: dict[str, RequestRecord] = {}
        # 原始来源 -> 有效来源。只登记前 max_sources 个不同来源，
        # 之后出现的新来源不再入表（保持内存有界），直接归并到溢出桶。
        self._source_map: dict[str, str] = {}
        self._overflow_records = 0
        self._overflow_sources_sample: set[str] = set()

    def _effective_source(self, raw: str | None) -> str:
        """在锁内把原始来源映射为有效来源（必要时登记或归并）。"""
        name = normalize_source(raw)
        effective = self._source_map.get(name)
        if effective is not None:
            return effective
        if len(self._source_map) < self._max_sources:
            self._source_map[name] = name
            return name
        # 超限：归并到溢出桶，并留下可观测痕迹（样本集有界）。
        if len(self._overflow_sources_sample) < _OVERFLOW_SAMPLE_CAP:
            self._overflow_sources_sample.add(name)
        return OVERFLOW_SOURCE

    def record(
        self,
        request_id: str,
        trace_id: str,
        duration_ms: float,
        status_code: int,
        error: str | None = None,
        sampled: bool = True,
        now: float | None = None,
        source: str | None = None,
    ) -> bool:
        """幂等记录一条请求指标。

        返回 True 表示首次记录、已计入；False 表示 request_id 重复、
        本次上报被忽略（first-write-wins，防止重复计数与指标漂移）。
        """
        if not isinstance(request_id, str) or not request_id:
            raise ValueError("request_id 必须是非空字符串")
        if duration_ms < 0:
            raise ValueError("duration_ms 不能为负数")

        with self._lock:
            # 先查重再登记来源：重复上报不得消耗来源基数配额，
            # 也不得影响溢出计数（保证重复上报不改变任何口径）。
            if request_id in self._records:
                return False
            effective = self._effective_source(source)
            record = RequestRecord(
                request_id=request_id,
                trace_id=trace_id,
                duration_ms=float(duration_ms),
                status_code=int(status_code),
                error=error,
                sampled=bool(sampled),
                recorded_at=time.monotonic() if now is None else now,
                source=effective,
            )
            self._records[request_id] = record
            if effective == OVERFLOW_SOURCE:
                self._overflow_records += 1
            return True

    def get(self, request_id: str) -> RequestRecord | None:
        with self._lock:
            return self._records.get(request_id)

    def _aggregate(self, records: list[RequestRecord], source: str | None) -> MetricSnapshot:
        by_status: dict[int, int] = {}
        sampled = 0
        error_count = 0
        total_duration = 0.0
        for rec in records:
            if not rec.sampled:
                continue
            sampled += 1
            total_duration += rec.duration_ms
            by_status[rec.status_code] = by_status.get(rec.status_code, 0) + 1
            if rec.error is not None or rec.status_code >= 500:
                error_count += 1
        return MetricSnapshot(
            total=len(records),
            sampled=sampled,
            error_count=error_count,
            sum_duration_ms=total_duration,
            by_status=by_status,
            source=source,
        )

    def snapshot(self, source: str | None = None) -> MetricSnapshot:
        """对当前已记录指标取一致快照（持锁聚合，避免并发上报导致口径撕裂）。

        ``source=None`` 返回全局汇总；给定来源时只统计该来源的记录。
        查询未携带/空白来源时按默认来源处理；查询一个被归并的原始来源
        时返回空快照（其数据只在 ``OVERFLOW_SOURCE`` 桶中可见）。
        """
        with self._lock:
            if source is None:
                return self._aggregate(list(self._records.values()), None)
            name = normalize_source(source)
            if name == OVERFLOW_SOURCE:
                effective = OVERFLOW_SOURCE
            else:
                effective = self._source_map.get(name)
            if effective is None:
                # 未知来源（或已被归并的原始来源）：空快照，不报错。
                return self._aggregate([], name)
            records = [r for r in self._records.values() if r.source == effective]
            return self._aggregate(records, effective)

    def sources(self) -> list[str]:
        """当前全部有效来源（含默认来源与溢出桶，若存在记录）。"""
        with self._lock:
            names = {r.source for r in self._records.values()}
            # 已登记但暂无记录的来源也列出，保证上限行为可观察。
            names.update(self._source_map.values())
            return sorted(names)

    def cardinality(self) -> CardinalityInfo:
        """来源基数观测：上限、当前来源数、是否发生归并及归并量。"""
        with self._lock:
            return CardinalityInfo(
                max_sources=self._max_sources,
                active_sources=len(self._source_map),
                overflowed=self._overflow_records > 0,
                overflow_records=self._overflow_records,
                overflow_sources_sample=tuple(sorted(self._overflow_sources_sample)),
            )

    def records(self) -> list[RequestRecord]:
        with self._lock:
            return list(self._records.values())

    def reset(self) -> None:
        with self._lock:
            self._records.clear()
            self._source_map.clear()
            self._overflow_records = 0
            self._overflow_sources_sample.clear()
