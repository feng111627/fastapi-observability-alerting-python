"""请求级指标注册表（含来源维度）。

按 request_id 幂等记录每次请求的耗时、状态码、错误信息与来源，
支持采样开关；同一 request_id 重复上报不会重复计数。

口径约定（详见 docs/observability.md）：
- total：去重后的请求总数（无论是否采样）；
- sampled：其中被采样、进入统计口径的请求数；
- error_count / sum_duration_ms / by_status / avg_duration_ms
  均只统计 sampled=True 的记录，未采样记录不污染统计口径；
- 同一 request_id 以第一次上报为准（first-write-wins），
  之后即使采样开关、来源或字段发生变化也不会引起指标漂移。

来源维度：
- 每条记录归属且仅归属一个“生效来源”：未携带或空白来源归一化为
  ``default``；来源名去首尾空白并截断到 ``MAX_SOURCE_LEN``；
- 全局快照 = 全部记录聚合；按来源快照 = 该来源记录聚合；
  各生效来源快照之和恒等于全局快照（每条记录只落一个桶）；
- 生效来源数量上限为 ``max_sources``：超出后新来源归并到保留桶
  ``__overflow__``（不丢弃数据，全局口径不失真），归并次数与被归并的
  原始来源名通过 :meth:`source_stats` 可观察。
"""

import threading
import time
from dataclasses import dataclass, field

#: 未携带/空白来源的默认归并桶。
DEFAULT_SOURCE = "default"
#: 来源数量超限后的归并桶（保留名，客户端同名上报视同该桶）。
OVERFLOW_SOURCE = "__overflow__"
#: 来源名最大长度，超出截断，防止超长字符串撑爆内存。
MAX_SOURCE_LEN = 128
#: 默认生效来源数量上限（含 default，不含 __overflow__ 保留桶）。
DEFAULT_MAX_SOURCES = 100
#: 被归并的原始来源名样本集合上限（仅用于观察，防无界增长）。
_OVERFLOW_SAMPLE_CAP = 1024


def normalize_source(source: str | None) -> str:
    """归一化来源名：空/空白 -> default；去首尾空白；超长截断。"""
    if not isinstance(source, str):
        return DEFAULT_SOURCE
    cleaned = source.strip()
    if not cleaned:
        return DEFAULT_SOURCE
    return cleaned[:MAX_SOURCE_LEN]


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
    """某一时刻的指标快照（一致视图）。"""

    total: int
    sampled: int
    error_count: int
    sum_duration_ms: float
    by_status: dict[int, int] = field(default_factory=dict)

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
class SourceStats:
    """来源维度元信息：上限、当前来源数与溢出归并的可观察口径。"""

    max_sources: int
    source_count: int
    sources: tuple[str, ...]
    overflow_source: str
    overflowed: bool
    overflow_records: int
    overflow_sources_seen: int
    overflow_samples_truncated: bool


class MetricsRegistry:
    """线程安全的指标注册表（带来源维度与数量上限）。"""

    def __init__(self, max_sources: int = DEFAULT_MAX_SOURCES) -> None:
        if max_sources < 1:
            raise ValueError("max_sources 必须 >= 1")
        self._max_sources = int(max_sources)
        self._lock = threading.RLock()
        self._records: dict[str, RequestRecord] = {}
        # 生效来源集合（含 default；超限后含 __overflow__ 保留桶）。
        self._sources: set[str] = set()
        # 溢出可观察口径：归并的记录条数 + 被归并的原始来源名样本。
        self._overflow_records = 0
        self._overflow_sources: set[str] = set()
        self._overflow_samples_truncated = False

    @property
    def max_sources(self) -> int:
        return self._max_sources

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
        本次上报被忽略（first-write-wins，防止重复计数与指标漂移；
        重复上报也不会重复计入溢出统计）。
        """
        if not isinstance(request_id, str) or not request_id:
            raise ValueError("request_id 必须是非空字符串")
        if duration_ms < 0:
            raise ValueError("duration_ms 不能为负数")

        effective = normalize_source(source)
        with self._lock:
            if request_id in self._records:
                return False
            if effective not in self._sources and effective != OVERFLOW_SOURCE:
                if len(self._sources) >= self._max_sources:
                    # 超限归并：数据不丢弃，落入保留桶，全局口径不变。
                    self._overflow_records += 1
                    if len(self._overflow_sources) < _OVERFLOW_SAMPLE_CAP:
                        self._overflow_sources.add(effective)
                    else:
                        self._overflow_samples_truncated = True
                    effective = OVERFLOW_SOURCE
                self._sources.add(effective)
            elif effective == OVERFLOW_SOURCE and effective not in self._sources:
                self._sources.add(effective)

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
            return True

    def get(self, request_id: str) -> RequestRecord | None:
        with self._lock:
            return self._records.get(request_id)

    @staticmethod
    def _aggregate(records: list[RequestRecord]) -> MetricSnapshot:
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
        )

    def snapshot(self) -> MetricSnapshot:
        """全局一致快照（持锁聚合，避免并发上报导致口径撕裂）。"""
        with self._lock:
            return self._aggregate(list(self._records.values()))

    def snapshot_by_source(self, source: str | None) -> MetricSnapshot:
        """指定来源的一致快照；来源名先归一化，未知来源返回全零快照。"""
        effective = normalize_source(source)
        with self._lock:
            return self._aggregate(
                [rec for rec in self._records.values() if rec.source == effective]
            )

    def snapshot_all_sources(self) -> dict[str, MetricSnapshot]:
        """所有生效来源各自的一致快照（同一临界区内聚合，可对账）。"""
        with self._lock:
            grouped: dict[str, list[RequestRecord]] = {
                src: [] for src in self._sources
            }
            for rec in self._records.values():
                grouped[rec.source].append(rec)
            return {src: self._aggregate(recs) for src, recs in grouped.items()}

    def sources(self) -> list[str]:
        with self._lock:
            return sorted(self._sources)

    def source_stats(self) -> SourceStats:
        """来源上限与溢出归并的可观察口径。"""
        with self._lock:
            return SourceStats(
                max_sources=self._max_sources,
                source_count=len(self._sources),
                sources=tuple(sorted(self._sources)),
                overflow_source=OVERFLOW_SOURCE,
                overflowed=self._overflow_records > 0,
                overflow_records=self._overflow_records,
                overflow_sources_seen=len(self._overflow_sources),
                overflow_samples_truncated=self._overflow_samples_truncated,
            )

    def records(self) -> list[RequestRecord]:
        with self._lock:
            return list(self._records.values())

    def reset(self) -> None:
        with self._lock:
            self._records.clear()
            self._sources.clear()
            self._overflow_records = 0
            self._overflow_sources.clear()
            self._overflow_samples_truncated = False
