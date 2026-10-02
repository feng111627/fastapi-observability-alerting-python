"""请求级指标注册表。

按 request_id 幂等记录每次请求的耗时、状态码、错误信息，
支持采样开关；同一 request_id 重复上报不会重复计数。

口径约定（详见 docs/observability.md）：
- total：去重后的请求总数（无论是否采样）；
- sampled：其中被采样、进入统计口径的请求数；
- error_count / sum_duration_ms / by_status / avg_duration_ms
  均只统计 sampled=True 的记录，未采样记录不污染统计口径；
- 同一 request_id 以第一次上报为准（first-write-wins），
  之后即使采样开关或字段发生变化也不会引起指标漂移。
"""

import threading
import time
from dataclasses import dataclass, field


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


class MetricsRegistry:
    """线程安全的指标注册表。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._records: dict[str, RequestRecord] = {}

    def record(
        self,
        request_id: str,
        trace_id: str,
        duration_ms: float,
        status_code: int,
        error: str | None = None,
        sampled: bool = True,
        now: float | None = None,
    ) -> bool:
        """幂等记录一条请求指标。

        返回 True 表示首次记录、True 已计入；False 表示 request_id 重复、
        本次上报被忽略（first-write-wins，防止重复计数与指标漂移）。
        """
        if not isinstance(request_id, str) or not request_id:
            raise ValueError("request_id 必须是非空字符串")
        if duration_ms < 0:
            raise ValueError("duration_ms 不能为负数")

        record = RequestRecord(
            request_id=request_id,
            trace_id=trace_id,
            duration_ms=float(duration_ms),
            status_code=int(status_code),
            error=error,
            sampled=bool(sampled),
            recorded_at=time.monotonic() if now is None else now,
        )
        with self._lock:
            # setdefault 保证并发重复上报时只有一条记录生效。
            existed = self._records.setdefault(request_id, record)
            return existed is record

    def get(self, request_id: str) -> RequestRecord | None:
        with self._lock:
            return self._records.get(request_id)

    def snapshot(self) -> MetricSnapshot:
        """对当前已记录指标取一致快照（持锁聚合，避免并发上报导致口径撕裂）。"""
        with self._lock:
            by_status: dict[int, int] = {}
            sampled = 0
            error_count = 0
            total_duration = 0.0
            for rec in self._records.values():
                if not rec.sampled:
                    continue
                sampled += 1
                total_duration += rec.duration_ms
                by_status[rec.status_code] = by_status.get(rec.status_code, 0) + 1
                if rec.error is not None or rec.status_code >= 500:
                    error_count += 1
            return MetricSnapshot(
                total=len(self._records),
                sampled=sampled,
                error_count=error_count,
                sum_duration_ms=total_duration,
                by_status=dict(by_status),
            )

    def records(self) -> list[RequestRecord]:
        with self._lock:
            return list(self._records.values())

    def reset(self) -> None:
        with self._lock:
            self._records.clear()
