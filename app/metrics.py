"""并发安全的请求级指标注册表。

口径见 docs/observability.md：
- 每个 request_id 全局只计一次，重复上报幂等丢弃；
- 采样关闭期间的新样本不计数；切换采样开关不会回溯、不产生漂移；
- 统计项：total、error_count（status_code >= 400 或带 error）、
  total_duration_ms（用于计算平均耗时）。
"""
from __future__ import annotations

import threading

from app.models import MetricSnapshot, RequestSample


def _is_error(sample: RequestSample) -> bool:
    return sample.status_code >= 400 or bool(sample.error)


class MetricsRegistry:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._seen: set[str] = set()
        self._sampling_on: bool = True
        self._total = 0
        self._error_count = 0
        self._total_duration_ms = 0.0
        self._last_request_id: str | None = None

    def record(self, sample: RequestSample) -> bool:
        """登记一个样本；request_id 重复或采样关闭时返回 False（未计数）。"""
        with self._lock:
            if not self._sampling_on:
                return False
            if sample.request_id in self._seen:
                return False
            self._seen.add(sample.request_id)
            self._total += 1
            if _is_error(sample):
                self._error_count += 1
            self._total_duration_ms += float(sample.duration_ms)
            self._last_request_id = sample.request_id
            return True

    def set_sampling(self, enabled: bool) -> None:
        """切换采样开关。只影响切换之后的上报，不动既有统计。"""
        with self._lock:
            self._sampling_on = bool(enabled)

    @property
    def sampling(self) -> bool:
        with self._lock:
            return self._sampling_on

    def is_duplicate(self, request_id: str) -> bool:
        with self._lock:
            return request_id in self._seen

    def snapshot(self) -> MetricSnapshot:
        with self._lock:
            return MetricSnapshot(
                total=self._total,
                error_count=self._error_count,
                total_duration_ms=self._total_duration_ms,
                last_request_id=self._last_request_id,
            )

    def reset(self) -> None:
        with self._lock:
            self._seen.clear()
            self._sampling_on = True
            self._total = 0
            self._error_count = 0
            self._total_duration_ms = 0.0
            self._last_request_id = None
