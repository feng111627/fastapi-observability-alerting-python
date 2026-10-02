"""把指标、告警、规则装配在一起的门面。

上报流程在同一把服务级锁内串行完成“指标去重登记 + 告警评估”，
保证并发上报时指标计数与告警结论严格一致，不会重复或互相污染。
"""
from __future__ import annotations

import threading
import time

from app.alerts import AlertManager
from app.metrics import MetricsRegistry
from app.models import AlertState, EvaluationResult, MetricSnapshot, RequestSample
from app.rules import RuleLoadResult, RuleStore


class ObservabilityService:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.metrics = MetricsRegistry()
        self.alerts = AlertManager()
        self.rules = RuleStore()

    def report(self, sample: RequestSample) -> tuple[bool, EvaluationResult]:
        """上报样本，返回 (是否被计数, 告警变化)。

        重复 request_id 或采样关闭时：不计数、也不进入告警评估，
        保证指标与告警口径一致。
        """
        if sample.timestamp <= 0:
            sample = RequestSample(
                request_id=sample.request_id,
                timestamp=time.time(),
                duration_ms=sample.duration_ms,
                status_code=sample.status_code,
                error=sample.error,
                method=sample.method,
                path=sample.path,
            )
        with self._lock:
            counted = self.metrics.record(sample)
            if not counted:
                return False, EvaluationResult()
            return True, self.alerts.ingest(sample)

    def reload_rules(self, payload: list[dict]) -> RuleLoadResult:
        """热更新：成功（至少一条有效规则）才同步给 AlertManager。"""
        with self._lock:
            result = self.rules.load(payload)
            if result.version > 0 and result.loaded:
                self.alerts.update_rules(result.loaded)
            return result

    def snapshot(self) -> MetricSnapshot:
        return self.metrics.snapshot()

    def tick(self, now: float | None = None) -> EvaluationResult:
        """推进告警时间（收敛判定）。"""
        return self.alerts.evaluate_at(now if now is not None else time.time())

    def sampling_off(self) -> None:
        self.metrics.set_sampling(False)

    def sampling_on(self) -> None:
        self.metrics.set_sampling(True)

    def reset(self) -> None:
        """仅供测试：清空全部状态。"""
        with self._lock:
            self.metrics.reset()
            self.alerts.reset()
            self.rules = RuleStore()


# 默认规则：错误率 >50%（持续 10s 升级）、单次耗时 >1000ms、窗口内错误数 >=5
DEFAULT_RULES: list[dict] = [
    {"name": "high_error_rate", "metric": "error_rate", "operator": ">",
     "threshold": 0.5, "window_s": 60, "for_s": 10, "cooldown_s": 30, "resolve_s": 120},
    {"name": "slow_request", "metric": "latency_ms", "operator": ">",
     "threshold": 1000, "window_s": 60, "for_s": 0, "cooldown_s": 30, "resolve_s": 120},
    {"name": "error_burst", "metric": "error_count", "operator": ">=",
     "threshold": 5, "window_s": 60, "for_s": 0, "cooldown_s": 30, "resolve_s": 120},
]


def build_default_service() -> ObservabilityService:
    svc = ObservabilityService()
    svc.reload_rules(DEFAULT_RULES)
    return svc


service_singleton = build_default_service()
