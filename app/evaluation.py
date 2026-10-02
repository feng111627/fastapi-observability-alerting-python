"""窗口指标聚合与告警评估编排。

把 :class:`MetricsRegistry` 的已记录指标按规则窗口切片，计算
error_rate / error_count / latency_avg / latency_p95，再交给
:class:`AlertEngine` 判定；评估前先做超时收敛。
所有时间戳统一来自可注入时钟（默认 ``time.monotonic``）。

来源维度：``by_source=True`` 的规则按窗口内样本的生效来源分组，
每个来源独立计算指标并交给告警引擎判定（各自独立的告警周期）；
窗口内没有样本的来源本轮不产生观测，其在飞告警随窗口到期自然收敛，
不影响其它来源。``by_source=False`` 的老规则仍按全局口径评估。
"""

import math
import time
from collections.abc import Callable

from app.alerts import AlertEngine, AlertState
from app.metrics import MetricsRegistry, RequestRecord
from app.rules import AlertRule, RuleStore


def percentile_95(durations: list[float]) -> float:
    """最近秩（nearest-rank）口径的 P95；空序列返回 0.0。"""
    if not durations:
        return 0.0
    ordered = sorted(durations)
    rank = max(1, math.ceil(0.95 * len(ordered)))
    return ordered[rank - 1]


def compute_metric(metric: str, samples: list[RequestRecord]) -> tuple[float, int]:
    """对一组窗口样本计算指标值，返回 (指标值, 样本数)。"""
    n = len(samples)
    if n == 0:
        return 0.0, 0
    if metric == "error_rate":
        errors = sum(1 for r in samples if r.error is not None or r.status_code >= 500)
        return errors / n, n
    if metric == "error_count":
        errors = sum(1 for r in samples if r.error is not None or r.status_code >= 500)
        return float(errors), n
    if metric == "latency_avg":
        return sum(r.duration_ms for r in samples) / n, n
    if metric == "latency_p95":
        return percentile_95([r.duration_ms for r in samples]), n
    return 0.0, n


#: 一次评估的变化记录：(规则, 变化类型, 指标值, 来源)；来源为 None 表示全局口径。
EvalChange = tuple[AlertRule, str, float, "str | None"]


class Evaluator:
    """读取注册表 -> 窗口聚合 -> 逐规则评估 -> 收敛。"""

    def __init__(
        self,
        registry: MetricsRegistry,
        engine: AlertEngine,
        store: RuleStore,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._registry = registry
        self._engine = engine
        self._store = store
        self._clock = clock

    def _window_samples(self, rule: AlertRule, now: float) -> list[RequestRecord]:
        cutoff = now - rule.window_seconds
        return [
            rec
            for rec in self._registry.records()
            if rec.sampled and rec.recorded_at >= cutoff
        ]

    def compute(self, rule: AlertRule, now: float | None = None) -> tuple[float, int]:
        """全局口径：返回 (指标值, 窗口内样本数)。无样本时样本数为 0，不应触发。"""
        if now is None:
            now = self._clock()
        return compute_metric(rule.metric, self._window_samples(rule, now))

    def compute_by_source(
        self, rule: AlertRule, now: float | None = None
    ) -> dict[str, tuple[float, int]]:
        """来源口径：窗口样本按生效来源分组，逐来源返回 (指标值, 样本数)。"""
        if now is None:
            now = self._clock()
        grouped: dict[str, list[RequestRecord]] = {}
        for rec in self._window_samples(rule, now):
            grouped.setdefault(rec.source, []).append(rec)
        return {src: compute_metric(rule.metric, recs) for src, recs in grouped.items()}

    def evaluate_once(self, now: float | None = None) -> list[EvalChange]:
        """对当前版本的每条规则评估一轮，返回 (规则, 变化类型, 指标值, 来源) 列表。"""
        if now is None:
            now = self._clock()
        results: list[EvalChange] = []
        # 评估前先收敛过期告警，避免“收敛后同一事件又被刷新”的竞态。
        self._engine.sweep(now)
        for rule in self._store.current().rules:
            if rule.by_source:
                for source, (value, n) in sorted(self.compute_by_source(rule, now).items()):
                    kind = self._engine.evaluate(
                        rule, value, firing=(n > 0 and value >= rule.threshold),
                        now=now, source=source,
                    )
                    results.append((rule, kind, value, source))
            else:
                value, n = self.compute(rule, now)
                kind = self._engine.evaluate(
                    rule, value, firing=(n > 0 and value >= rule.threshold), now=now
                )
                results.append((rule, kind, value, None))
        return results

    def sweep(self, now: float | None = None) -> list[AlertState]:
        if now is None:
            now = self._clock()
        return self._engine.sweep(now)
