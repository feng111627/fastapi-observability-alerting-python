"""窗口指标聚合与告警评估编排（支持来源维度）。

把 :class:`MetricsRegistry` 的已记录指标按规则窗口切片，计算
error_rate / error_count / latency_avg / latency_p95，再交给
:class:`AlertEngine` 判定；评估前先做超时收敛。
所有时间戳统一来自可注入时钟（默认 ``time.monotonic``）。

来源维度语义：
- 规则不带 ``source``：按全局汇总口径评估（与历史版本行为一致）；
- 规则 ``source="*"``：对每个已知来源分别评估一次，同一条规则在
  不同来源上各自独立触发/合并/升级/收敛，互不影响；
- 规则指定具体来源：只统计该来源的样本。
"""

import math
import time
from collections.abc import Callable
from typing import NamedTuple

from app.alerts import AlertEngine, AlertState
from app.metrics import MetricsRegistry, RequestRecord
from app.rules import SOURCE_ALL, AlertRule, RuleStore


def percentile_95(durations: list[float]) -> float:
    """最近秩（nearest-rank）口径的 P95；空序列返回 0.0。"""
    if not durations:
        return 0.0
    ordered = sorted(durations)
    rank = max(1, math.ceil(0.95 * len(ordered)))
    return ordered[rank - 1]


class EvalResult(NamedTuple):
    """一次规则评估的结果；``source`` 为 None 表示全局口径。"""

    rule: AlertRule
    kind: str
    value: float
    source: str | None = None


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

    def _window_samples(
        self, rule: AlertRule, now: float, source: str | None = None
    ) -> list[RequestRecord]:
        cutoff = now - rule.window_seconds
        return [
            rec
            for rec in self._registry.records()
            if rec.sampled
            and rec.recorded_at >= cutoff
            and (source is None or rec.source == source)
        ]

    def compute(
        self, rule: AlertRule, now: float | None = None, source: str | None = None
    ) -> tuple[float, int]:
        """返回 (指标值, 窗口内样本数)。无样本时样本数为 0，不应触发。"""
        if now is None:
            now = self._clock()
        samples = self._window_samples(rule, now, source)
        n = len(samples)
        if n == 0:
            return 0.0, 0

        if rule.metric == "error_rate":
            errors = sum(
                1 for r in samples if r.error is not None or r.status_code >= 500
            )
            return errors / n, n
        if rule.metric == "error_count":
            errors = sum(
                1 for r in samples if r.error is not None or r.status_code >= 500
            )
            return float(errors), n
        if rule.metric == "latency_avg":
            return sum(r.duration_ms for r in samples) / n, n
        if rule.metric == "latency_p95":
            return percentile_95([r.duration_ms for r in samples]), n
        return 0.0, n

    def _evaluate_target(
        self, rule: AlertRule, now: float, source: str | None
    ) -> EvalResult:
        value, n = self.compute(rule, now, source=source)
        kind = self._engine.evaluate(
            rule, value, firing=(n > 0 and value >= rule.threshold), now=now, source=source
        )
        return EvalResult(rule, kind, value, source)

    def evaluate_once(self, now: float | None = None) -> list[EvalResult]:
        """对当前版本的每条规则评估一次。

        返回每次评估的 (规则, 变化类型, 指标值, 来源)；带 ``source="*"``
        的规则会展开为每个来源一条结果。
        """
        if now is None:
            now = self._clock()
        results: list[EvalResult] = []
        # 评估前先收敛过期告警，避免“收敛后同一事件又被刷新”的竞态。
        self._engine.sweep(now)
        for rule in self._store.current().rules:
            if rule.source == SOURCE_ALL:
                # 每个已知来源独立评估；来源集合有界（注册表基数上限）。
                for src in self._registry.sources():
                    results.append(self._evaluate_target(rule, now, src))
            elif rule.source is not None:
                results.append(self._evaluate_target(rule, now, rule.source))
            else:
                results.append(self._evaluate_target(rule, now, None))
        return results

    def sweep(self, now: float | None = None) -> list[AlertState]:
        if now is None:
            now = self._clock()
        return self._engine.sweep(now)
