"""基于时间窗口的告警判定、合并去重与收敛。

状态机（指纹 = 规则名）：
    首次越界 -> PENDING；持续 for_s -> FIRING(level=1)（计入 fired）
    FIRING 持续越界：每经过一个升级周期 level+1（计入 upgraded），不产生新告警
    FIRING 期间重复越界：只刷新 last_seen，受 cooldown_s 合并，绝不重复告警
    FIRING 后 resolve_s 内未再越界 -> RESOLVED（计入 resolved）
    RESOLVED 后 cooldown_s 内再次越界：静默 reopen；超过 cooldown 才算一条新告警
    PENDING 未持续即恢复：静默清除（不产生告警）
所有状态变更都在同一把锁内串行完成，并发上报不会重复或漏报。
"""
from __future__ import annotations

import operator
import threading
from collections import deque

from app.models import Alert, AlertRule, AlertState, EvaluationResult, RequestSample

_MAX_LEVEL = 5
_OPS = {
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
}


def _sample_value(rule: AlertRule, sample: RequestSample) -> float:
    """把一次请求折算成规则关心的窗口事件值。"""
    if rule.metric == "latency_ms":
        return float(sample.duration_ms)
    return 1.0 if (sample.status_code >= 400 or bool(sample.error)) else 0.0


class SlidingWindow:
    """固定时长滑动窗口，元素为 (时间戳, 值)。"""

    def __init__(self, window_s: float) -> None:
        self._lock = threading.Lock()
        self._window_s = float(window_s)
        self._events: deque[tuple[float, float]] = deque()

    def resize(self, window_s: float) -> None:
        with self._lock:
            self._window_s = float(window_s)

    def add(self, ts: float, value: float) -> None:
        with self._lock:
            self._events.append((ts, float(value)))

    def prune(self, now: float) -> None:
        with self._lock:
            cutoff = now - self._window_s
            while self._events and self._events[0][0] < cutoff:
                self._events.popleft()

    def _snapshot(self, now: float) -> list[float]:
        cutoff = now - self._window_s
        return [v for ts, v in self._events if ts >= cutoff]

    def latest(self, now: float) -> float | None:
        values = self._snapshot(now)
        return max(values) if values else None

    def ratio(self, now: float) -> float | None:
        values = self._snapshot(now)
        if not values:
            return None
        return sum(1 for v in values if v > 0.0) / len(values)

    def truthy_count(self, now: float) -> int:
        return sum(1 for v in self._snapshot(now) if v > 0.0)


class AlertManager:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._rules: dict[str, AlertRule] = {}
        self._windows: dict[str, SlidingWindow] = {}
        self._alerts: dict[str, Alert] = {}

    # ---- 规则管理 -------------------------------------------------------
    def update_rules(self, rules: list[AlertRule]) -> None:
        """原子替换规则集；同名规则保留窗口与告警状态。"""
        with self._lock:
            new_rules = {r.name: r for r in rules}
            for name, rule in new_rules.items():
                win = self._windows.get(name)
                if win is None:
                    self._windows[name] = SlidingWindow(rule.window_s)
                else:
                    win.resize(rule.window_s)
            self._windows = {n: w for n, w in self._windows.items() if n in new_rules}
            self._rules = new_rules

    def get_rules(self) -> list[AlertRule]:
        with self._lock:
            return list(self._rules.values())

    # ---- 摄入与评估 -----------------------------------------------------
    def ingest(self, sample: RequestSample) -> EvaluationResult:
        with self._lock:
            now = sample.timestamp
            result = EvaluationResult()
            for name, rule in self._rules.items():
                self._windows[name].add(now, _sample_value(rule, sample))
                self._evaluate_rule(rule, now, result)
            return result

    def evaluate_at(self, now: float) -> EvaluationResult:
        """推进时间：清理窗口、判定收敛（测试与后台定时器使用）。"""
        with self._lock:
            result = EvaluationResult()
            for win in self._windows.values():
                win.prune(now)
            for name, rule in self._rules.items():
                self._evaluate_rule(rule, now, result)
            return result

    def _rule_value(self, rule: AlertRule, now: float) -> float | None:
        win = self._windows[rule.name]
        if rule.metric == "error_rate":
            return win.ratio(now)
        if rule.metric == "error_count":
            return float(win.truthy_count(now))
        return win.latest(now)  # latency_ms：窗口内最大单次耗时

    def _evaluate_rule(self, rule: AlertRule, now: float, result: EvaluationResult) -> None:
        value = self._rule_value(rule, now)
        breaching = value is not None and _OPS[rule.operator](value, rule.threshold)
        alert = self._alerts.get(rule.name)

        if breaching:
            if alert is None or alert.state == AlertState.RESOLVED and (
                alert.resolved_at is None or now - alert.resolved_at >= rule.cooldown_s
            ):
                alert = self._new_alert(rule, now, value)  # type: ignore[arg-type]
                self._alerts[rule.name] = alert
                result.fired.append(alert) if alert.state == AlertState.FIRING else None
            elif alert is not None and alert.state == AlertState.RESOLVED:
                # cooldown 内：静默 reopen 同一条，不重复发告警，升级计时重新开始
                alert.state = AlertState.FIRING
                alert.level = 1
                alert.fired_at = now
                alert.resolved_at = None
            self._refresh(rule, alert, now, value, result)  # type: ignore[arg-type]
            return

        # 未越界
        if alert is None:
            return
        if alert.state == AlertState.PENDING:
            del self._alerts[rule.name]  # 未持续，静默清除
            return
        if alert.state == AlertState.FIRING and now - alert.last_seen >= rule.resolve_s:
            alert.state = AlertState.RESOLVED
            alert.resolved_at = now
            result.resolved.append(alert)

    def _new_alert(self, rule: AlertRule, now: float, value: float) -> Alert:
        """越界后创建追踪状态；for_s==0 直接 FIRING，否则 PENDING。"""
        if rule.for_s <= 0:
            return Alert(
                rule_name=rule.name, fingerprint=rule.name,
                state=AlertState.FIRING, level=1,
                first_seen=now, last_seen=now, last_value=value, fired_at=now,
            )
        return Alert(
            rule_name=rule.name, fingerprint=rule.name,
            state=AlertState.PENDING, level=0,
            first_seen=now, last_seen=now, last_value=value,
        )

    def _refresh(self, rule: AlertRule, alert: Alert, now: float,
                 value: float, result: EvaluationResult) -> None:
        alert.last_seen = now
        alert.last_value = value
        alert.count += 1
        if alert.state == AlertState.PENDING and now - alert.first_seen >= rule.for_s:
            alert.state = AlertState.FIRING
            alert.level = 1
            alert.fired_at = now
            result.fired.append(alert)
            return
        if alert.state == AlertState.FIRING:
            step = rule.for_s if rule.for_s > 0 else rule.cooldown_s
            if step > 0 and alert.fired_at is not None:
                expected = alert.level * step
                if alert.level < _MAX_LEVEL and now - alert.fired_at >= expected:
                    alert.level += 1
                    result.upgraded.append(alert)

    # ---- 查询 -----------------------------------------------------------
    def list_alerts(self, state: AlertState | None = None) -> list[Alert]:
        with self._lock:
            alerts = list(self._alerts.values())
        if state is not None:
            alerts = [a for a in alerts if a.state == state]
        return alerts

    def reset(self) -> None:
        with self._lock:
            self._rules = {}
            self._windows = {}
            self._alerts = {}
