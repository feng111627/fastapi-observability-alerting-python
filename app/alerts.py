"""基于时间窗口的告警引擎。

职责与语义（详见 docs/observability.md）：
- 以规则名（by_source 规则再加上来源）为 fingerprint，同一规则同一来源
  同一时间只保留一条进行中的有效告警，窗口内重复触发合并计数，不会重复告警；
  不同来源的告警流完全独立：各自触发、合并、升级、收敛，互不影响；
- 首次越阈 -> ``triggered``（warning 或规则声明的初始级别）；
  持续触发达到 ``upgrade_after`` 次后升级为 critical -> ``upgraded``，
  之后的命中只合并计数 -> ``repeated``；
- 异常结束：在 ``window_seconds`` 内没有新的命中，:meth:`sweep`
  会把告警明确收敛为 ``resolved`` 并记录收敛时间；
- 所有判定在同一把锁内串行完成：并发上报下只有一个调用能得到
  ``triggered``，其余为 ``repeated``/``upgraded``，不重复、不漏记；
- 收敛后再次越阈视为新一轮异常，产生新的告警周期。
"""

import threading
from dataclasses import dataclass, replace

from app.rules import AlertRule

# 每个 fingerprint 最多保留的历史（已收敛）告警条数，防止无界增长。
_HISTORY_PER_KEY = 8
# 浮点比较容差：避免 20.0-9.9=9.9999999 这类误差让收敛推迟一个轮询周期。
_EPS = 1e-9
_TRIGGERED = "triggered"
_UPGRADED = "upgraded"
_REPEATED = "repeated"
_RESOLVED = "resolved"
_NONE = "none"
_FIRING = "firing"


@dataclass
class AlertState:
    """一条进行中/已收敛的告警状态。

    source 为 None 表示全局口径告警（老规则）；by_source 规则下
    每个来源一条独立告警流，source 记录该流归属的来源。
    """

    fingerprint: str
    rule: str
    severity: str
    status: str  # firing | resolved
    count: int
    first_seen: float
    last_seen: float
    window_seconds: float
    source: str | None = None
    resolved_at: float | None = None


class AlertEngine:
    """线程安全的窗口告警引擎。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._active: dict[str, AlertState] = {}
        self._history: dict[str, list[AlertState]] = {}

    @staticmethod
    def fingerprint(rule_name: str, source: str | None = None) -> str:
        # 全局规则：rule:<name>（与老版本完全一致）；
        # by_source 规则：rule:<name>|source:<source>，每个来源一条独立告警流。
        if source is None:
            return f"rule:{rule_name}"
        return f"rule:{rule_name}|source:{source}"

    def evaluate(
        self,
        rule: AlertRule,
        value: float,
        firing: bool,
        now: float,
        source: str | None = None,
    ) -> str:
        """评估单条规则的一次观测（source 非空时按来源独立判定）。

        返回状态变化类型：
        - ``triggered``：新建一条有效告警；
        - ``repeated``：窗口内重复命中，已合并到同一告警；
        - ``upgraded``：持续命中达到升级次数，已升为 critical；
        - ``none``：未越阈，不产生变化（收敛由 :meth:`sweep` 负责）。
        """
        fp = self.fingerprint(rule.name, source)
        with self._lock:
            if not firing:
                # 单次未越阈不立即收敛：异常是否结束以时间窗口为准，
                # 避免一次抖动样本导致告警反复开合。
                return _NONE

            state = self._active.get(fp)
            if state is None:
                state = AlertState(
                    fingerprint=fp,
                    rule=rule.name,
                    severity=rule.severity,
                    status=_FIRING,
                    count=1,
                    first_seen=now,
                    last_seen=now,
                    window_seconds=rule.window_seconds,
                    source=source,
                )
                self._active[fp] = state
                return _TRIGGERED

            # 合并到同一条有效告警：刷新窗口、累计命中次数。
            state.count += 1
            state.last_seen = now

            if (
                rule.upgrade_after > 0
                and state.count >= rule.upgrade_after + 1
                and state.severity != "critical"
            ):
                state.severity = "critical"
                return _UPGRADED
            return _REPEATED

    def sweep(self, now: float, active_rules: list[AlertRule] | None = None) -> list[AlertState]:
        """收敛超过各自窗口未再触发的告警，返回本次新收敛列表。

        窗口取告警创建时规则携带的 ``window_seconds``，因此热更新规则
        不会改变一条在飞告警的收敛节奏；规则被删除后其告警仍会按时收敛。
        """
        newly_resolved: list[AlertState] = []
        with self._lock:
            expired = [
                fp
                for fp, state in self._active.items()
                if now - state.last_seen + _EPS >= state.window_seconds
            ]
            for fp in expired:
                state = self._active.pop(fp)
                state.status = _RESOLVED
                state.resolved_at = now
                history = self._history.setdefault(fp, [])
                history.append(state)
                if len(history) > _HISTORY_PER_KEY:
                    del history[:-_HISTORY_PER_KEY]
                newly_resolved.append(replace(state))
        return newly_resolved

    def states(self) -> list[AlertState]:
        """返回所有进行中告警及最近收敛历史的副本（按首次发现时间排序）。"""
        with self._lock:
            out = [replace(s) for s in self._active.values()]
            for history in self._history.values():
                out.extend(replace(s) for s in history)
        out.sort(key=lambda s: (s.first_seen, s.fingerprint))
        return out

    def active(self) -> list[AlertState]:
        """仅返回当前进行中的告警副本。"""
        with self._lock:
            return [replace(s) for s in self._active.values()]

    def reset(self) -> None:
        with self._lock:
            self._active.clear()
            self._history.clear()
