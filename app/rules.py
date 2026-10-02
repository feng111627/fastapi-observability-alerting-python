"""告警规则模型、校验与热更新。

规则存储采用不可变快照 + 读多写少的原子替换：
- 逐条校验候选规则，单条坏规则只记录可区分的失败原因并跳过，
  不影响同批其它规则生效；
- 至少有一条合法规则时，整体原子切换为新版本；
- 整批全部非法（或为空）时保留上一版可用规则，版本号不推进；
- 读路径无锁（快照不可变），写路径串行化。
"""

import threading
from dataclasses import dataclass, field

VALID_METRICS = frozenset({"error_rate", "latency_p95", "latency_avg", "error_count"})
VALID_SEVERITIES = frozenset({"warning", "critical"})


@dataclass(frozen=True)
class AlertRule:
    """一条经过校验的不可变告警规则。"""

    name: str
    metric: str
    threshold: float
    window_seconds: float
    severity: str = "warning"
    upgrade_after: int = 0


class RuleValidationError(ValueError):
    """规则内容非法，消息中携带规则名与具体原因。"""


@dataclass(frozen=True)
class RuleSet:
    """某一版规则集合（不可变快照）。"""

    version: int
    rules: tuple[AlertRule, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class LoadReport:
    """一次规则加载的结果报告。"""

    ok: bool
    version: int
    loaded: int
    rejected: int
    errors: tuple[str, ...] = field(default_factory=tuple)


def _label(raw: object) -> str:
    if isinstance(raw, dict):
        name = raw.get("name")
        if isinstance(name, str) and name:
            return name
    return f"<未命名:{type(raw).__name__}>"


def validate_rule(raw: dict) -> AlertRule:
    """校验并构造一条规则。

    非法时抛出 :class:`RuleValidationError`，消息格式为
    ``规则[<name>] <字段>: <原因>``，保证失败原因可区分、可定位。
    """
    if not isinstance(raw, dict):
        raise RuleValidationError(f"规则[{_label(raw)}] 整体: 必须是对象/字典")

    label = _label(raw)

    name = raw.get("name")
    if not isinstance(name, str) or not name.strip():
        raise RuleValidationError(f"规则[{label}] name: 必须是非空字符串")
    name = name.strip()

    metric = raw.get("metric")
    if metric not in VALID_METRICS:
        raise RuleValidationError(
            f"规则[{name}] metric: 取值 {metric!r} 非法，"
            f"必须是 {sorted(VALID_METRICS)} 之一"
        )

    threshold = raw.get("threshold")
    if isinstance(threshold, bool) or not isinstance(threshold, (int, float)):
        raise RuleValidationError(f"规则[{name}] threshold: 必须是数值，收到 {threshold!r}")
    threshold = float(threshold)
    if threshold < 0:
        raise RuleValidationError(f"规则[{name}] threshold: 不能为负数，收到 {threshold}")
    if metric == "error_rate" and threshold > 1:
        raise RuleValidationError(
            f"规则[{name}] threshold: error_rate 口径阈值必须在 [0, 1] 区间，收到 {threshold}"
        )

    window = raw.get("window_seconds")
    if isinstance(window, bool) or not isinstance(window, (int, float)):
        raise RuleValidationError(
            f"规则[{name}] window_seconds: 必须是数值，收到 {window!r}"
        )
    window = float(window)
    if window <= 0:
        raise RuleValidationError(
            f"规则[{name}] window_seconds: 必须为正数，收到 {window}"
        )

    severity = raw.get("severity", "warning")
    if severity not in VALID_SEVERITIES:
        raise RuleValidationError(
            f"规则[{name}] severity: 取值 {severity!r} 非法，"
            f"必须是 {sorted(VALID_SEVERITIES)} 之一"
        )

    upgrade_after = raw.get("upgrade_after", 0)
    if isinstance(upgrade_after, bool) or not isinstance(upgrade_after, int):
        raise RuleValidationError(
            f"规则[{name}] upgrade_after: 必须是非负整数，收到 {upgrade_after!r}"
        )
    if upgrade_after < 0:
        raise RuleValidationError(
            f"规则[{name}] upgrade_after: 不能为负数，收到 {upgrade_after}"
        )

    return AlertRule(
        name=name,
        metric=metric,
        threshold=threshold,
        window_seconds=window,
        severity=severity,
        upgrade_after=upgrade_after,
    )


class RuleStore:
    """规则热更新存储：原子快照替换，坏版本不影响在用版本。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._current = RuleSet(version=0)

    def load(self, raws: list[dict]) -> LoadReport:
        """加载一批候选规则。

        逐条校验：坏规则单独记录原因并跳过，不影响其他规则；
        若整批没有任何合法规则，则保留上一版规则且不推进版本号。
        """
        if not isinstance(raws, list):
            raise TypeError("raws 必须是列表")

        valid: list[AlertRule] = []
        errors: list[str] = []
        seen: set[str] = set()

        for index, raw in enumerate(raws):
            try:
                rule = validate_rule(raw)
            except RuleValidationError as exc:
                errors.append(f"第{index}条: {exc}")
                continue
            if rule.name in seen:
                errors.append(
                    f"第{index}条: 规则[{rule.name}] name: 与同批其它规则重名，已拒绝"
                )
                continue
            seen.add(rule.name)
            valid.append(rule)

        with self._lock:
            if not valid:
                # 整批失败（或为空）：保留上一版，返回可区分的失败原因。
                if not errors:
                    errors.append("整批规则为空，保留上一版规则不变")
                return LoadReport(
                    ok=False,
                    version=self._current.version,
                    loaded=0,
                    rejected=len(raws),
                    errors=tuple(errors),
                )

            new_set = RuleSet(version=self._current.version + 1, rules=tuple(valid))
            self._current = new_set
            return LoadReport(
                ok=True,
                version=new_set.version,
                loaded=len(valid),
                rejected=len(errors),
                errors=tuple(errors),
            )

    def current(self) -> RuleSet:
        # 快照不可变，读路径无需加锁。
        return self._current

    def reset(self) -> None:
        with self._lock:
            self._current = RuleSet(version=0)
