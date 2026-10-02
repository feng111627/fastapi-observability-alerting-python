"""告警规则热更新：校验、部分降级、版本留痕。

- validate_rule 对每类问题给出固定、可区分的 reason 代码；
- parse_rules 单条坏规则不影响其它规则；
- RuleStore 只有在“至少一条有效规则”时才原子切换版本，
  整批无效则保留上一版可用规则。
"""
from __future__ import annotations

import threading
from typing import Any

from app.models import AlertRule, RuleLoadResult, RuleValidationError

VALID_METRICS = {"error_rate", "latency_ms", "error_count"}
VALID_OPERATORS = {">", ">=", "<", "<="}

# reason 代码
R_NOT_OBJECT = "not_an_object"
R_NAME_MISSING = "name_missing"
R_NAME_TYPE = "name_invalid"
R_METRIC_UNKNOWN = "metric_unknown"
R_OPERATOR_UNSUPPORTED = "operator_unsupported"
R_THRESHOLD_INVALID = "threshold_invalid"
R_WINDOW_INVALID = "window_invalid"
R_FOR_INVALID = "for_invalid"
R_COOLDOWN_INVALID = "cooldown_invalid"
R_RESOLVE_INVALID = "resolve_invalid"
R_DUPLICATE_NAME = "duplicate_name"


def _as_float(data: dict, key: str, reason: str, rule_name: str) -> float:
    if key not in data:
        raise RuleValidationError(rule_name, reason)
    value = data[key]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RuleValidationError(rule_name, reason)
    return float(value)


def validate_rule(data: Any) -> AlertRule:
    """校验并构造单条规则；失败抛 RuleValidationError。"""
    if not isinstance(data, dict):
        raise RuleValidationError("<unknown>", R_NOT_OBJECT)
    name = data.get("name")
    if name is None:
        raise RuleValidationError("<unknown>", R_NAME_MISSING)
    if not isinstance(name, str) or not name.strip():
        raise RuleValidationError(str(name), R_NAME_TYPE)
    name = name.strip()

    metric = data.get("metric")
    if metric not in VALID_METRICS:
        raise RuleValidationError(name, R_METRIC_UNKNOWN)
    operator = data.get("operator")
    if operator not in VALID_OPERATORS:
        raise RuleValidationError(name, R_OPERATOR_UNSUPPORTED)

    threshold = _as_float(data, "threshold", R_THRESHOLD_INVALID, name)

    window_s = data.get("window_s", 60.0)
    if isinstance(window_s, bool) or not isinstance(window_s, (int, float)) or window_s <= 0:
        raise RuleValidationError(name, R_WINDOW_INVALID)
    for_s = data.get("for_s", 0.0)
    if isinstance(for_s, bool) or not isinstance(for_s, (int, float)) or for_s < 0:
        raise RuleValidationError(name, R_FOR_INVALID)
    cooldown_s = data.get("cooldown_s", 30.0)
    if isinstance(cooldown_s, bool) or not isinstance(cooldown_s, (int, float)) or cooldown_s < 0:
        raise RuleValidationError(name, R_COOLDOWN_INVALID)
    resolve_s = data.get("resolve_s", 120.0)
    if isinstance(resolve_s, bool) or not isinstance(resolve_s, (int, float)) or resolve_s <= 0:
        raise RuleValidationError(name, R_RESOLVE_INVALID)

    return AlertRule(
        name=name,
        metric=metric,
        operator=operator,
        threshold=threshold,
        window_s=float(window_s),
        for_s=float(for_s),
        cooldown_s=float(cooldown_s),
        resolve_s=float(resolve_s),
    )


def parse_rules(payload: Any) -> tuple[list[AlertRule], dict[str, str]]:
    """批量解析。返回（有效规则, {规则键: 失败原因}）。"""
    rejected: dict[str, str] = {}
    loaded: list[AlertRule] = []
    seen_names: set[str] = set()
    if not isinstance(payload, list):
        return [], {"<payload>": R_NOT_OBJECT}
    for index, raw in enumerate(payload):
        try:
            rule = validate_rule(raw)
        except RuleValidationError as exc:
            key = exc.rule_name if exc.rule_name != "<unknown>" else f"#{index}"
            rejected[key] = exc.reason
            continue
        if rule.name in seen_names:
            rejected[rule.name] = R_DUPLICATE_NAME
            continue
        seen_names.add(rule.name)
        loaded.append(rule)
    return loaded, rejected


class RuleStore:
    """线程安全规则仓库。加载整批无效时保留上一版。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._rules: list[AlertRule] = []
        self._version: int = 0
        self._last_error: str | None = None
        self._last_rejected: dict[str, str] = {}

    def load(self, payload: Any) -> RuleLoadResult:
        with self._lock:
            loaded, rejected = parse_rules(payload)
            if not loaded:
                # 整批无效：保留旧版，记录原因
                self._last_error = "no_valid_rules"
                self._last_rejected = rejected
                return RuleLoadResult(loaded=[], rejected=rejected, version=self._version)
            self._rules = loaded
            self._version += 1
            self._last_error = None if not rejected else "partial_rejected"
            self._last_rejected = rejected
            return RuleLoadResult(loaded=loaded, rejected=rejected, version=self._version)

    def current(self) -> list[AlertRule]:
        with self._lock:
            return list(self._rules)

    @property
    def version(self) -> int:
        with self._lock:
            return self._version

    @property
    def last_error(self) -> str | None:
        with self._lock:
            return self._last_error

    @property
    def last_rejected(self) -> dict[str, str]:
        with self._lock:
            return dict(self._last_rejected)
