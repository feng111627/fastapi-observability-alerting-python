"""领域数据模型（仅类型定义）。"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class AlertState(str, Enum):
    """告警生命周期状态。"""
    PENDING = "pending"      # 窗口内持续触发、尚未升级
    FIRING = "firing"        # 已升级为有效告警
    RESOLVED = "resolved"    # 异常已收敛


@dataclass(frozen=True)
class RequestSample:
    """一次请求的指标样本。request_id 为链路唯一标识。"""
    request_id: str
    timestamp: float
    duration_ms: float
    status_code: int
    error: str | None = None
    method: str = "GET"
    path: str = "/"


@dataclass
class MetricSnapshot:
    """指标快照。"""
    total: int = 0
    error_count: int = 0
    total_duration_ms: float = 0.0
    last_request_id: str | None = None


@dataclass(frozen=True)
class AlertRule:
    """单条告警规则。

    threshold: 触发阈值；window_s: 统计窗口；for_s: 持续多久后升级；
    cooldown_s: 同一异常合并去重的最小间隔；resolve_s: 多久未见触发即收敛。
    """
    name: str
    metric: str
    operator: str
    threshold: float
    window_s: float = 60.0
    for_s: float = 0.0
    cooldown_s: float = 30.0
    resolve_s: float = 120.0


@dataclass
class Alert:
    """一条有效告警。"""
    rule_name: str
    fingerprint: str
    state: AlertState
    level: int = 1
    first_seen: float = 0.0
    last_seen: float = 0.0
    last_value: float = 0.0
    fired_at: float | None = None
    resolved_at: float | None = None
    count: int = 1


@dataclass
class RuleLoadResult:
    """规则热更新结果。"""
    loaded: list[AlertRule] = field(default_factory=list)
    rejected: dict[str, str] = field(default_factory=dict)
    version: int = 0


@dataclass
class EvaluationResult:
    """一次评估产生的告警变化。"""
    fired: list[Alert] = field(default_factory=list)
    upgraded: list[Alert] = field(default_factory=list)
    resolved: list[Alert] = field(default_factory=list)


class RuleValidationError(ValueError):
    """规则校验失败（可区分原因）。"""

    def __init__(self, rule_name: str, reason: str) -> None:
        self.rule_name = rule_name
        self.reason = reason
        super().__init__(f"rule {rule_name!r}: {reason}")
