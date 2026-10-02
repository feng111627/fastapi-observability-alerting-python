"""API 数据模型。"""

from pydantic import BaseModel, Field


class MetricReport(BaseModel):
    """外部上报的一次请求指标。

    request_id 用于幂等去重；未带时服务端生成；
    sampled 为采样开关，关闭后该请求计入 total 但不进入统计口径。
    """

    request_id: str | None = None
    trace_id: str | None = None
    duration_ms: float = Field(ge=0)
    status_code: int = Field(ge=100, le=599)
    error: str | None = None
    sampled: bool = True


class MetricSnapshotOut(BaseModel):
    """指标快照。"""

    total: int
    sampled: int
    error_count: int
    sum_duration_ms: float
    avg_duration_ms: float
    error_rate: float
    by_status: dict[int, int]


class AlertRuleIn(BaseModel):
    """告警规则入参（热更新为整批替换）。"""

    name: str = Field(min_length=1)
    metric: str
    threshold: float = Field(ge=0)
    window_seconds: float = Field(gt=0)
    severity: str = "warning"
    upgrade_after: int = Field(default=0, ge=0)


class RuleLoadResult(BaseModel):
    """规则加载结果：坏规则逐条给出可区分原因。"""

    ok: bool
    version: int
    loaded: int
    rejected: int
    errors: list[str]


class AlertOut(BaseModel):
    """告警输出。"""

    fingerprint: str
    rule: str
    severity: str
    status: str
    count: int
    first_seen: float
    last_seen: float
    window_seconds: float
    resolved_at: float | None = None


class RuleReloadRequest(BaseModel):
    """规则热更新请求体。"""

    rules: list[AlertRuleIn]
