"""API 数据模型。"""

from pydantic import BaseModel, Field

from app.metrics import MAX_SOURCE_LEN


class MetricReport(BaseModel):
    """外部上报的一次请求指标。

    request_id 用于幂等去重；未带时服务端生成；
    sampled 为采样开关，关闭后该请求计入 total 但不进入统计口径；
    source 为来源维度，未带/空白时落入默认来源。
    """

    request_id: str | None = None
    trace_id: str | None = None
    duration_ms: float = Field(ge=0)
    status_code: int = Field(ge=100, le=599)
    error: str | None = None
    sampled: bool = True
    source: str | None = Field(default=None, max_length=MAX_SOURCE_LEN)


class MetricSnapshotOut(BaseModel):
    """指标快照。source 为 None 表示全局汇总，否则为对应来源口径。"""

    total: int
    sampled: int
    error_count: int
    sum_duration_ms: float
    avg_duration_ms: float
    error_rate: float
    by_status: dict[int, int]
    source: str | None = None


class CardinalityOut(BaseModel):
    """来源基数观测。"""

    max_sources: int
    active_sources: int
    overflowed: bool
    overflow_records: int
    overflow_sources_sample: list[str]


class SourcesOverviewOut(BaseModel):
    """各来源快照与基数观测：全局快照 = 各来源快照之和。"""

    cardinality: CardinalityOut
    sources: list[MetricSnapshotOut]


class AlertRuleIn(BaseModel):
    """告警规则入参（热更新为整批替换）。

    source 缺省为全局口径；"*" 表示按来源分别判定；
    其它取值表示只对指定来源判定。
    """

    name: str = Field(min_length=1)
    metric: str
    threshold: float = Field(ge=0)
    window_seconds: float = Field(gt=0)
    severity: str = "warning"
    upgrade_after: int = Field(default=0, ge=0)
    source: str | None = Field(default=None, max_length=MAX_SOURCE_LEN)


class RuleLoadResult(BaseModel):
    """规则加载结果：坏规则逐条给出可区分原因。"""

    ok: bool
    version: int
    loaded: int
    rejected: int
    errors: list[str]


class AlertOut(BaseModel):
    """告警输出。source 为 None 表示全局口径告警。"""

    fingerprint: str
    rule: str
    severity: str
    status: str
    count: int
    first_seen: float
    last_seen: float
    window_seconds: float
    resolved_at: float | None = None
    source: str | None = None


class RuleReloadRequest(BaseModel):
    """规则热更新请求体。"""

    rules: list[AlertRuleIn]
