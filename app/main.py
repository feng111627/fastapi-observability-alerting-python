"""FastAPI 装配：链路中间件、上报接口、指标查询、告警评估与规则热更新。

时间统一走可注入时钟，便于测试确定性验证窗口语义。
"""

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from app import tracing
from app.alerts import AlertEngine
from app.evaluation import Evaluator
from app.metrics import DEFAULT_MAX_SOURCES, MetricsRegistry
from app.rules import RuleStore
from app.schemas import (
    AlertOut,
    MetricReport,
    MetricSnapshotOut,
    RuleLoadResult,
    RuleReloadRequest,
    SourceMetricsOut,
    SourceStatsOut,
)

logger = logging.getLogger("observability")
_SWEEP_INTERVAL = 0.2


class MonotonicClock:
    """单调时钟包装（测试可替换实例方法）。"""

    def time(self) -> float:
        import time

        return time.monotonic()


def create_app(clock: MonotonicClock | None = None, max_sources: int = DEFAULT_MAX_SOURCES) -> FastAPI:
    """应用工厂：组装注册表、告警引擎、规则存储、评估器与路由。

    max_sources：生效来源数量上限，超出后新来源归并到 __overflow__
    保留桶（见 GET /metrics/sources 的 stats 口径）。
    """
    clock = clock or MonotonicClock()
    registry = MetricsRegistry(max_sources=max_sources)
    engine = AlertEngine()
    store = RuleStore()
    evaluator = Evaluator(registry, engine, store, clock=clock.time)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        task: asyncio.Task | None = None

        async def sweeper() -> None:
            while True:
                await asyncio.sleep(_SWEEP_INTERVAL)
                try:
                    resolved = evaluator.sweep()
                    for state in resolved:
                        logger.info(
                            "告警收敛 rule=%s fingerprint=%s at=%.3f",
                            state.rule,
                            state.fingerprint,
                            state.resolved_at,
                        )
                except Exception:  # 收敛循环不能因异常退出
                    logger.exception("告警收敛循环异常")

        task = asyncio.create_task(sweeper())
        try:
            yield
        finally:
            task.cancel()

    app = FastAPI(title="fastapi-observability-alerting", lifespan=lifespan)
    app.state.registry = registry
    app.state.engine = engine
    app.state.store = store
    app.state.evaluator = evaluator
    app.state.clock = clock

    @app.middleware("http")
    async def observability_middleware(request: Request, call_next):
        request_id = request.headers.get("x-request-id") or tracing.new_request_id()
        trace_id = request.headers.get("x-trace-id") or tracing.new_trace_id()
        source = request.headers.get("x-source")  # 未带时落入默认来源
        tokens = tracing.set_context(request_id, trace_id)
        start = clock.time()
        response = None
        error: str | None = None
        try:
            response = await call_next(request)
            return response
        except Exception as exc:  # 未捕获异常也要留下指标
            error = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            elapsed_ms = (clock.time() - start) * 1000.0
            status_code = response.status_code if response is not None else 500
            registry.record(
                request_id=request_id,
                trace_id=trace_id,
                duration_ms=elapsed_ms,
                status_code=status_code,
                error=error,
                sampled=True,
                now=clock.time(),
                source=source,
            )
            if response is not None:
                response.headers["x-request-id"] = request_id
                response.headers["x-trace-id"] = trace_id
            tracing.reset_context(tokens)

    @app.post("/metrics/report", tags=["metrics"])
    def report_metric(body: MetricReport):
        """幂等上报一次请求指标；同一 request_id 重复上报不重复计数。

        source 未携带时落入默认来源 default；来源数量超限后归并到
        __overflow__ 保留桶，响应中的 source 为实际生效来源。
        """
        request_id = body.request_id or tracing.current_request_id() or tracing.new_request_id()
        trace_id = body.trace_id or tracing.current_trace_id() or tracing.new_trace_id()
        accepted = registry.record(
            request_id=request_id,
            trace_id=trace_id,
            duration_ms=body.duration_ms,
            status_code=body.status_code,
            error=body.error,
            sampled=body.sampled,
            now=clock.time(),
            source=body.source,
        )
        return {
            "request_id": request_id,
            "trace_id": trace_id,
            "source": registry.get(request_id).source,
            "accepted": accepted,
            "duplicate": not accepted,
        }

    @app.get("/metrics", response_model=MetricSnapshotOut, tags=["metrics"])
    def get_metrics():
        """全局一致快照（= 各生效来源快照之和）。"""
        return registry.snapshot()

    @app.get("/metrics/sources", response_model=SourceMetricsOut, tags=["metrics"])
    def get_metrics_by_source():
        """按来源的一致快照 + 来源上限/溢出归并的可观察口径。"""
        stats = registry.source_stats()
        return SourceMetricsOut(
            stats=SourceStatsOut(
                max_sources=stats.max_sources,
                source_count=stats.source_count,
                sources=list(stats.sources),
                overflow_source=stats.overflow_source,
                overflowed=stats.overflowed,
                overflow_records=stats.overflow_records,
                overflow_sources_seen=stats.overflow_sources_seen,
                overflow_samples_truncated=stats.overflow_samples_truncated,
            ),
            by_source={
                src: MetricSnapshotOut(
                    total=snap.total,
                    sampled=snap.sampled,
                    error_count=snap.error_count,
                    sum_duration_ms=snap.sum_duration_ms,
                    avg_duration_ms=snap.avg_duration_ms,
                    error_rate=snap.error_rate,
                    by_status=snap.by_status,
                )
                for src, snap in registry.snapshot_all_sources().items()
            },
        )

    @app.post("/alerts/evaluate", response_model=list[AlertOut], tags=["alerts"])
    def evaluate_alerts():
        """对当前规则集评估一轮，返回当前全部告警（含本次变化）。"""
        changes = evaluator.evaluate_once()
        for rule, kind, value, source in changes:
            if kind in ("triggered", "upgraded"):
                logger.warning(
                    "告警%s rule=%s source=%s metric=%s value=%.4f kind=%s",
                    "升级" if kind == "upgraded" else "触发",
                    rule.name,
                    source or "<global>",
                    rule.metric,
                    value,
                    kind,
                )
        return engine.states()

    @app.get("/alerts", response_model=list[AlertOut], tags=["alerts"])
    def list_alerts():
        return engine.states()

    @app.post("/rules/reload", response_model=RuleLoadResult, tags=["rules"])
    def reload_rules(body: RuleReloadRequest):
        """整批热更新规则；单条坏规则隔离，整批失败保留上一版。"""
        raws = [item.model_dump() for item in body.rules]
        report = store.load(raws)
        return RuleLoadResult(
            ok=report.ok,
            version=report.version,
            loaded=report.loaded,
            rejected=report.rejected,
            errors=list(report.errors),
        )

    @app.get("/rules", tags=["rules"])
    def get_rules():
        current = store.current()
        return {
            "version": current.version,
            "rules": [
                {
                    "name": r.name,
                    "metric": r.metric,
                    "threshold": r.threshold,
                    "window_seconds": r.window_seconds,
                    "severity": r.severity,
                    "upgrade_after": r.upgrade_after,
                    "by_source": r.by_source,
                }
                for r in current.rules
            ],
        }

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
