"""FastAPI 应用与 HTTP 接口。"""
from __future__ import annotations

import asyncio
import contextlib
import time

from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import BaseModel

from app.models import Alert, AlertState, RequestSample
from app.service import service_singleton
from app.tracing import REQUEST_ID_HEADER, TracingMiddleware, get_request_id


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    """后台每 1s 推进一次告警状态机，驱动 PENDING 超时与 FIRING 收敛。"""
    async def _tick_loop() -> None:
        while True:
            await asyncio.sleep(1.0)
            await asyncio.to_thread(service_singleton.tick)

    task = asyncio.create_task(_tick_loop())
    try:
        yield
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


app = FastAPI(title="fastapi-observability-alerting", version="0.1.0", lifespan=lifespan)
app.add_middleware(TracingMiddleware)


class ReportIn(BaseModel):
    request_id: str | None = None
    duration_ms: float = 0.0
    status_code: int = 200
    error: str | None = None
    method: str = "GET"
    path: str = "/"


def _alert_dict(a: Alert) -> dict:
    return {
        "rule_name": a.rule_name,
        "fingerprint": a.fingerprint,
        "state": a.state.value,
        "level": a.level,
        "first_seen": a.first_seen,
        "last_seen": a.last_seen,
        "fired_at": a.fired_at,
        "resolved_at": a.resolved_at,
        "last_value": a.last_value,
        "count": a.count,
    }


def _changes(result) -> dict:
    return {
        "fired": [_alert_dict(a) for a in result.fired],
        "upgraded": [_alert_dict(a) for a in result.upgraded],
        "resolved": [_alert_dict(a) for a in result.resolved],
    }


@app.post("/api/report")
def report(body: ReportIn, request: Request):
    """上报一次请求指标；request_id 缺省取链路标识，重复上报幂等。"""
    rid = (body.request_id or get_request_id(request)).strip() or get_request_id(request)
    sample = RequestSample(
        request_id=rid,
        timestamp=time.time(),
        duration_ms=float(body.duration_ms),
        status_code=body.status_code,
        error=body.error,
        method=body.method,
        path=body.path,
    )
    counted, changes = service_singleton.report(sample)
    return {"request_id": rid, "counted": counted, "changes": _changes(changes)}


@app.get("/api/metrics")
def get_metrics():
    """指标口径：total/error_count 为去重后的请求数，avg_duration_ms 仅按已计数样本。"""
    snap = service_singleton.snapshot()
    avg = snap.total_duration_ms / snap.total if snap.total else 0.0
    return {
        "total": snap.total,
        "error_count": snap.error_count,
        "avg_duration_ms": round(avg, 3),
        "sampling": service_singleton.metrics.sampling,
        "last_request_id": snap.last_request_id,
    }


@app.get("/api/alerts")
def get_alerts(state: str | None = Query(default=None)):
    target = None
    if state is not None:
        try:
            target = AlertState(state)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"unknown state: {state}")
    return [_alert_dict(a) for a in service_singleton.alerts.list_alerts(target)]


@app.post("/api/rules/reload")
def reload_rules(payload: list[dict]):
    """热更新规则。整批无效返回 422 且保留上一版；部分坏规则返回 200 并列出原因。"""
    result = service_singleton.reload_rules(payload)
    body = {
        "version": result.version,
        "loaded": [r.name for r in result.loaded],
        "rejected": result.rejected,
        "last_error": service_singleton.rules.last_error,
    }
    if not result.loaded:
        raise HTTPException(status_code=422, detail=body)
    return body


@app.get("/api/rules")
def list_rules():
    store = service_singleton.rules
    return {
        "version": store.version,
        "last_error": store.last_error,
        "rules": [
            {
                "name": r.name, "metric": r.metric, "operator": r.operator,
                "threshold": r.threshold, "window_s": r.window_s,
                "for_s": r.for_s, "cooldown_s": r.cooldown_s, "resolve_s": r.resolve_s,
            }
            for r in store.current()
        ],
    }


@app.get("/api/demo")
def demo(request: Request, fail: int = 0, delay_ms: int = 0):
    """自带埋点的演示业务：自动计时、按 request_id 去重上报。"""
    start = time.perf_counter()
    rid = get_request_id(request)
    status = 500 if fail else 200
    error = "forced failure" if fail else None
    # 模拟业务耗时
    if delay_ms:
        time.sleep(delay_ms / 1000.0)
    duration_ms = (time.perf_counter() - start) * 1000.0
    sample = RequestSample(
        request_id=rid, timestamp=time.time(), duration_ms=duration_ms,
        status_code=status, error=error, method="GET", path="/api/demo",
    )
    counted, changes = service_singleton.report(sample)
    return {
        "request_id": rid, "ok": not fail, "duration_ms": round(duration_ms, 3),
        "counted": counted, "changes": _changes(changes),
    }


@app.get("/")
def read_root():
    return {"service": "fastapi-observability-alerting", "docs": "/docs"}
