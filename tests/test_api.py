"""业务: HTTP 接口与端到端并发一致性。

覆盖：链路标识响应头、幂等上报接口、并发上报指标一致、
规则热更新接口降级、告警触发/去重/升级/收敛的端到端链路。
"""

import threading

import pytest
from fastapi.testclient import TestClient

from app.main import MonotonicClock, create_app


class FakeClock:
    """可控时钟，所有组件共享同一时间源。"""

    def __init__(self, start: float = 1000.0):
        self.now = start

    def time(self) -> float:
        return self.now


@pytest.fixture
def client():
    app = create_app(clock=FakeClock())
    with TestClient(app) as c:
        yield c


def test_中间件分配并回传请求与链路标识(client):
    r = client.get("/metrics")
    assert r.status_code == 200
    assert r.headers.get("x-request-id")
    assert r.headers.get("x-trace-id")


def test_客户端透传的id被原样使用(client):
    r = client.get("/metrics", headers={"x-request-id": "rid-xyz", "x-trace-id": "tid-xyz"})
    assert r.headers["x-request-id"] == "rid-xyz"
    assert r.headers["x-trace-id"] == "tid-xyz"


def test_上报接口幂等_重复上报不重复计数(client):
    payload = {"request_id": "biz-1", "duration_ms": 120, "status_code": 500, "error": "boom"}
    first = client.post("/metrics/report", json=payload).json()
    second = client.post("/metrics/report", json=payload).json()
    assert first["accepted"] is True and first["duplicate"] is False
    assert second["accepted"] is False and second["duplicate"] is True
    assert second["request_id"] == "biz-1"

    snap = client.get("/metrics").json()
    # 业务记录 1 条；/metrics 与两次 /metrics/report 还会被中间件记录，
    # 但 sampled 口径里业务错误只有 biz-1 这一条 500 错误
    business_records = [a for a in client.app.state.registry.records() if a.request_id == "biz-1"]
    assert len(business_records) == 1


def test_采样关闭记录不进入统计口径(client):
    client.post("/metrics/report", json={
        "request_id": "s1", "duration_ms": 10, "status_code": 200, "sampled": True})
    client.post("/metrics/report", json={
        "request_id": "s2", "duration_ms": 10, "status_code": 500, "error": "E", "sampled": False})
    client.post("/metrics/report", json={
        "request_id": "s3", "duration_ms": 10, "status_code": 200, "sampled": True})
    snap = client.app.state.registry.snapshot()
    sampled = [r for r in snap.by_status.keys()]
    assert snap.by_status.get(500) is None  # 未采样的 500 不进口径
    assert 200 in sampled


def test_非法上报被参数校验拒绝(client):
    r = client.post("/metrics/report", json={"duration_ms": -1, "status_code": 200})
    assert r.status_code == 422


def test_规则热更新接口_混合批次隔离坏规则(client):
    r = client.post("/rules/reload", json={"rules": [
        {"name": "ok", "metric": "error_rate", "threshold": 0.5, "window_seconds": 60},
        {"name": "bad", "metric": "unknown", "threshold": 1, "window_seconds": 10},
    ]})
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True and body["loaded"] == 1 and body["rejected"] == 1
    assert any("bad" in e for e in body["errors"])

    current = client.get("/rules").json()
    assert current["version"] == 1
    assert [r["name"] for r in current["rules"]] == ["ok"]


def test_整批非法接口降级_保留上一版(client):
    client.post("/rules/reload", json={"rules": [
        {"name": "keep", "metric": "error_rate", "threshold": 0.5, "window_seconds": 60},
    ]})
    r = client.post("/rules/reload", json={"rules": [
        {"name": "x", "metric": "nope", "threshold": 1, "window_seconds": 10},
    ]})
    body = r.json()
    assert body["ok"] is False and body["version"] == 1 and body["loaded"] == 0
    names = [x["name"] for x in client.get("/rules").json()["rules"]]
    assert names == ["keep"]


def test_告警端到端_触发去重升级收敛(client):
    clock = client.app.state.clock
    client.post("/rules/reload", json={"rules": [
        {"name": "err", "metric": "error_count", "threshold": 1,
         "window_seconds": 10, "severity": "warning", "upgrade_after": 2},
    ]})

    # 连续三个错误请求
    for i in range(3):
        client.post("/metrics/report", json={
            "request_id": f"e{i}", "duration_ms": 10, "status_code": 500, "error": "boom"})

    kinds: list[str] = []
    for _ in range(3):
        states = client.post("/alerts/evaluate").json()
        firing = [s for s in states if s["rule"] == "err"]
        assert len(firing) == 1, "窗口内始终只有一条有效告警"
        kinds.append(firing[0]["severity"])

    assert firing[0]["count"] == 3 and firing[0]["severity"] == "critical"

    # 窗口过期后再评估 -> 收敛
    clock.now += 11
    states = client.post("/alerts/evaluate").json()
    err_states = [s for s in states if s["rule"] == "err"]
    assert err_states and err_states[-1]["status"] == "resolved"
    assert err_states[-1]["resolved_at"] == clock.now


def test_并发上报同id_接口层只有一条生效(client):
    n = 24
    results: list[bool] = []
    lock = threading.Lock()
    barrier = threading.Barrier(n)

    def worker():
        barrier.wait()
        body = client.post("/metrics/report", json={
            "request_id": "race-same", "duration_ms": 5, "status_code": 200}).json()
        with lock:
            results.append(body["accepted"])

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sum(results) == 1
    reg = client.app.state.registry
    assert reg.get("race-same") is not None
    snap = reg.snapshot()
    assert sum(1 for r in reg.records() if r.request_id == "race-same") == 1


def test_并发上报不同id_无丢失且快照一致(client):
    n_threads, per = 12, 100
    barrier = threading.Barrier(n_threads)

    def worker(tid: int):
        barrier.wait()
        for i in range(per):
            client.post("/metrics/report", json={
                "request_id": f"biz-c{tid}-{i}", "duration_ms": float(i),
                "status_code": 500 if i % 2 else 200,
                "error": "E" if i % 2 else None})

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    reg = client.app.state.registry
    total = sum(1 for r in reg.records() if r.request_id.startswith("biz-c"))
    snap = reg.snapshot()
    assert total == n_threads * per
    # 错误数与状态码分布对得上，没有重复或丢失
    business = [r for r in reg.records() if r.request_id.startswith("biz-c")]
    assert sum(1 for r in business if r.error is not None) == total // 2
    assert snap.by_status[500] == total // 2


def test_并发评估只产生一条告警(client):
    client.post("/rules/reload", json={"rules": [
        {"name": "err", "metric": "error_count", "threshold": 1, "window_seconds": 60},
    ]})
    client.post("/metrics/report", json={
        "request_id": "only-err", "duration_ms": 10, "status_code": 500, "error": "E"})

    n = 24
    barrier = threading.Barrier(n)
    outcomes: list[int] = []
    lock = threading.Lock()

    def worker():
        barrier.wait()
        states = client.post("/alerts/evaluate").json()
        with lock:
            outcomes.append(len([s for s in states if s["rule"] == "err" and s["status"] == "firing"]))

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert outcomes and all(c == 1 for c in outcomes), outcomes
    engine = client.app.state.engine
    active = [s for s in engine.active() if s.rule == "err"]
    assert len(active) == 1
    assert active[0].count == n
