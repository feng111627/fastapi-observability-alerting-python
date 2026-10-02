"""业务: 来源维度 HTTP 接口端到端。

覆盖：带来源上报与按来源查询、全局与来源口径对账、旧上报兼容、
来源超限归并的可观测接口、按来源规则的告警端到端生命周期。
"""

import pytest
from fastapi.testclient import TestClient

from app.main import MonotonicClock, create_app


class FakeClock:
    def __init__(self, start: float = 1000.0):
        self.now = start

    def time(self) -> float:
        return self.now


@pytest.fixture
def client():
    app = create_app(clock=FakeClock())
    with TestClient(app) as c:
        yield c


def _report(client, rid, source=None, status=200, error=None, duration=10.0):
    body = {"request_id": rid, "duration_ms": duration, "status_code": status}
    if source is not None:
        body["source"] = source
    if error is not None:
        body["error"] = error
    return client.post("/metrics/report", json=body)


def _business_snapshots(client):
    """/metrics/sources 中各来源快照（按 source 索引）。"""
    body = client.get("/metrics/sources").json()
    return body, {s["source"]: s for s in body["sources"]}


def test_带来源上报_可按来源分别查询(client):
    _report(client, "w1", source="web", status=500, error="E")
    _report(client, "w2", source="web")
    _report(client, "a1", source="app")

    web = client.get("/metrics", params={"source": "web"}).json()
    assert web["source"] == "web"
    assert web["total"] == 2 and web["error_count"] == 1
    app = client.get("/metrics", params={"source": "app"}).json()
    assert app["total"] == 1 and app["error_count"] == 0
    # 未知来源：空快照而不是报错
    unknown = client.get("/metrics", params={"source": "nope"}).json()
    assert unknown["total"] == 0


def test_旧上报不带来源_落入默认来源且全局对账(client):
    _report(client, "old-1", status=500, error="E")  # 不带来源
    _report(client, "w1", source="web")

    body, by_source = _business_snapshots(client)
    default = by_source["default"]
    assert default["total"] >= 1  # 中间件自记录也进默认来源
    old = client.app.state.registry.get("old-1")
    assert old.source == "default"

    # 全局 == 各来源之和（用注册表口径对账，排除接口噪声）
    reg = client.app.state.registry
    global_snap = reg.snapshot()
    assert sum(reg.snapshot(source=s).total for s in reg.sources()) == global_snap.total
    assert sum(reg.snapshot(source=s).error_count for s in reg.sources()) == global_snap.error_count


def test_来源总览接口_基数与归并可观察(client):
    _report(client, "w1", source="web")
    body, by_source = _business_snapshots(client)
    card = body["cardinality"]
    assert card["max_sources"] == 128
    assert card["overflowed"] is False and card["overflow_records"] == 0
    assert "web" in by_source and "default" in by_source


def test_来源超限_归并到溢出桶且接口可见():
    # 中间件自记录占用默认来源 1 席，故上限设为 3：default/a/b 各占一席
    app = create_app(clock=FakeClock(), max_sources=3)
    with TestClient(app) as client:
        _report(client, "k1", source="a")
        _report(client, "k2", source="b")
        _report(client, "k3", source="rogue-1", status=500, error="E")
        _report(client, "k4", source="rogue-2")

        body, by_source = _business_snapshots(client)
        card = body["cardinality"]
        assert card["overflowed"] is True
        assert card["overflow_records"] == 2
        assert set(card["overflow_sources_sample"]) == {"rogue-1", "rogue-2"}
        overflow = by_source["__overflow__"]
        assert overflow["total"] == 2 and overflow["error_count"] == 1
        # 已有来源口径未被溢出污染
        assert by_source["a"]["total"] == 1
        assert by_source["b"]["total"] == 1
        # 全局 == 各来源之和（同一时刻的注册表口径对账）
        reg = client.app.state.registry
        global_snap = reg.snapshot()
        assert sum(reg.snapshot(source=s).total for s in reg.sources()) == global_snap.total


def test_来源名超长被参数校验拒绝(client):
    r = _report(client, "x1", source="y" * 65)
    assert r.status_code == 422


def test_按来源规则端到端_异常来源触发正常来源安静(client):
    client.post("/rules/reload", json={"rules": [
        {"name": "per-src-err", "metric": "error_count", "threshold": 1,
         "window_seconds": 60, "source": "*"},
    ]})
    _report(client, "w1", source="web", status=500, error="E")
    _report(client, "a1", source="app")  # 正常请求

    states = client.post("/alerts/evaluate").json()
    firing = [s for s in states if s["rule"] == "per-src-err" and s["status"] == "firing"]
    assert len(firing) == 1
    assert firing[0]["source"] == "web"
    assert firing[0]["fingerprint"] == "rule:per-src-err|source:web"


def test_老规则不带来源_告警仍走全局口径(client):
    client.post("/rules/reload", json={"rules": [
        {"name": "global-err", "metric": "error_count", "threshold": 1, "window_seconds": 60},
    ]})
    _report(client, "w1", source="web", status=500, error="E")
    states = client.post("/alerts/evaluate").json()
    firing = [s for s in states if s["rule"] == "global-err" and s["status"] == "firing"]
    assert len(firing) == 1
    assert firing[0]["source"] is None
    assert firing[0]["fingerprint"] == "rule:global-err"


def test_按来源规则_各自收敛互不影响_端到端(client):
    clock = client.app.state.clock
    client.post("/rules/reload", json={"rules": [
        {"name": "per-src-err", "metric": "error_count", "threshold": 1,
         "window_seconds": 10, "source": "*"},
    ]})
    _report(client, "w1", source="web", status=500, error="E")
    client.post("/alerts/evaluate")
    assert {s.source for s in client.app.state.engine.active()} == {"web"}

    # web 窗口到期收敛；app 新触发，两者互不影响
    clock.now += 9
    _report(client, "a1", source="app", status=500, error="E")
    clock.now += 1.5  # web 距上次命中 10.5s -> 收敛；app 刚触发
    client.post("/alerts/evaluate")

    engine = client.app.state.engine
    assert {s.source for s in engine.active()} == {"app"}
    web_states = [s for s in engine.states() if s.source == "web"]
    assert web_states and all(s.status == "resolved" for s in web_states)
