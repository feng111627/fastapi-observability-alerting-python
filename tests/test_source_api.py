"""业务: 来源维度 HTTP 端到端。

覆盖：上报携带来源与响应回显生效来源、/metrics/sources 全局与分来源对账、
x-source 请求头进入中间件指标、上限归并的接口可观察性、
by_source 规则端到端告警、老规则集（无 by_source 字段）接口兼容。
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


def report(client, request_id, source=None, status=200, error=None, sampled=True):
    payload = {
        "request_id": request_id,
        "duration_ms": 10,
        "status_code": status,
        "sampled": sampled,
    }
    if error:
        payload["error"] = error
    if source is not None:
        payload["source"] = source
    return client.post("/metrics/report", json=payload)


def test_上报携带来源_响应回显生效来源(client):
    r = report(client, "s1", source="caller-a").json()
    assert r["accepted"] is True and r["source"] == "caller-a"
    # 不带来源的老上报：落入默认来源，行为与以前一致
    r2 = report(client, "s2").json()
    assert r2["accepted"] is True and r2["source"] == "default"


def test_按来源查询与全局对账(client):
    report(client, "a1", source="web", status=500, error="E")
    report(client, "a2", source="web")
    report(client, "b1", source="app", status=500, error="E")

    body = client.get("/metrics/sources").json()
    assert body["stats"]["overflowed"] is False
    by_source = body["by_source"]
    assert by_source["web"]["total"] == 2
    assert by_source["web"]["error_count"] == 1
    assert by_source["web"]["error_rate"] == pytest.approx(0.5)
    assert by_source["web"]["by_status"] == {"500": 1, "200": 1}
    assert by_source["app"]["error_count"] == 1

    # 全局 = 各来源之和（中间件自记录落在 default 桶，也参与对账）。
    # 对账走进程内注册表，避免两次 HTTP 读取之间又夹了新的自记录。
    reg = client.app.state.registry
    per_source = reg.snapshot_all_sources()
    global_snap = reg.snapshot()
    assert sum(s.total for s in per_source.values()) == global_snap.total
    assert sum(s.error_count for s in per_source.values()) == global_snap.error_count


def test_x_source请求头进入中间件指标(client):
    client.get("/metrics", headers={"x-source": "gateway"})
    by_source = client.get("/metrics/sources").json()["by_source"]
    assert "gateway" in by_source
    assert by_source["gateway"]["total"] == 1


def test_来源超限归并的接口可观察性():
    # 上限 3：中间件自记录占 default 桶 + web + app 恰好占满
    app = create_app(clock=FakeClock(), max_sources=3)
    with TestClient(app) as client:
        report(client, "k1", source="web")
        report(client, "k2", source="app")
        # 第四个新来源超限 -> 归并到保留桶，响应回显实际生效来源
        r = report(client, "k3", source="evil", status=500, error="E").json()
        assert r["accepted"] is True and r["source"] == "__overflow__"

        body = client.get("/metrics/sources").json()
        stats = body["stats"]
        assert stats["max_sources"] == 3
        assert stats["overflowed"] is True
        assert stats["overflow_records"] == 1
        assert stats["overflow_sources_seen"] == 1
        assert stats["overflow_source"] == "__overflow__"
        # 归并桶数据完整、全局仍对账（进程内同一时刻对账）
        assert body["by_source"]["__overflow__"]["error_count"] == 1
        reg = client.app.state.registry
        per_source = reg.snapshot_all_sources()
        assert sum(s.total for s in per_source.values()) == reg.snapshot().total


def test_by_source规则端到端_各来源独立告警(client):
    clock = client.app.state.clock
    r = client.post("/rules/reload", json={"rules": [
        {"name": "err", "metric": "error_count", "threshold": 1,
         "window_seconds": 10, "upgrade_after": 1, "by_source": True},
    ]})
    assert r.json()["ok"] is True

    # app 的错误落在首轮窗口边缘（995），下一轮评估即滑出窗口不再命中
    clock.now = 995.0
    report(client, "a1", source="app", status=500, error="E")
    clock.now = 1000.0
    report(client, "w1", source="web", status=500, error="E")

    states = client.post("/alerts/evaluate").json()
    firing = {s["source"]: s for s in states if s["rule"] == "err" and s["status"] == "firing"}
    assert set(firing) == {"web", "app"}
    assert firing["web"]["count"] == 1 and firing["app"]["count"] == 1

    # 只让 web 继续出错并再评估：web 升级，app 不被带着计数或升级
    clock.now = 1006.0
    report(client, "w2", source="web", status=500, error="E")
    states = client.post("/alerts/evaluate").json()
    firing = {s["source"]: s for s in states if s["rule"] == "err" and s["status"] == "firing"}
    assert firing["web"]["severity"] == "critical" and firing["web"]["count"] == 2
    assert firing["app"]["severity"] == "warning" and firing["app"]["count"] == 1

    # app 窗口到期独立收敛；web 续期后仍在
    clock.now = 1016.0
    report(client, "w3", source="web", status=500, error="E")
    states = client.post("/alerts/evaluate").json()
    err_states = [s for s in states if s["rule"] == "err"]
    app_resolved = [s for s in err_states if s["source"] == "app" and s["status"] == "resolved"]
    assert app_resolved, "app 应独立收敛"
    assert any(s["source"] == "web" and s["status"] == "firing" for s in err_states)


def test_老规则集接口兼容_无by_source字段(client):
    r = client.post("/rules/reload", json={"rules": [
        {"name": "err", "metric": "error_count", "threshold": 1, "window_seconds": 10},
    ]})
    body = r.json()
    assert body["ok"] is True and body["loaded"] == 1
    rules = client.get("/rules").json()["rules"]
    assert rules[0]["by_source"] is False

    report(client, "e1", source="web", status=500, error="E")
    states = client.post("/alerts/evaluate").json()
    firing = [s for s in states if s["rule"] == "err" and s["status"] == "firing"]
    assert len(firing) == 1 and firing[0]["source"] is None


def test_并发多来源上报接口层对账一致(client):
    sources = ["web", "app", "partner"]
    n_threads, per = 9, 30
    barrier = threading.Barrier(n_threads)

    def worker(tid: int):
        source = sources[tid % len(sources)]
        barrier.wait()
        for i in range(per):
            report(client, f"c{tid}-{i}", source=source,
                   status=500 if i % 2 else 200, error="E" if i % 2 else None)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    body = client.get("/metrics/sources").json()
    by_source = body["by_source"]
    for source in sources:
        assert by_source[source]["total"] == 3 * per
        assert by_source[source]["error_count"] == 3 * (per // 2)
    # 进程内同一时刻对账，避免两次 HTTP 读取之间夹入中间件自记录
    reg = client.app.state.registry
    per_source = reg.snapshot_all_sources()
    global_snap = reg.snapshot()
    assert sum(s.total for s in per_source.values()) == global_snap.total
    assert sum(s.error_count for s in per_source.values()) == global_snap.error_count
