"""业务域：HTTP 接口端到端行为。"""
from __future__ import annotations


class TestReportAndMetricsApi:
    def test_report_is_idempotent_by_request_id(self, client):
        r1 = client.post("/api/report", json={"request_id": "api-1", "duration_ms": 1200})
        r2 = client.post("/api/report", json={"request_id": "api-1", "duration_ms": 5000})
        assert r1.json()["counted"] is True
        assert r2.json()["counted"] is False
        m = client.get("/api/metrics").json()
        assert m["total"] == 1 and m["avg_duration_ms"] == 1200.0

    def test_response_carries_request_id(self, client):
        r = client.post("/api/report", json={"duration_ms": 10},
                        headers={"X-Request-ID": "http-trace"})
        assert r.headers["x-request-id"] == "http-trace"
        assert r.json()["request_id"] == "http-trace"

    def test_demo_endpoint_instruments_itself(self, client):
        r = client.get("/api/demo", params={"fail": 1},
                       headers={"X-Request-ID": "demo-1"})
        assert r.json()["ok"] is False
        m = client.get("/api/metrics").json()
        assert m["error_count"] == 1


class TestAlertsApi:
    def test_slow_request_fires_visible_alert(self, client):
        client.post("/api/report", json={"request_id": "x1", "duration_ms": 5000})
        alerts = client.get("/api/alerts", params={"state": "firing"}).json()
        names = {a["rule_name"] for a in alerts}
        assert "slow_request" in names

    def test_invalid_state_returns_400(self, client):
        assert client.get("/api/alerts", params={"state": "bogus"}).status_code == 400


class TestRulesApi:
    def test_partial_reload_reports_rejections(self, client):
        r = client.post("/api/rules/reload", json=[
            {"name": "ok-rule", "metric": "latency_ms", "operator": ">",
             "threshold": 50},
            {"name": "bad-rule", "metric": "ghost"},
        ])
        assert r.status_code == 200
        body = r.json()
        assert body["loaded"] == ["ok-rule"]
        assert body["rejected"]["bad-rule"] == "metric_unknown"

    def test_all_bad_reload_keeps_last_version_422(self, client):
        before = client.get("/api/rules").json()
        r = client.post("/api/rules/reload", json=[{"name": "x", "metric": "nope"}])
        assert r.status_code == 422
        after = client.get("/api/rules").json()
        assert after["version"] == before["version"]
        assert {x["name"] for x in after["rules"]} == {x["name"] for x in before["rules"]}
