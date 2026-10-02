"""业务域：时间窗口告警判定、合并去重、升级与收敛。"""
from __future__ import annotations

import concurrent.futures

from app.alerts import AlertManager
from app.models import AlertRule, AlertState, RequestSample


def _latency_rule(**kw) -> AlertRule:
    base = dict(name="slow", metric="latency_ms", operator=">", threshold=100,
                window_s=10, for_s=0, cooldown_s=5, resolve_s=10)
    base.update(kw)
    return AlertRule(**base)


def _err_rule(**kw) -> AlertRule:
    base = dict(name="err", metric="error_rate", operator=">", threshold=0.5,
                window_s=100, for_s=5, cooldown_s=5, resolve_s=30)
    base.update(kw)
    return AlertRule(**base)


class TestFireAndDedup:
    """业务：同一异常窗口内只保留一条有效告警，重复触发不重复告警。"""

    def test_first_breach_fires_once(self):
        mgr = AlertManager()
        mgr.update_rules([_latency_rule()])
        r1 = mgr.ingest(RequestSample("a", 0.0, 200, 200))
        assert len(r1.fired) == 1 and r1.fired[0].state == AlertState.FIRING
        r2 = mgr.ingest(RequestSample("b", 1.0, 300, 200))
        assert r2.fired == [] and r2.upgraded == []
        firing = mgr.list_alerts(AlertState.FIRING)
        assert len(firing) == 1 and firing[0].fingerprint == "slow"

    def test_non_breaching_does_not_alert(self):
        mgr = AlertManager()
        mgr.update_rules([_latency_rule()])
        r = mgr.ingest(RequestSample("a", 0.0, 50, 200))
        assert r.fired == [] and mgr.list_alerts() == []


class TestPendingAndEscalation:
    """业务：持续触发先 PENDING 后 FIRING，并可逐级升级。"""

    def test_pending_then_fire_after_for_duration(self):
        mgr = AlertManager()
        mgr.update_rules([_err_rule()])
        assert mgr.ingest(RequestSample("a", 0, 10, 500)).fired == []
        assert mgr.list_alerts(AlertState.PENDING)
        res = mgr.ingest(RequestSample("b", 5, 10, 500))
        assert len(res.fired) == 1 and res.fired[0].level == 1

    def test_pending_recovers_silently(self):
        mgr = AlertManager()
        mgr.update_rules([_err_rule()])
        mgr.ingest(RequestSample("a", 0, 10, 500))
        mgr.evaluate_at(101)  # 窗口清空，错误率无数据 -> 未持续
        assert mgr.list_alerts() == []

    def test_firing_escalates_without_duplicate_alert(self):
        mgr = AlertManager()
        mgr.update_rules([_err_rule()])
        mgr.ingest(RequestSample("a", 0, 10, 500))
        res = mgr.ingest(RequestSample("b", 5, 10, 500))  # FIRING level1
        assert len(res.fired) == 1
        res = mgr.ingest(RequestSample("c", 11, 10, 500))  # +for_s -> level2
        assert res.fired == [] and len(res.upgraded) == 1
        assert res.upgraded[0].level == 2
        assert len(mgr.list_alerts()) == 1  # 始终只有一条


class TestResolve:
    """业务：异常结束后明确收敛。"""

    def test_firing_resolves_after_silence(self):
        mgr = AlertManager()
        mgr.update_rules([_latency_rule()])
        mgr.ingest(RequestSample("a", 0.0, 200, 200))
        res = mgr.evaluate_at(11.5)
        assert len(res.resolved) == 1
        assert res.resolved[0].state == AlertState.RESOLVED

    def test_resolve_then_reopen_within_cooldown_is_silent(self):
        mgr = AlertManager()
        mgr.update_rules([_latency_rule()])
        mgr.ingest(RequestSample("a", 0.0, 200, 200))
        mgr.evaluate_at(11.5)  # resolve
        res = mgr.ingest(RequestSample("b", 12.0, 200, 200))  # cooldown 5s 内
        assert res.fired == []
        assert len(mgr.list_alerts(AlertState.FIRING)) == 1

    def test_new_incident_after_cooldown_is_a_new_alert(self):
        mgr = AlertManager()
        mgr.update_rules([_latency_rule()])
        mgr.ingest(RequestSample("a", 0.0, 200, 200))
        mgr.evaluate_at(11.5)   # resolve at 11.5
        mgr.ingest(RequestSample("b", 12.0, 200, 200))  # reopen（静默）
        mgr.evaluate_at(25.0)   # 再次 resolve
        res = mgr.ingest(RequestSample("c", 31.0, 200, 200))  # 31-25>=5 新告警
        assert len(res.fired) == 1 and res.fired[0].level == 1


class TestConcurrentAlerts:
    """业务：并发上报下不得重复告警，也不得漏告警。"""

    def test_concurrent_breaches_produce_single_alert(self):
        mgr = AlertManager()
        mgr.update_rules([_latency_rule()])

        def task(i: int):
            return mgr.ingest(RequestSample(f"cc-{i}", 0.0, 200, 200))

        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as pool:
            results = list(pool.map(task, range(64)))
        total_fired = sum(len(r.fired) for r in results)
        total_upgraded = sum(len(r.upgraded) for r in results)
        assert total_fired == 1
        assert total_upgraded == 0
        assert len(mgr.list_alerts(AlertState.FIRING)) == 1

    def test_concurrent_independent_rules_each_alert(self):
        mgr = AlertManager()
        mgr.update_rules([_latency_rule(), _err_rule(name="err")])

        def slow_task(i: int):
            # 又慢又错：latency 立即触发，error_rate 处于 PENDING
            return mgr.ingest(RequestSample(f"s-{i}", 0.0, 200, 500))

        def err_task(i: int):
            return mgr.ingest(RequestSample(f"e-{i}", 0.0, 10, 500))

        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
            slow = list(pool.map(slow_task, range(32)))
            errs = list(pool.map(err_task, range(32)))
        fired = {a.rule_name for r in slow + errs for a in r.fired}
        assert "slow" in fired  # for_s=0 立即触发
        assert "err" not in fired  # for_s=5，当前仍 PENDING
        assert len(mgr.list_alerts(AlertState.PENDING)) == 1
