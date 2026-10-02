"""业务: 时间窗口告警判定与合并去重。

覆盖：窗口内同一异常只保留一条、持续触发升级、窗口到期明确收敛、
收敛后再触发为新周期、并发上报不重复告警也不漏计、规则删除仍按时收敛。
"""

import threading

import pytest

from app.alerts import AlertEngine
from app.rules import AlertRule


def make_rule(upgrade_after: int = 0, window: float = 10.0) -> AlertRule:
    return AlertRule(
        name="error_rate_high",
        metric="error_rate",
        threshold=0.5,
        window_seconds=window,
        severity="warning",
        upgrade_after=upgrade_after,
    )


def test_首次越阈触发_窗口内重复只合并为一条(engine):
    rule = make_rule()
    assert engine.evaluate(rule, 0.9, True, 0) == "triggered"
    for t in range(1, 6):
        assert engine.evaluate(rule, 0.9, True, float(t)) == "repeated"

    active = engine.active()
    assert len(active) == 1
    assert active[0].count == 6
    assert active[0].status == "firing"


def test_持续触发达到升级次数后升级且只升级一次(engine):
    rule = make_rule(upgrade_after=2)  # 第 1 次触发，第 2 次 repeated，第 3 次升级
    assert engine.evaluate(rule, 0.9, True, 0) == "triggered"
    assert engine.evaluate(rule, 0.9, True, 1) == "repeated"
    assert engine.evaluate(rule, 0.9, True, 2) == "upgraded"
    assert engine.active()[0].severity == "critical"
    # 再触发不再重复升级
    assert engine.evaluate(rule, 0.9, True, 3) == "repeated"
    assert engine.active()[0].severity == "critical"
    assert engine.active()[0].count == 4


def test_未越阈不产生告警(engine):
    rule = make_rule()
    assert engine.evaluate(rule, 0.1, False, 0) == "none"
    assert engine.active() == []


def test_单次未越阈不立即收敛_窗口到期才明确收敛(engine):
    rule = make_rule(window=10.0)
    engine.evaluate(rule, 0.9, True, 0)
    assert engine.evaluate(rule, 0.1, False, 5) == "none"
    assert len(engine.active()) == 1  # 抖动样本不应导致开合

    assert engine.sweep(9) == []          # 距离 last_seen=5 仅 4s
    resolved = engine.sweep(15)          # 距首次触发 15s >= 10s
    assert len(resolved) == 1
    state = resolved[0]
    assert state.status == "resolved" and state.resolved_at == 15

    all_states = engine.states()
    assert all_states[-1].status == "resolved"


def test_窗口内续期会推迟收敛(engine):
    rule = make_rule(window=10.0)
    engine.evaluate(rule, 0.9, True, 0)
    engine.evaluate(rule, 0.9, True, 8)   # 刷新 last_seen
    assert engine.sweep(9) == []           # 按旧窗口差点收敛
    assert engine.sweep(17) == []          # 距 last_seen=8 为 9s
    resolved = engine.sweep(18)           # 10s 整
    assert len(resolved) == 1


def test_收敛后再越阈视为新一轮告警(engine):
    rule = make_rule(window=10.0, upgrade_after=5)
    engine.evaluate(rule, 0.9, True, 0)
    engine.sweep(10)
    assert engine.evaluate(rule, 0.9, True, 20) == "triggered"
    active = engine.active()
    assert len(active) == 1 and active[0].count == 1 and active[0].severity == "warning"


def test_规则被删除后在飞告警仍按时收敛(engine):
    rule = make_rule(window=10.0)
    engine.evaluate(rule, 0.9, True, 0)
    # active_rules 传空（模拟规则删除）也必须能收敛
    resolved = engine.sweep(10, active_rules=[])
    assert len(resolved) == 1 and resolved[0].rule == "error_rate_high"


def test_并发上报只有一条告警且计数无丢失(engine):
    rule = make_rule()
    n = 64
    barrier = threading.Barrier(n)
    kinds: list[str] = []
    lock = threading.Lock()

    def worker(i: int):
        barrier.wait()
        kind = engine.evaluate(rule, 0.9, True, 0.0)
        with lock:
            kinds.append(kind)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    triggered = [k for k in kinds if k == "triggered"]
    assert len(triggered) == 1, "并发下只允许一条 triggered"
    assert len([k for k in kinds if k == "repeated"]) == n - 1
    active = engine.active()
    assert len(active) == 1 and active[0].count == n


def test_并发下升级恰好一次(engine):
    rule = make_rule(upgrade_after=2)
    n = 32
    barrier = threading.Barrier(n)
    kinds: list[str] = []
    lock = threading.Lock()

    def worker():
        barrier.wait()
        kind = engine.evaluate(rule, 0.9, True, 0.0)
        with lock:
            kinds.append(kind)

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert kinds.count("triggered") == 1
    assert kinds.count("upgraded") == 1, kinds
    assert engine.active()[0].severity == "critical"
    assert engine.active()[0].count == n


def test_外部拿到的状态副本不被内部修改影响(engine):
    rule = make_rule()
    engine.evaluate(rule, 0.9, True, 0)
    snapshot = engine.active()[0]
    snapshot.count = 999
    snapshot.severity = "critical"
    again = engine.active()[0]
    assert again.count == 1 and again.severity == "warning"


def test_不同规则互不污染(engine):
    r1 = AlertRule("a", "error_rate", 0.5, 10.0)
    r2 = AlertRule("b", "latency_p95", 500.0, 10.0)
    assert engine.evaluate(r1, 0.9, True, 0) == "triggered"
    assert engine.evaluate(r2, 600.0, True, 0) == "triggered"
    assert {a.rule for a in engine.active()} == {"a", "b"}
    resolved = engine.sweep(10, [r1, r2])
    assert {a.rule for a in resolved} == {"a", "b"}


def test_fingerprint按规则名稳定():
    assert AlertEngine.fingerprint("a") == "rule:a"
    assert AlertEngine.fingerprint("a") == AlertEngine.fingerprint("a")
    assert AlertEngine.fingerprint("a") != AlertEngine.fingerprint("b")
