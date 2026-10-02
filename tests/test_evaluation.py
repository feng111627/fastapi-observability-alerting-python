"""业务: 时间窗口指标聚合与评估编排。

覆盖：各指标口径（error_rate/error_count/latency_avg/latency_p95）、
窗口边界只含窗口内样本、无样本不触发、评估前自动收敛、
热更新规则后评估按新版执行且旧告警按旧窗口收敛。
"""

import pytest

from app.alerts import AlertEngine
from app.evaluation import Evaluator, percentile_95
from app.metrics import MetricsRegistry
from app.rules import AlertRule, RuleStore


def test_p95最近秩口径():
    assert percentile_95([]) == 0.0
    assert percentile_95([1.0]) == 1.0
    assert percentile_95([float(i) for i in range(1, 101)]) == 95.0
    # 20 个样本，ceil(0.95*20)=19
    assert percentile_95([float(i) for i in range(1, 21)]) == 19.0


@pytest.fixture
def harness():
    registry = MetricsRegistry()
    engine = AlertEngine()
    store = RuleStore()
    t = {"now": 1000.0}
    evaluator = Evaluator(registry, engine, store, clock=lambda: t["now"])
    return registry, engine, store, evaluator, t


def rule(name="e", metric="error_rate", threshold=0.5, window=60.0, **kw):
    return AlertRule(name, metric, threshold, window, **kw)


def test_error_rate窗口聚合与触发(harness):
    registry, engine, store, evaluator, t = harness
    store.load([{"name": "e", "metric": "error_rate", "threshold": 0.5, "window_seconds": 60.0}])
    # 60s 窗口 [940,1000]
    registry.record("a", "tr", 10, 200, now=940.0)   # 边界包含
    registry.record("b", "tr", 10, 500, error="x", now=950.0)
    registry.record("c", "tr", 10, 500, error="x", now=999.0)
    registry.record("old", "tr", 10, 500, error="x", now=939.9)  # 窗口外

    results = evaluator.evaluate_once()
    _r, kind, value, _src = results[0]
    assert kind == "triggered"
    assert value == pytest.approx(2 / 3)  # 旧样本不计入


def test_窗口滑动后样本老化自动收敛(harness):
    registry, engine, store, evaluator, t = harness
    store.load([{"name": "e", "metric": "error_count", "threshold": 1, "window_seconds": 10.0}])
    registry.record("a", "tr", 10, 500, error="x", now=0.0)
    t["now"] = 0.0
    assert evaluator.evaluate_once()[0][1] == "triggered"

    t["now"] = 9.9
    assert evaluator.evaluate_once()[0][1] == "repeated"  # 样本仍在窗口，并刷新 last_seen

    t["now"] = 10.1
    kind = evaluator.evaluate_once()[0][1]
    assert kind == "none"
    assert len(engine.active()) == 1  # 距上次命中 0.2s，尚未到期收敛

    t["now"] = 19.9
    evaluator.evaluate_once()  # 评估前 sweep：19.9-9.9≈10s（含浮点容差），收敛
    assert engine.active() == []


def test_无样本不触发(harness):
    registry, engine, store, evaluator, t = harness
    store.load([{"name": "e", "metric": "error_rate", "threshold": 0.0, "window_seconds": 10.0}])
    results = evaluator.evaluate_once()
    assert results[0][1] == "none"
    assert engine.states() == []


def test_latency_avg与p95口径(harness):
    registry, engine, store, evaluator, t = harness
    for i in range(1, 101):
        registry.record(f"r{i}", "tr", float(i), 200, now=0.0)

    ravg = rule("avg", "latency_avg", threshold=50.0, window=60.0)
    store2 = RuleStore()
    ev2 = Evaluator(registry, AlertEngine(), store2, clock=lambda: 0.0)
    store2.load([{"name": "avg", "metric": "latency_avg", "threshold": 50.0, "window_seconds": 60.0}])
    value, n = ev2.compute(ravg, now=0.0)
    assert n == 100 and value == pytest.approx(50.5)

    rp95 = rule("p95", "latency_p95", threshold=90.0, window=60.0)
    value95, n95 = ev2.compute(rp95, now=0.0)
    assert n95 == 100 and value95 == 95.0


def test_规则热更新后按新版评估_单条坏规则不影响(harness):
    registry, engine, store, evaluator, t = harness
    registry.record("a", "tr", 10, 500, error="x", now=0.0)
    t["now"] = 0.0

    store.load([{"name": "v1", "metric": "error_count", "threshold": 100, "window_seconds": 60.0}])
    assert evaluator.evaluate_once()[0][1] == "none"  # 阈值 100，不触发

    report = store.load(
        [
            {"name": "v2", "metric": "error_count", "threshold": 1, "window_seconds": 60.0},
            {"name": "bad", "metric": "??", "threshold": 1, "window_seconds": 60.0},
        ]
    )
    assert report.loaded == 1 and report.rejected == 1
    results = evaluator.evaluate_once()
    assert len(results) == 1
    _r, kind, _v, _src = results[0]
    assert kind == "triggered" and _r.name == "v2"


def test_采样关闭的记录不参与窗口聚合(harness):
    registry, engine, store, evaluator, t = harness
    store.load([{"name": "e", "metric": "error_rate", "threshold": 0.1, "window_seconds": 60.0}])
    registry.record("a", "tr", 10, 500, error="x", sampled=False, now=0.0)
    results = evaluator.evaluate_once()
    assert results[0][1] == "none"
