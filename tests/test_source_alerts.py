"""业务: 按来源的告警判定与来源规则校验。

覆盖：同一规则在不同来源上各自独立的触发/去重/升级/收敛周期、
source="*" 展开评估、指定来源规则、老规则（无来源）兼容、
来源字段校验、并发混跑下各来源告警结论一致。
"""

import threading

import pytest

from app.alerts import AlertEngine
from app.evaluation import Evaluator
from app.metrics import DEFAULT_SOURCE, MetricsRegistry
from app.rules import AlertRule, RuleStore, RuleValidationError, validate_rule


def make_rule(source: str | None = None, upgrade_after: int = 0, window: float = 10.0) -> AlertRule:
    return AlertRule(
        name="err",
        metric="error_count",
        threshold=1.0,
        window_seconds=window,
        severity="warning",
        upgrade_after=upgrade_after,
        source=source,
    )


# ---------- 引擎层：同一规则多来源互不影响 ----------

def test_fingerprint带来源维度():
    assert AlertEngine.fingerprint("r") == "rule:r"  # 旧格式不变
    assert AlertEngine.fingerprint("r", "web") == "rule:r|source:web"
    assert AlertEngine.fingerprint("r", "web") != AlertEngine.fingerprint("r", "app")
    assert AlertEngine.fingerprint("r", "web") != AlertEngine.fingerprint("r")


def test_同一规则不同来源各自触发互不合并(engine):
    rule = make_rule()
    assert engine.evaluate(rule, 2.0, True, 0.0, source="web") == "triggered"
    assert engine.evaluate(rule, 2.0, True, 0.0, source="app") == "triggered"
    # 各自重复命中只合并到自己的告警上
    assert engine.evaluate(rule, 2.0, True, 1.0, source="web") == "repeated"

    active = sorted(engine.active(), key=lambda s: s.source)
    assert [s.source for s in active] == ["app", "web"]
    by_source = {s.source: s for s in active}
    assert by_source["web"].count == 2
    assert by_source["app"].count == 1  # 没被 web 的命中带着计数


def test_同一规则不同来源各自升级(engine):
    rule = make_rule(upgrade_after=1)
    engine.evaluate(rule, 2.0, True, 0.0, source="web")
    engine.evaluate(rule, 2.0, True, 0.0, source="app")
    # web 达到升级条件，app 不应跟着升级
    assert engine.evaluate(rule, 2.0, True, 1.0, source="web") == "upgraded"
    by_source = {s.source: s for s in engine.active()}
    assert by_source["web"].severity == "critical"
    assert by_source["app"].severity == "warning"


def test_一个来源收敛不影响其它来源(engine):
    rule = make_rule(window=10.0)
    engine.evaluate(rule, 2.0, True, 0.0, source="web")
    engine.evaluate(rule, 2.0, True, 8.0, source="app")  # app 的窗口更晚

    resolved = engine.sweep(10.0)  # web 到期，app 距 last_seen 仅 2s
    assert [s.source for s in resolved] == ["web"]
    remaining = engine.active()
    assert len(remaining) == 1 and remaining[0].source == "app"

    # web 收敛后再触发是新周期；app 仍在 firing 不受影响
    assert engine.evaluate(rule, 2.0, True, 11.0, source="web") == "triggered"
    by_source = {s.source: s for s in engine.active()}
    assert by_source["web"].count == 1
    assert by_source["app"].count == 1


def test_全局告警与来源告警互不干扰(engine):
    rule = make_rule()
    assert engine.evaluate(rule, 2.0, True, 0.0) == "triggered"  # 全局口径
    assert engine.evaluate(rule, 2.0, True, 0.0, source="web") == "triggered"
    assert len(engine.active()) == 2
    resolved = engine.sweep(10.0)
    assert len(resolved) == 2  # 各自按自己的窗口收敛


def test_并发多来源评估_每个来源恰好一条告警(engine):
    rule = make_rule()
    sources = [f"src-{i}" for i in range(8)]
    n_per = 16
    barrier = threading.Barrier(len(sources) * n_per)
    kinds: list[tuple[str, str]] = []
    lock = threading.Lock()

    def worker(src: str):
        barrier.wait()
        kind = engine.evaluate(rule, 2.0, True, 0.0, source=src)
        with lock:
            kinds.append((src, kind))

    threads = [
        threading.Thread(target=worker, args=(src,))
        for src in sources
        for _ in range(n_per)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    for src in sources:
        src_kinds = [k for s, k in kinds if s == src]
        assert src_kinds.count("triggered") == 1, (src, src_kinds)
        assert src_kinds.count("repeated") == n_per - 1
    active = engine.active()
    assert len(active) == len(sources)
    for state in active:
        assert state.count == n_per


# ---------- 规则校验：source 字段 ----------

def _raw(source):
    return {
        "name": "r",
        "metric": "error_rate",
        "threshold": 0.5,
        "window_seconds": 10,
        "source": source,
    }


def test_来源规则校验_合法取值():
    assert validate_rule(_raw("*")).source == "*"
    assert validate_rule(_raw("web")).source == "web"
    assert validate_rule(_raw(" web ")).source == "web"  # 去空白
    # 缺省为全局口径
    raw = _raw("*")
    del raw["source"]
    assert validate_rule(raw).source is None


@pytest.mark.parametrize("bad", ["", "   ", 123, "x" * 65])
def test_来源规则校验_非法取值可区分(bad):
    with pytest.raises(RuleValidationError) as exc:
        validate_rule(_raw(bad))
    assert "source" in str(exc.value)


def test_老规则集无source字段加载不失败():
    store = RuleStore()
    report = store.load([
        {"name": "old", "metric": "error_rate", "threshold": 0.5, "window_seconds": 60},
        {"name": "new", "metric": "error_count", "threshold": 1, "window_seconds": 60, "source": "*"},
    ])
    assert report.ok and report.loaded == 2
    rules = {r.name: r for r in store.current().rules}
    assert rules["old"].source is None
    assert rules["new"].source == "*"


# ---------- 评估编排：按来源展开 ----------

@pytest.fixture
def harness():
    registry = MetricsRegistry()
    engine = AlertEngine()
    store = RuleStore()
    t = {"now": 1000.0}
    evaluator = Evaluator(registry, engine, store, clock=lambda: t["now"])
    return registry, engine, store, evaluator, t


def test_按来源规则只命中异常来源_不被其它来源稀释(harness):
    registry, engine, store, evaluator, t = harness
    store.load([{"name": "per-src", "metric": "error_rate", "threshold": 0.5,
                 "window_seconds": 60.0, "source": "*"}])
    # web 全错（错误率 1.0），app 全好（0.0）：全局口径 2/6 不会越阈，
    # 按来源口径 web 必须触发、app 必须安静。
    for i in range(3):
        registry.record(f"w{i}", "t", 10, 500, error="E", source="web", now=t["now"])
        registry.record(f"a{i}", "t", 10, 200, source="app", now=t["now"])

    results = evaluator.evaluate_once()
    by_source = {r.source: r for r in results}
    assert set(by_source) == {"web", "app"}
    assert by_source["web"].kind == "triggered"
    assert by_source["web"].value == pytest.approx(1.0)
    assert by_source["app"].kind == "none"
    assert by_source["app"].value == pytest.approx(0.0)

    active = engine.active()
    assert len(active) == 1 and active[0].source == "web"


def test_按来源规则_未带来源的旧上报归入默认来源判定(harness):
    registry, engine, store, evaluator, t = harness
    store.load([{"name": "per-src", "metric": "error_count", "threshold": 1,
                 "window_seconds": 60.0, "source": "*"}])
    registry.record("old-1", "t", 10, 500, error="E", now=t["now"])  # 无来源

    results = evaluator.evaluate_once()
    assert len(results) == 1
    assert results[0].source == DEFAULT_SOURCE and results[0].kind == "triggered"


def test_指定来源规则只统计该来源(harness):
    registry, engine, store, evaluator, t = harness
    store.load([{"name": "only-web", "metric": "error_count", "threshold": 1,
                 "window_seconds": 60.0, "source": "web"}])
    registry.record("a1", "t", 10, 500, error="E", source="app", now=t["now"])
    assert evaluator.evaluate_once()[0].kind == "none"  # app 的错误不算

    registry.record("w1", "t", 10, 500, error="E", source="web", now=t["now"])
    result = evaluator.evaluate_once()[0]
    assert result.kind == "triggered" and result.source == "web"


def test_全局规则口径不受来源拆分影响(harness):
    registry, engine, store, evaluator, t = harness
    store.load([{"name": "global", "metric": "error_count", "threshold": 2,
                 "window_seconds": 60.0}])
    registry.record("a", "t", 10, 500, error="E", source="web", now=t["now"])
    registry.record("b", "t", 10, 500, error="E", source="app", now=t["now"])

    results = evaluator.evaluate_once()
    assert len(results) == 1 and results[0].source is None
    assert results[0].kind == "triggered" and results[0].value == 2.0
    assert engine.active()[0].fingerprint == "rule:global"  # 旧 fingerprint 格式


def test_按来源告警各自收敛_评估编排端到端(harness):
    registry, engine, store, evaluator, t = harness
    store.load([{"name": "per-src", "metric": "error_count", "threshold": 1,
                 "window_seconds": 10.0, "source": "*"}])
    registry.record("w1", "t", 10, 500, error="E", source="web", now=0.0)
    t["now"] = 0.0
    evaluator.evaluate_once()
    assert {s.source for s in engine.active()} == {"web"}

    # web 的样本老化出窗口后：web 自己收敛；app 新触发不受影响
    registry.record("a1", "t", 10, 500, error="E", source="app", now=9.0)
    t["now"] = 10.5
    evaluator.evaluate_once()
    assert {s.source for s in engine.active()} == {"app"}
    resolved_web = [s for s in engine.states() if s.source == "web"]
    assert resolved_web and all(s.status == "resolved" for s in resolved_web)

    # app 的窗口到期后也独立收敛，不会漏
    t["now"] = 21.0
    evaluator.evaluate_once()
    assert engine.active() == []
    assert all(s.status == "resolved" for s in engine.states())


def test_上报评估热更新并发混跑_各来源结论对得上(harness):
    registry, engine, store, evaluator, t = harness
    store.load([{"name": "per-src", "metric": "error_count", "threshold": 1,
                 "window_seconds": 600.0, "source": "*"}])
    sources = ["web", "app", "batch"]
    barrier = threading.Barrier(len(sources) + 1)

    def reporter(src: str):
        barrier.wait()
        for i in range(50):
            registry.record(f"{src}-{i}", "t", 10, 500, error="E",
                            source=src, now=t["now"])

    def evaluator_worker():
        barrier.wait()
        for _ in range(20):
            evaluator.evaluate_once(now=t["now"])

    threads = [threading.Thread(target=reporter, args=(s,)) for s in sources]
    threads.append(threading.Thread(target=evaluator_worker))
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    # 最终再评估一轮，此时全部记录已就位：每个来源都必须有且仅有一条 firing
    evaluator.evaluate_once(now=t["now"])
    active = engine.active()
    assert {s.source for s in active} == set(sources)
    assert len(active) == len(sources)  # 无重复告警
    # 每个来源的窗口样本数与注册表口径一致（不漏不重复）
    for src in sources:
        assert registry.snapshot(source=src).error_count == 50
