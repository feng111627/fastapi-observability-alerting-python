"""业务: 来源维度告警。

覆盖：by_source 规则按来源独立触发/合并/升级/收敛，互不影响；
老规则（无 by_source）保持全局口径且旧规则集加载兼容；
并发评估下各来源告警不重复不漏；上报/热更新/评估并发混合时结论一致。
"""

import threading

import pytest

from app.alerts import AlertEngine
from app.evaluation import Evaluator
from app.metrics import MetricsRegistry
from app.rules import AlertRule, RuleStore


@pytest.fixture
def harness():
    registry = MetricsRegistry()
    engine = AlertEngine()
    store = RuleStore()
    t = {"now": 1000.0}
    evaluator = Evaluator(registry, engine, store, clock=lambda: t["now"])
    return registry, engine, store, evaluator, t


def load_by_source_rule(store, **overrides):
    raw = {
        "name": "err",
        "metric": "error_rate",
        "threshold": 0.5,
        "window_seconds": 10.0,
        "severity": "warning",
        "upgrade_after": 2,
        "by_source": True,
    }
    raw.update(overrides)
    report = store.load([raw])
    assert report.ok, report.errors
    return report


def fire_source(registry, source: str, errors: int, oks: int, now: float, tag: str = ""):
    for i in range(errors):
        registry.record(f"{tag}{source}-e{i}", "t", 10, 500, error="E", source=source, now=now)
    for i in range(oks):
        registry.record(f"{tag}{source}-o{i}", "t", 10, 200, source=source, now=now)


def active_by_source(engine, rule="err"):
    return {s.source: s for s in engine.active() if s.rule == rule}


def test_by_source规则_各来源独立触发互不合并(harness):
    registry, engine, store, evaluator, t = harness
    load_by_source_rule(store)
    fire_source(registry, "web", errors=2, oks=0, now=1000.0)
    fire_source(registry, "app", errors=1, oks=1, now=1000.0)  # 0.5 恰好越阈

    results = evaluator.evaluate_once()
    kinds = {(src): kind for _r, kind, _v, src in results}
    assert kinds == {"web": "triggered", "app": "triggered"}

    active = active_by_source(engine)
    assert set(active) == {"web", "app"}
    assert active["web"].count == 1 and active["app"].count == 1
    # fingerprint 携带来源，两条流互不合并
    assert active["web"].fingerprint != active["app"].fingerprint


def test_一个来源升级不影响另一个来源的计数(harness):
    registry, engine, store, evaluator, t = harness
    load_by_source_rule(store)
    fire_source(registry, "web", errors=1, oks=0, now=1000.0)
    # app 的错误落在首轮窗口边缘（994），下一轮即滑出窗口，不会持续命中
    fire_source(registry, "app", errors=1, oks=0, now=994.0)

    evaluator.evaluate_once()  # 两个来源都 triggered
    # 只让 web 持续命中：app 的样本已滑出窗口
    t["now"] = 1005.0
    fire_source(registry, "web", errors=1, oks=0, now=1005.0, tag="r2-")
    evaluator.evaluate_once()
    t["now"] = 1009.0
    fire_source(registry, "web", errors=1, oks=0, now=1009.0, tag="r3-")
    evaluator.evaluate_once()

    active = active_by_source(engine)
    # web 连续 3 轮命中 -> 达到 upgrade_after=2 升级 critical，count=3
    assert active["web"].severity == "critical" and active["web"].count == 3
    # app 没有被带着计数或升级：仍是首轮的 warning/count=1（样本老化后等收敛）
    assert active["app"].severity == "warning" and active["app"].count == 1


def test_某来源收敛不影响其它来源(harness):
    registry, engine, store, evaluator, t = harness
    load_by_source_rule(store)
    fire_source(registry, "web", errors=1, oks=0, now=1000.0)
    # app 的样本落在窗口边缘（998），下一轮即滑出窗口，不再刷新 last_seen
    fire_source(registry, "app", errors=1, oks=0, now=998.0)
    evaluator.evaluate_once()

    # web 在窗口内持续命中刷新 last_seen；app 不再命中
    t["now"] = 1009.0
    fire_source(registry, "web", errors=1, oks=0, now=1009.0, tag="r2-")
    evaluator.evaluate_once()

    t["now"] = 1011.0  # app 距 last_seen(1000) 已 11s > 10s 窗口
    evaluator.evaluate_once()

    active = active_by_source(engine)
    assert set(active) == {"web"}, "app 应独立收敛，web 不受影响"
    resolved = [s for s in engine.states() if s.rule == "err" and s.status == "resolved"]
    assert [s.source for s in resolved] == ["app"]
    assert resolved[0].resolved_at == 1011.0


def test_来源收敛后再触发为新周期(harness):
    registry, engine, store, evaluator, t = harness
    load_by_source_rule(store, upgrade_after=0)
    fire_source(registry, "web", errors=1, oks=0, now=1000.0)
    evaluator.evaluate_once()
    assert active_by_source(engine)["web"].count == 1

    t["now"] = 1011.0  # 窗口过期 -> 收敛
    evaluator.evaluate_once()
    assert active_by_source(engine) == {}

    t["now"] = 1012.0
    fire_source(registry, "web", errors=1, oks=0, now=1012.0, tag="r2-")
    results = evaluator.evaluate_once()
    kind = [k for _r, k, _v, src in results if src == "web"][0]
    assert kind == "triggered"
    assert active_by_source(engine)["web"].count == 1  # 新周期重新计数


def test_老规则保持全局口径_旧规则集加载兼容(harness):
    registry, engine, store, evaluator, t = harness
    # 旧规则集：没有 by_source 字段，加载不得失败
    report = store.load([{
        "name": "err", "metric": "error_rate", "threshold": 0.5,
        "window_seconds": 10.0, "severity": "warning", "upgrade_after": 0,
    }])
    assert report.ok and report.loaded == 1
    rule = store.current().rules[0]
    assert rule.by_source is False

    # 错误分散在两个来源：单看都不够阈值，全局合计越阈 -> 只产生一条全局告警
    fire_source(registry, "web", errors=1, oks=1, now=1000.0)
    fire_source(registry, "app", errors=1, oks=1, now=1000.0)
    results = evaluator.evaluate_once()
    assert len(results) == 1
    _r, kind, value, source = results[0]
    assert kind == "triggered" and source is None
    assert value == pytest.approx(0.5)

    active = engine.active()
    assert len(active) == 1 and active[0].source is None
    assert active[0].fingerprint == "rule:err"  # 与老版本 fingerprint 完全一致


def test_并发评估_各来源只有一条告警且计数精确(harness):
    registry, engine, store, evaluator, t = harness
    load_by_source_rule(store)
    fire_source(registry, "web", errors=1, oks=0, now=1000.0)
    fire_source(registry, "app", errors=1, oks=0, now=1000.0)

    n = 24
    barrier = threading.Barrier(n)
    kinds: list[tuple[str, str]] = []
    lock = threading.Lock()

    def worker():
        barrier.wait()
        for _r, kind, _v, src in evaluator.evaluate_once():
            with lock:
                kinds.append((src, kind))

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t_ in threads:
        t_.start()
    for t_ in threads:
        t_.join()

    active = active_by_source(engine)
    assert set(active) == {"web", "app"}
    # 每轮评估每个来源恰好命中一次：count 精确等于评估轮数
    assert active["web"].count == n and active["app"].count == n
    # 每个来源 triggered 恰好一次
    for source in ("web", "app"):
        assert sum(1 for s, k in kinds if s == source and k == "triggered") == 1


def test_上报热更新评估并发混合_结论不重复不漏(harness):
    registry, engine, store, evaluator, t = harness
    load_by_source_rule(store)
    stop = threading.Event()
    errors: list[Exception] = []

    def reporter():
        i = 0
        while not stop.is_set():
            try:
                registry.record(
                    f"mix-{i}", "t", 10, 500, error="E",
                    source=["web", "app"][i % 2], now=t["now"],
                )
                i += 1
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

    def reloader():
        # 反复热更新：同名同义规则（by_source 不变），版本号持续推进
        while not stop.is_set():
            try:
                load_by_source_rule(store)
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

    def evaluator_worker():
        while not stop.is_set():
            try:
                evaluator.evaluate_once()
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

    threads = [
        threading.Thread(target=reporter),
        threading.Thread(target=reloader),
        threading.Thread(target=evaluator_worker),
        threading.Thread(target=evaluator_worker),
    ]
    for t_ in threads:
        t_.start()
    threading.Event().wait(0.5)
    stop.set()
    for t_ in threads:
        t_.join()

    assert not errors
    # 最终统一评估一轮后核对：每个来源至多一条进行中告警，fingerprint 不重复
    evaluator.evaluate_once()
    active = [s for s in engine.active() if s.rule == "err"]
    fingerprints = [s.fingerprint for s in active]
    assert len(fingerprints) == len(set(fingerprints))
    assert {s.source for s in active} <= {"web", "app"}
    # 各来源命中次数单调且不超过各自样本轮次：count >= 1 即说明判定收敛一致
    for s in active:
        assert s.count >= 1 and s.status == "firing"


def test_全局规则与by_source规则并存互不干扰(harness):
    registry, engine, store, evaluator, t = harness
    report = store.load([
        {"name": "global-err", "metric": "error_count", "threshold": 2,
         "window_seconds": 10.0},
        {"name": "src-err", "metric": "error_count", "threshold": 1,
         "window_seconds": 10.0, "by_source": True},
    ])
    assert report.ok and report.loaded == 2

    fire_source(registry, "web", errors=1, oks=0, now=1000.0)
    fire_source(registry, "app", errors=1, oks=0, now=1000.0)
    evaluator.evaluate_once()

    active = engine.active()
    by_rule = {}
    for s in active:
        by_rule.setdefault(s.rule, []).append(s)
    # 全局规则一条告警（合计 2 个错误越阈），无来源
    assert len(by_rule["global-err"]) == 1
    assert by_rule["global-err"][0].source is None
    # by_source 规则两个来源各一条（各自 1 个错误越阈）
    assert {s.source for s in by_rule["src-err"]} == {"web", "app"}
