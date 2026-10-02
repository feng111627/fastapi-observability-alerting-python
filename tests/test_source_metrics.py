"""业务: 来源维度指标与来源基数上限。

覆盖：默认来源归集、按来源快照与全局汇总对账、重复/并发/采样在
来源维度下的隔离与一致性、来源数量超限后的归并策略与可观测性。
"""

import threading

import pytest

from app.metrics import (
    DEFAULT_SOURCE,
    MAX_SOURCE_LEN,
    OVERFLOW_SOURCE,
    MetricsRegistry,
    normalize_source,
)


def test_未携带来源落入默认来源(registry):
    registry.record("a", "t", 10.0, 200, sampled=True)
    registry.record("b", "t", 10.0, 200, sampled=True, source=None)
    registry.record("c", "t", 10.0, 200, sampled=True, source="   ")  # 空白视同未带

    snap = registry.snapshot(source=DEFAULT_SOURCE)
    assert snap.total == 3 and snap.sampled == 3
    assert registry.sources() == [DEFAULT_SOURCE]
    # 全局与唯一来源完全一致
    global_snap = registry.snapshot()
    assert global_snap.total == snap.total and global_snap.sampled == snap.sampled


def test_来源名归一化_空白与截断():
    assert normalize_source(None) == DEFAULT_SOURCE
    assert normalize_source("  ") == DEFAULT_SOURCE
    assert normalize_source(" web ") == "web"
    long_name = "x" * (MAX_SOURCE_LEN + 50)
    assert normalize_source(long_name) == "x" * MAX_SOURCE_LEN


def test_按来源快照互相独立且与全局对账(registry):
    registry.record("g1", "t", 100.0, 200, source="web")
    registry.record("g2", "t", 200.0, 500, error="E", source="web")
    registry.record("g3", "t", 300.0, 503, error="E", source="app")
    registry.record("g4", "t", 50.0, 200, source="app", sampled=False)  # 未采样
    registry.record("g5", "t", 10.0, 404)  # 默认来源

    web = registry.snapshot(source="web")
    assert web.total == 2 and web.sampled == 2
    assert web.error_count == 1 and web.by_status == {200: 1, 500: 1}
    assert web.sum_duration_ms == 300.0

    app = registry.snapshot(source="app")
    assert app.total == 2 and app.sampled == 1  # 未采样计入 total 不进口径
    assert app.error_count == 1 and app.by_status == {503: 1}

    default = registry.snapshot(source=DEFAULT_SOURCE)
    assert default.total == 1 and default.by_status == {404: 1}
    assert default.error_count == 0  # 4xx 不算错误

    # 全局 == 各来源之和（total/sampled/error/耗时/状态码分布逐项对账）
    global_snap = registry.snapshot()
    parts = [web, app, default]
    assert global_snap.total == sum(p.total for p in parts)
    assert global_snap.sampled == sum(p.sampled for p in parts)
    assert global_snap.error_count == sum(p.error_count for p in parts)
    assert global_snap.sum_duration_ms == sum(p.sum_duration_ms for p in parts)
    merged: dict[int, int] = {}
    for p in parts:
        for code, n in p.by_status.items():
            merged[code] = merged.get(code, 0) + n
    assert global_snap.by_status == merged


def test_同一request_id跨来源重复上报只计一次且不消耗来源配额(registry):
    assert registry.record("r1", "t", 10.0, 200, source="web") is True
    # 同一 id 换个来源再报：忽略，且不把新来源登记进基数
    assert registry.record("r1", "t", 20.0, 500, source="app") is False

    assert registry.get("r1").source == "web"
    assert registry.sources() == ["web"]
    assert registry.snapshot().total == 1
    assert registry.snapshot(source="app").total == 0


def test_查询未知来源返回空快照不报错(registry):
    registry.record("a", "t", 10.0, 200, source="web")
    snap = registry.snapshot(source="no-such-source")
    assert snap.total == 0 and snap.sampled == 0
    assert snap.error_rate == 0.0 and snap.avg_duration_ms == 0.0


def test_并发多来源上报_隔离且全局对账(registry):
    n_threads, per = 8, 150
    sources = [f"src-{i}" for i in range(n_threads)]
    barrier = threading.Barrier(n_threads)

    def worker(idx: int):
        barrier.wait()
        for i in range(per):
            registry.record(
                f"{sources[idx]}-{i}", "t", float(i),
                500 if i % 2 else 200,
                error="E" if i % 2 else None,
                source=sources[idx],
            )

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 每个来源各自的口径精确，互不污染
    for src in sources:
        snap = registry.snapshot(source=src)
        assert snap.total == per and snap.sampled == per
        assert snap.error_count == per // 2
        assert snap.by_status == {200: per // 2, 500: per // 2}

    # 全局 == 各来源之和
    global_snap = registry.snapshot()
    assert global_snap.total == n_threads * per
    assert global_snap.error_count == n_threads * per // 2
    assert sum(registry.snapshot(source=s).total for s in registry.sources()) == global_snap.total


def test_并发同一id不同来源抢报只有一条生效(registry):
    n = 32
    barrier = threading.Barrier(n)
    accepted: list[bool] = []
    lock = threading.Lock()

    def worker(i: int):
        barrier.wait()
        ok = registry.record("race-id", "t", 10.0, 200, source=f"src-{i}")
        with lock:
            accepted.append(ok)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sum(accepted) == 1
    assert registry.snapshot().total == 1
    # 只有赢家那个来源被登记，其余来源不进入口径
    assert len(registry.sources()) == 1


def test_采样开关在来源维度下互不污染(registry):
    registry.record("a", "t", 100.0, 500, error="E", sampled=False, source="web")
    registry.record("b", "t", 100.0, 500, error="E", sampled=True, source="app")

    web = registry.snapshot(source="web")
    assert web.total == 1 and web.sampled == 0 and web.error_count == 0
    app = registry.snapshot(source="app")
    assert app.sampled == 1 and app.error_count == 1
    # 全局口径同样只对采样记录统计
    assert registry.snapshot().error_count == 1


def test_来源超限归并到溢出桶且已统计数字不变():
    reg = MetricsRegistry(max_sources=3)
    reg.record("k0", "t", 10.0, 200)  # 默认来源占 1 席
    reg.record("k1", "t", 10.0, 200, source="a")
    reg.record("k2", "t", 10.0, 200, source="b")
    # 已满 3 席，新来源 c / d 归并到溢出桶
    reg.record("k3", "t", 10.0, 500, error="E", source="c")
    reg.record("k4", "t", 20.0, 200, source="d")
    reg.record("k5", "t", 30.0, 200, source="c")  # c 的记录也进溢出桶

    assert reg.get("k3").source == OVERFLOW_SOURCE
    assert reg.get("k5").source == OVERFLOW_SOURCE

    # 已有来源的口径不受溢出影响
    assert reg.snapshot(source="a").total == 1
    assert reg.snapshot(source="b").total == 1

    overflow = reg.snapshot(source=OVERFLOW_SOURCE)
    assert overflow.total == 3 and overflow.error_count == 1

    # 归并后全局仍 == 各来源之和，一笔不重复也不漏
    global_snap = reg.snapshot()
    per_source_total = sum(reg.snapshot(source=s).total for s in reg.sources())
    assert global_snap.total == 6 == per_source_total

    # 归并可观察：溢出标记、归并记录数、被归并的原始来源样本
    card = reg.cardinality()
    assert card.max_sources == 3 and card.active_sources == 3
    assert card.overflowed is True and card.overflow_records == 3
    assert set(card.overflow_sources_sample) == {"c", "d"}


def test_溢出桶中的来源查询原始名返回空快照():
    reg = MetricsRegistry(max_sources=1)
    reg.record("k1", "t", 10.0, 200, source="real")
    reg.record("k2", "t", 10.0, 200, source="overflowed-src")
    # 原始名已被归并，只能经由溢出桶看到
    assert reg.snapshot(source="overflowed-src").total == 0
    assert reg.snapshot(source=OVERFLOW_SOURCE).total == 1


def test_重复上报不增加溢出计数():
    reg = MetricsRegistry(max_sources=1)
    reg.record("k1", "t", 10.0, 200, source="real")
    reg.record("k2", "t", 10.0, 200, source="junk")
    assert reg.cardinality().overflow_records == 1
    # 同一 id 重复上报：去重优先，不重复计溢出
    assert reg.record("k2", "t", 10.0, 200, source="junk") is False
    assert reg.cardinality().overflow_records == 1


def test_超限后乱造来源不会拖垮内存():
    reg = MetricsRegistry(max_sources=2)
    reg.record("k0", "t", 10.0, 200, source="keep")
    reg.record("k1", "t", 10.0, 200, source="keep2")
    for i in range(5000):
        reg.record(f"junk-{i}", "t", 1.0, 200, source=f" junk-src-{i} ")
    card = reg.cardinality()
    assert card.active_sources == 2
    assert card.overflow_records == 5000
    # 观测样本有界，不随乱造来源数量增长
    assert len(card.overflow_sources_sample) <= 64
    assert len(reg.sources()) == 3  # keep / keep2 / __overflow__
    # 有效来源口径未被污染
    assert reg.snapshot(source="keep").total == 1
    assert reg.snapshot(source=OVERFLOW_SOURCE).total == 5000


def test_max_sources非法快速失败():
    with pytest.raises(ValueError):
        MetricsRegistry(max_sources=0)
