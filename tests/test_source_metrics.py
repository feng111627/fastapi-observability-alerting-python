"""业务: 来源维度指标与上限归并。

覆盖：缺省/空白来源落入 default、来源名归一化、按来源快照与全局对账、
来源间隔离、并发多来源上报一致性、同 id 跨来源重复上报 first-write-wins、
采样开关在来源维度下的口径、来源数超限后的归并与可观察性。
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


def test_缺省与空白来源落入default(registry):
    assert registry.record("a", "t", 10, 200) is True
    assert registry.record("b", "t", 10, 200, source=None) is True
    assert registry.record("c", "t", 10, 200, source="") is True
    assert registry.record("d", "t", 10, 200, source="   ") is True

    assert registry.sources() == [DEFAULT_SOURCE]
    snap = registry.snapshot_by_source(None)
    assert snap.total == 4
    # 全局与默认来源口径一致
    assert registry.snapshot().total == 4


def test_来源名归一化_去空白与超长截断(registry):
    assert normalize_source("  caller-a  ") == "caller-a"
    assert normalize_source("x" * 500) == "x" * MAX_SOURCE_LEN
    assert normalize_source(None) == DEFAULT_SOURCE
    assert normalize_source(123) == DEFAULT_SOURCE  # 非字符串兜底

    registry.record("a", "t", 10, 200, source="  web  ")
    registry.record("b", "t", 10, 200, source="web")
    # 归一化后是同一来源，只算一个来源桶
    assert registry.sources() == ["web"]
    assert registry.snapshot_by_source("web").total == 2


def test_按来源快照与全局对账(registry):
    registry.record("a1", "t", 10, 200, source="web")
    registry.record("a2", "t", 20, 500, error="E", source="web")
    registry.record("b1", "t", 30, 404, source="app")       # 4xx 不计错误
    registry.record("b2", "t", 40, 500, error="E", source="app", sampled=False)
    registry.record("c1", "t", 50, 200)                     # 默认来源

    global_snap = registry.snapshot()
    per_source = registry.snapshot_all_sources()
    assert set(per_source) == {"web", "app", DEFAULT_SOURCE}

    # 各来源之和 == 全局（total/sampled/error_count/耗时/状态码分布逐项对账）
    assert sum(s.total for s in per_source.values()) == global_snap.total
    assert sum(s.sampled for s in per_source.values()) == global_snap.sampled
    assert sum(s.error_count for s in per_source.values()) == global_snap.error_count
    assert sum(s.sum_duration_ms for s in per_source.values()) == pytest.approx(
        global_snap.sum_duration_ms
    )
    merged: dict[int, int] = {}
    for s in per_source.values():
        for code, n in s.by_status.items():
            merged[code] = merged.get(code, 0) + n
    assert merged == global_snap.by_status


def test_来源之间口径隔离(registry):
    registry.record("w1", "t", 10, 200, source="web")
    registry.record("w2", "t", 30, 200, source="web")
    registry.record("m1", "t", 100, 500, error="E", source="mobile")

    web = registry.snapshot_by_source("web")
    mobile = registry.snapshot_by_source("mobile")
    assert web.error_rate == 0.0 and web.avg_duration_ms == pytest.approx(20.0)
    assert web.by_status == {200: 2}
    assert mobile.error_rate == 1.0 and mobile.avg_duration_ms == pytest.approx(100.0)
    assert mobile.by_status == {500: 1}
    # 未知来源返回全零快照而不是报错
    empty = registry.snapshot_by_source("no-such-source")
    assert empty.total == 0 and empty.error_rate == 0.0


def test_并发多来源上报_隔离且对账一致():
    registry = MetricsRegistry()
    sources = ["web", "app", "partner"]
    n_threads, per = 9, 60
    barrier = threading.Barrier(n_threads)

    def worker(tid: int):
        source = sources[tid % len(sources)]
        barrier.wait()
        for i in range(per):
            registry.record(
                f"{source}-{tid}-{i}", "t", float(i),
                500 if i % 3 == 0 else 200,
                error="E" if i % 3 == 0 else None,
                source=source,
                now=0.0,
            )

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    per_source = registry.snapshot_all_sources()
    global_snap = registry.snapshot()
    # 每个来源恰好 3 个线程 * 60 条，互不串扰
    for source in sources:
        snap = per_source[source]
        assert snap.total == 3 * per
        assert snap.error_count == 3 * (per // 3)  # 每线程 i%3==0 计错误
    assert sum(s.total for s in per_source.values()) == global_snap.total
    assert sum(s.error_count for s in per_source.values()) == global_snap.error_count
    assert global_snap.total == n_threads * per


def test_同一request_id跨来源重复上报_先到为准(registry):
    assert registry.record("k1", "t", 10, 500, error="E", source="web") is True
    # 同一 id 换个来源再报：忽略，不重复计数也不改来源归属
    assert registry.record("k1", "t", 99, 200, source="mobile") is False

    assert registry.sources() == ["web"]
    assert registry.snapshot().total == 1
    assert registry.snapshot_by_source("web").error_count == 1
    assert registry.snapshot_by_source("mobile").total == 0


def test_采样开关在来源维度下的口径(registry):
    registry.record("a", "t", 10, 500, error="E", source="web", sampled=False)
    registry.record("b", "t", 20, 200, source="web", sampled=True)

    web = registry.snapshot_by_source("web")
    assert web.total == 2        # 未采样仍占 total
    assert web.sampled == 1      # 但不进统计口径
    assert web.error_count == 0
    assert web.by_status == {200: 1}
    # 全局口径同样对得上
    assert registry.snapshot().total == 2
    assert registry.snapshot().sampled == 1


def test_来源超限归并到保留桶且可观察():
    registry = MetricsRegistry(max_sources=2)
    registry.record("a", "t", 10, 200, source="web")
    registry.record("b", "t", 10, 500, error="E", source="app")
    # 第三个来源超限 -> 归并，数据不丢弃
    registry.record("c", "t", 10, 500, error="E", source="evil-1")
    registry.record("d", "t", 10, 200, source="evil-2")
    registry.record("e", "t", 10, 200, source="evil-1")  # 同一名再次归并

    stats = registry.source_stats()
    assert stats.max_sources == 2
    assert stats.overflowed is True
    assert stats.overflow_records == 3        # 3 条记录被归并
    assert stats.overflow_sources_seen == 2   # 涉及 2 个不同来源名
    assert stats.overflow_source == OVERFLOW_SOURCE
    assert OVERFLOW_SOURCE in stats.sources

    # 归并桶的数据完整可查，全局口径不失真
    overflow = registry.snapshot_by_source(OVERFLOW_SOURCE)
    assert overflow.total == 3 and overflow.error_count == 1
    per_source = registry.snapshot_all_sources()
    assert sum(s.total for s in per_source.values()) == registry.snapshot().total == 5
    assert sum(s.error_count for s in per_source.values()) == 2


def test_超限后既有来源与已统计数字不受影响():
    registry = MetricsRegistry(max_sources=1)
    registry.record("a", "t", 10, 500, error="E", source="web")
    before = registry.snapshot_by_source("web")

    # 大量乱造来源打不垮既有统计
    for i in range(500):
        registry.record(f"flood-{i}", "t", 1, 200, source=f"evil-{i}")

    after = registry.snapshot_by_source("web")
    assert after == before  # 已统计好的数字不被改坏
    stats = registry.source_stats()
    assert stats.overflow_records == 500
    assert stats.overflow_sources_seen == 500
    # 内存有界：生效来源数不超过 max_sources + 1 个保留桶
    assert stats.source_count <= stats.max_sources + 1
    assert registry.snapshot().total == 501


def test_max_sources非法值快速失败():
    with pytest.raises(ValueError):
        MetricsRegistry(max_sources=0)


def test_reset清空来源与溢出口径():
    registry = MetricsRegistry(max_sources=1)
    registry.record("a", "t", 10, 200, source="web")
    registry.record("b", "t", 10, 200, source="evil")
    assert registry.source_stats().overflowed is True

    registry.reset()
    stats = registry.source_stats()
    assert stats.source_count == 0 and stats.overflowed is False
    assert stats.overflow_records == 0 and registry.snapshot().total == 0
