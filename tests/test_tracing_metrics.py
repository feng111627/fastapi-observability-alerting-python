"""业务: 请求级指标与链路标识。

覆盖：唯一标识、上下文传递、幂等去重、采样口径、
同一标识重复上报/采样切换不重复计数、并发上报一致性。
"""

import threading

import pytest

from app import metrics as metrics_mod
from app import tracing


def test_request_id_全局唯一_无碰撞():
    ids = {tracing.new_request_id() for _ in range(50_000)}
    assert len(ids) == 50_000


def test_上下文绑定与恢复互不串扰():
    barrier = threading.Barrier(4)
    seen: list[tuple[str, str]] = []
    seen_lock = threading.Lock()

    def worker(i: int):
        rid, tid = f"r{i}", f"t{i}"
        tokens = tracing.set_context(rid, tid)
        barrier.wait()  # 让四个线程的上下文同时存在
        with seen_lock:
            seen.append((tracing.current_request_id(), tracing.current_trace_id()))
        tracing.reset_context(tokens)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(seen) == [(f"r{i}", f"t{i}") for i in range(4)]
    assert tracing.current_request_id() is None


def test_同一request_id重复上报只计一次且先到为准(registry):
    assert registry.record("r1", "t1", 100.0, 200, sampled=True) is True
    # 后续上报即便数值/采样开关不同也被忽略，统计不漂移
    assert registry.record("r1", "t1", 999.0, 500, error="boom", sampled=False) is False
    rec = registry.get("r1")
    assert rec.duration_ms == 100.0 and rec.status_code == 200 and rec.sampled is True

    snap = registry.snapshot()
    assert snap.total == 1 and snap.sampled == 1


def test_采样关闭计入总量但不污染统计口径(registry):
    registry.record("a", "t", 100.0, 200, sampled=True)
    registry.record("b", "t", 200.0, 500, error="E", sampled=False)
    registry.record("c", "t", 300.0, 200, sampled=False)

    snap = registry.snapshot()
    assert snap.total == 3          # 总量含未采样
    assert snap.sampled == 1        # 统计口径只含采样
    assert snap.error_count == 0
    assert snap.sum_duration_ms == 100.0
    assert snap.avg_duration_ms == 100.0
    assert snap.error_rate == 0.0


def test_错误口径_显式错误或5xx都计错误(registry):
    registry.record("a", "t", 10, 500, sampled=True)
    registry.record("b", "t", 10, 200, error="Boom", sampled=True)
    registry.record("c", "t", 10, 404, sampled=True)  # 4xx 不算错误
    snap = registry.snapshot()
    assert snap.error_count == 2 and pytest.approx(snap.error_rate) == 2 / 3


def test_并发上报同一id只有一条生效(registry):
    n = 32
    accepted = []
    barrier = threading.Barrier(n)

    def worker():
        barrier.wait()
        accepted.append(
            registry.record("same-id", "t", 10.0, 200, sampled=True)
        )

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sum(accepted) == 1
    snap = registry.snapshot()
    assert snap.total == 1 and snap.sampled == 1


def test_并发上报不同id不丢失且聚合一致(registry):
    n_threads, per = 16, 200
    barrier = threading.Barrier(n_threads)

    def worker(tid: int):
        barrier.wait()
        for i in range(per):
            rid = f"t{tid}-{i}"
            registry.record(rid, "trace", float(i), 500 if i % 2 else 200,
                            error="E" if i % 2 else None, sampled=True)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    snap = registry.snapshot()
    expected = n_threads * per
    assert snap.total == expected and snap.sampled == expected
    assert snap.error_count == expected // 2
    assert snap.by_status == {200: expected // 2, 500: expected // 2}
    assert sum(snap.by_status.values()) == expected


def test_并发下采样开关切换不引起指标漂移(registry, monkeypatch):
    # 同一批 id 由不同线程以相反的 sampled 值抢报，
    # 结果必须满足：total=去重id数，sampled<=total，且每个id只有一个终态。
    ids = [f"id-{i}" for i in range(500)]

    def worker(sampled: bool):
        for rid in ids:
            registry.record(rid, "trace", 5.0, 200, sampled=sampled)

    t1 = threading.Thread(target=worker, args=(True,))
    t2 = threading.Thread(target=worker, args=(False,))
    t1.start(); t2.start(); t1.join(); t2.join()

    snap = registry.snapshot()
    assert snap.total == len(ids)
    assert snap.sampled <= snap.total
    # 每条记录的终态唯一：snapshot 的统计与逐条 records 完全一致
    records = registry.records()
    assert len(records) == len(ids)
    assert sum(1 for r in records if r.sampled) == snap.sampled
    assert sum(r.duration_ms for r in records if r.sampled) == snap.sum_duration_ms


def test_record非法参数快速失败(registry):
    with pytest.raises(ValueError):
        registry.record("", "t", 1.0, 200)
    with pytest.raises(ValueError):
        registry.record("x", "t", -1.0, 200)
