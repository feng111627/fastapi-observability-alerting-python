"""业务域：请求级指标与链路标识（去重、采样、并发一致性）。"""
from __future__ import annotations

import concurrent.futures

from app.models import RequestSample
from app.service import service_singleton


class TestRequestIdDedup:
    """业务：同一 request_id 重复上报只计一次。"""

    def test_duplicate_request_id_counted_once(self):
        ok1, _ = service_singleton.report(RequestSample("dup-1", 1.0, 10, 200))
        ok2, _ = service_singleton.report(RequestSample("dup-1", 2.0, 99, 500, "x"))
        assert ok1 is True and ok2 is False
        snap = service_singleton.snapshot()
        assert snap.total == 1
        assert snap.error_count == 0  # 第二次是重复上报，不改变错误口径

    def test_duplicate_does_not_drift_duration(self):
        service_singleton.report(RequestSample("dup-2", 1.0, 100, 200))
        for _ in range(20):
            service_singleton.report(RequestSample("dup-2", 1.0, 999, 500))
        snap = service_singleton.snapshot()
        assert snap.total_duration_ms == 100.0

    def test_distinct_ids_all_counted(self):
        for i in range(5):
            ok, _ = service_singleton.report(RequestSample(f"id-{i}", float(i), 1, 200))
            assert ok
        assert service_singleton.snapshot().total == 5


class TestSamplingSwitch:
    """业务：采样开关切换不回溯、不漂移。"""

    def test_samples_during_off_not_counted(self):
        service_singleton.report(RequestSample("s-1", 1.0, 10, 200))
        service_singleton.sampling_off()
        ok, _ = service_singleton.report(RequestSample("s-2", 1.0, 10, 500))
        assert ok is False
        service_singleton.sampling_on()
        ok, _ = service_singleton.report(RequestSample("s-3", 1.0, 10, 500))
        assert ok is True
        snap = service_singleton.snapshot()
        assert snap.total == 2 and snap.error_count == 1

    def test_known_id_seen_during_off_still_dedup_after_on(self):
        service_singleton.sampling_off()
        # 关闭期间上报不应写入 seen，否则会污染开启后的计数
        service_singleton.report(RequestSample("s-x", 1.0, 10, 200))
        service_singleton.sampling_on()
        ok, _ = service_singleton.report(RequestSample("s-x", 1.0, 10, 200))
        assert ok is True


class TestConcurrentReport:
    """业务：并发上报，计数与去重结论必须对得上。"""

    def test_concurrent_unique_ids_no_loss(self):
        n = 200

        def task(i: int):
            return service_singleton.report(
                RequestSample(f"c-{i}", float(i), 5, 200 if i % 3 else 500,
                              "err" if i % 3 == 0 else None)
            )[0]

        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
            results = list(pool.map(task, range(n)))
        snap = service_singleton.snapshot()
        assert sum(results) == n
        assert snap.total == n
        assert snap.error_count == len([i for i in range(n) if i % 3 == 0])
        assert snap.total_duration_ms == 5 * n

    def test_concurrent_same_id_exactly_one_winner(self):
        n = 64

        def task(_: int) -> bool:
            return service_singleton.report(RequestSample("same", 1.0, 7, 200))[0]

        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as pool:
            results = list(pool.map(task, range(n)))
        assert sum(results) == 1
        assert service_singleton.snapshot().total == 1
