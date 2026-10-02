"""业务域：告警规则热更新、部分降级与失败原因可区分。"""
from __future__ import annotations

import concurrent.futures

from app.models import AlertRule
from app.rules import (
    R_METRIC_UNKNOWN, R_NAME_MISSING, R_NOT_OBJECT, R_OPERATOR_UNSUPPORTED,
    R_THRESHOLD_INVALID, R_WINDOW_INVALID, R_DUPLICATE_NAME,
    RuleStore, validate_rule, RuleValidationError,
)
from app.service import service_singleton


class TestValidationReasons:
    """业务：坏规则给出可区分的失败原因。"""

    def test_each_failure_has_distinct_reason(self):
        cases = [
            ({"metric": "x"}, R_NAME_MISSING),
            ({"name": "r", "metric": "nope", "operator": ">", "threshold": 1},
             R_METRIC_UNKNOWN),
            ({"name": "r", "metric": "error_rate", "operator": "==", "threshold": 1},
             R_OPERATOR_UNSUPPORTED),
            ({"name": "r", "metric": "error_rate", "operator": ">", "threshold": "x"},
             R_THRESHOLD_INVALID),
            ({"name": "r", "metric": "error_rate", "operator": ">", "threshold": 1,
              "window_s": -1}, R_WINDOW_INVALID),
        ]
        for payload, reason in cases:
            try:
                validate_rule(payload)
                raise AssertionError(f"should reject {payload}")
            except RuleValidationError as exc:
                assert exc.reason == reason, (payload, exc.reason)

    def test_non_dict_payload(self):
        try:
            validate_rule("junk")
        except RuleValidationError as exc:
            assert exc.reason == R_NOT_OBJECT


class TestPartialDegradation:
    """业务：单条坏规则不影响其它规则生效。"""

    def test_bad_rule_isolated(self):
        store = RuleStore()
        result = store.load([
            {"name": "good", "metric": "latency_ms", "operator": ">", "threshold": 100},
            {"name": "bad", "metric": "unknown", "operator": ">", "threshold": 1},
        ])
        assert [r.name for r in result.loaded] == ["good"]
        assert result.rejected == {"bad": R_METRIC_UNKNOWN}
        assert store.version == 1
        assert store.last_error == "partial_rejected"

    def test_duplicate_name_rejected(self):
        store = RuleStore()
        result = store.load([
            {"name": "dup", "metric": "latency_ms", "operator": ">", "threshold": 1},
            {"name": "dup", "metric": "error_rate", "operator": ">", "threshold": 0.1},
        ])
        assert [r.name for r in result.loaded] == ["dup"]
        assert result.rejected == {"dup": R_DUPLICATE_NAME}


class TestKeepLastGoodVersion:
    """业务：整批加载失败时保留上一版可用规则。"""

    def test_all_bad_payload_keeps_previous(self):
        store = RuleStore()
        store.load([{"name": "v1", "metric": "latency_ms", "operator": ">",
                     "threshold": 10}])
        result = store.load([{"name": "broken", "metric": "??"}])
        assert result.loaded == []
        assert store.version == 1
        assert [r.name for r in store.current()] == ["v1"]
        assert store.last_error == "no_valid_rules"

    def test_non_list_payload_keeps_previous(self):
        store = RuleStore()
        store.load([{"name": "v1", "metric": "latency_ms", "operator": ">",
                     "threshold": 10}])
        store.load({"not": "a list"})
        assert store.version == 1 and len(store.current()) == 1


class TestHotReloadConcurrency:
    """业务：并发上报与热更新同时进行时，上报始终命中某一完整规则版本。"""

    def test_reload_during_concurrent_reports(self):
        from app.models import RequestSample

        svc = service_singleton

        def reload_job(version: int) -> int:
            payload = [{
                "name": f"slow_v{version}", "metric": "latency_ms",
                "operator": ">", "threshold": 100, "window_s": 600,
            }]
            return svc.reload_rules(payload).version

        def report_job(i: int):
            ok, _ = svc.report(
                RequestSample(f"hot-{i}", float(i), 200, 200))
            return ok

        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
            reloads = [pool.submit(reload_job, v) for v in range(1, 11)]
            reports = [pool.submit(report_job, i) for i in range(100)]
            versions = [f.result() for f in reloads]
            reported = [f.result() for f in reports]
        # 每次有效加载都严格 +1（完成顺序可以乱序）
        assert sorted(versions) == list(range(2, 12))
        assert all(reported)
        snap = svc.snapshot()
        assert snap.total == 100  # 热更新期间没有样本丢失
        # 任何时刻只应有一个规则名生效，告警按该版本指纹归并
        rules = svc.rules.current()
        assert len(rules) == 1 and rules[0].name.startswith("slow_v")
