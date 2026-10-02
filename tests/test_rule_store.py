"""业务: 告警规则热更新与降级。

覆盖：逐条校验隔离、坏规则原因可区分、整批失败保留上一版、
热更新原子生效、重名拒绝、空批降级、并发热更新一致性。
"""

import threading

import pytest

from app.rules import RuleStore, RuleValidationError, validate_rule


def valid_rule(name: str = "a", **over) -> dict:
    base = {
        "name": name,
        "metric": "error_rate",
        "threshold": 0.5,
        "window_seconds": 60.0,
        "severity": "warning",
        "upgrade_after": 0,
    }
    base.update(over)
    return base


def test_合法规则通过校验():
    rule = validate_rule(valid_rule())
    assert rule.name == "a" and rule.threshold == 0.5


@pytest.mark.parametrize(
    "raw, fragment",
    [
        (valid_rule(metric="unknown"), "metric"),
        (valid_rule(threshold="high"), "threshold"),
        (valid_rule(threshold=2.0), "[0, 1]"),       # error_rate 越界
        (valid_rule(window_seconds=0), "window_seconds"),
        (valid_rule(window_seconds=-1), "window_seconds"),
        (valid_rule(severity="fatal"), "severity"),
        (valid_rule(upgrade_after=-1), "upgrade_after"),
        (valid_rule(upgrade_after=1.5), "upgrade_after"),
        ({"metric": "error_rate", "threshold": 0.5, "window_seconds": 1}, "name"),
        ("not-a-dict", "必须是对象"),
    ],
)
def test_坏规则原因可区分(raw, fragment):
    with pytest.raises(RuleValidationError) as exc:
        validate_rule(raw)
    assert fragment in str(exc.value)


def test_加载混合批次_好规则生效坏规则被隔离(rule_store):
    report = rule_store.load(
        [
            valid_rule("good1"),
            valid_rule("good2", metric="latency_p95", threshold=500.0),
            valid_rule("bad1", metric="nope"),
            {"name": "bad2", "metric": "error_rate"},  # 缺字段
        ]
    )
    assert report.ok is True
    assert report.version == 1
    assert report.loaded == 2 and report.rejected == 2
    names = [r.name for r in rule_store.current().rules]
    assert names == ["good1", "good2"]
    # 每条坏规则的失败原因都可区分
    assert any("bad1" in e for e in report.errors)
    assert any("bad2" in e for e in report.errors)
    assert report.errors[0] != report.errors[1]


def test_整批非法时保留上一版规则且版本不推进(rule_store):
    report1 = rule_store.load([valid_rule("v1-rule")])
    assert report1.version == 1

    report2 = rule_store.load([valid_rule("x", metric="bad")])
    assert report2.ok is False
    assert report2.version == 1, "降级时仍返回上一版版本号"
    assert report2.loaded == 0 and report2.rejected == 1
    current = rule_store.current()
    assert current.version == 1
    assert [r.name for r in current.rules] == ["v1-rule"]


def test_空批次降级保留上一版(rule_store):
    rule_store.load([valid_rule("keep")])
    report = rule_store.load([])
    assert report.ok is False and report.version == 1
    assert [r.name for r in rule_store.current().rules] == ["keep"]
    assert "为空" in report.errors[0]


def test_热更新成功后原子切换为新版(rule_store):
    rule_store.load([valid_rule("old")])
    report = rule_store.load([valid_rule("new1"), valid_rule("new2")])
    assert report.ok and report.version == 2
    current = rule_store.current()
    assert current.version == 2 and len(current.rules) == 2
    assert {r.name for r in current.rules} == {"new1", "new2"}


def test_同批重名只保留一条并给出原因(rule_store):
    report = rule_store.load([valid_rule("dup"), valid_rule("dup", threshold=0.9)])
    assert report.ok and report.loaded == 1 and report.rejected == 1
    assert any("重名" in e for e in report.errors)
    assert [r.name for r in rule_store.current().rules] == ["dup"]


def test_部分合法部分非法仍推进版本但仅含合法规则(rule_store):
    report = rule_store.load([valid_rule("a"), valid_rule("b", metric="bad")])
    assert report.ok and report.version == 1 and report.loaded == 1
    assert [r.name for r in rule_store.current().rules] == ["a"]


def test_规则不可变():
    rule = validate_rule(valid_rule())
    with pytest.raises(Exception):
        rule.threshold = 1.0  # type: ignore[misc]


def test_并发热更新版本号严格递增且无撕裂快照(rule_store):
    versions: list[int] = []
    lock = threading.Lock()

    def loader(v: int):
        report = rule_store.load(
            [valid_rule(f"r{v}-{i}", threshold=0.01 * (i + 1)) for i in range(3)]
        )
        with lock:
            versions.append(report.version)
        # 任何时候读到的快照必须完整：3 条规则且名称同批
        snap = rule_store.current()
        if snap.version > 0:
            assert len(snap.rules) == 3

    threads = [threading.Thread(target=loader, args=(v,)) for v in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(versions) == list(range(1, 21)), versions
    assert rule_store.current().version == 20
