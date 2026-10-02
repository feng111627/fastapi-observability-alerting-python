"""pytest 插件：按业务域分组打印用例与执行情况。

约定：每个测试模块的模块 docstring 以 ``业务: xxx`` 声明所属业务，例如::

    \"\"\"业务: 请求级指标与链路标识。\"\"\"

运行结束后输出：
- 本次测试覆盖了哪些业务；
- 每个业务下每条用例的名称与 PASS/FAIL/SKIP 结果；
- 每个业务及全部用例的合计统计。
"""

import re
from collections import OrderedDict

_DEFAULT_BUSINESS = "未分类"
_BIZ_RE = re.compile(r"业务[:：]\s*(.+)")

# nodeid -> {"business", "name", "outcome"}（单进程一次运行内有效）
_CASES: "OrderedDict[str, dict]" = OrderedDict()


def _business_of(module) -> str:
    doc = (module.__doc__ or "").strip()
    match = _BIZ_RE.search(doc.splitlines()[0] if doc else "")
    return match.group(1).strip().rstrip("。.") if match else _DEFAULT_BUSINESS


def pytest_configure(config):
    _CASES.clear()


def pytest_collection_modifyitems(config, items):
    for item in items:
        module = getattr(item, "module", None)
        business = _business_of(module) if module is not None else _DEFAULT_BUSINESS
        _CASES[item.nodeid] = {
            "business": business,
            "name": item.name,
            "file": item.location[0],
            "outcome": None,
        }


def pytest_runtest_logreport(report):
    case = _CASES.get(report.nodeid)
    if case is None:
        return

    if report.when == "setup" and report.outcome == "failed":
        case["outcome"] = "error"
    elif report.when == "call":
        if report.outcome == "passed":
            case["outcome"] = "passed"
        elif report.outcome == "failed":
            case["outcome"] = "failed"
        elif report.outcome == "skipped":
            case["outcome"] = "skipped"
    elif report.when == "teardown" and report.outcome == "failed" and case["outcome"] is None:
        case["outcome"] = "error"


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    grouped: OrderedDict[str, list] = OrderedDict()
    for case in _CASES.values():
        grouped.setdefault(case["business"], []).append(case)

    writer = terminalreporter
    writer.write_sep("=", "业务覆盖与用例结果")

    totals = {"passed": 0, "failed": 0, "skipped": 0, "error": 0}
    for business, biz_cases in grouped.items():
        writer.write_line(f"业务: {business}")
        for case in biz_cases:
            outcome = case["outcome"] or "error"
            mark = {
                "passed": "PASS",
                "failed": "FAIL",
                "skipped": "SKIP",
                "error": "ERROR",
            }.get(outcome, "???")
            totals[outcome] = totals.get(outcome, 0) + 1
            writer.write_line(f"  [{mark}] {case['name']}")
        passed = sum(1 for c in biz_cases if c["outcome"] == "passed")
        failed = sum(1 for c in biz_cases if c["outcome"] == "failed")
        skipped = sum(1 for c in biz_cases if c["outcome"] == "skipped")
        errors = sum(1 for c in biz_cases if c["outcome"] in (None, "error"))
        writer.write_line(
            f"  小计: {len(biz_cases)} 用例 -> {passed} 通过 / {failed} 失败 / {skipped} 跳过 / {errors} 错误"
        )
        writer.write_line("-" * 60)

    writer.write_line(
        f"合计: {len(_CASES)} 用例, 覆盖业务 {len(grouped)} 个 -> "
        f"{totals['passed']} 通过 / {totals['failed']} 失败 / "
        f"{totals['skipped']} 跳过 / {totals['error']} 错误"
    )
