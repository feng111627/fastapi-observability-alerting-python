"""共享 fixture 与业务日志插件加载。"""

import pytest

# tests/ 目录会被 pytest 加入 sys.path（conftest.py 所在目录），
# 业务分组日志以独立插件形式实现，便于单独维护。
pytest_plugins = ("bizlog",)


@pytest.fixture
def registry():
    from app.metrics import MetricsRegistry

    return MetricsRegistry()


@pytest.fixture
def engine():
    from app.alerts import AlertEngine

    return AlertEngine()


@pytest.fixture
def rule_store():
    from app.rules import RuleStore

    return RuleStore()
