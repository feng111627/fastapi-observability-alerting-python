"""测试夹具：每个用例后重置单例服务，避免用例间互相污染。"""
from __future__ import annotations

import pytest

from app import main as main_module
from app.service import ObservabilityService, service_singleton


@pytest.fixture(autouse=True)
def _reset_service():
    service_singleton.reset()
    service_singleton.reload_rules(main_module_default_rules())
    yield
    service_singleton.reset()


def main_module_default_rules():
    from app.service import DEFAULT_RULES
    return DEFAULT_RULES


@pytest.fixture
def fresh_service() -> ObservabilityService:
    svc = ObservabilityService()
    return svc


@pytest.fixture
def client():
    from fastapi.testclient import TestClient
    with TestClient(main_module.app) as c:
        yield c
