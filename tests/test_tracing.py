"""业务域：链路标识中间件。"""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.requests import Request

from app.tracing import REQUEST_ID_HEADER, TracingMiddleware, get_request_id, new_request_id


def _build_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(TracingMiddleware)

    @app.get("/whoami")
    def whoami(request: Request):
        return {"rid": get_request_id(request)}

    return app


class TestTracing:
    def test_client_request_id_is_honored(self):
        client = TestClient(_build_app())
        r = client.get("/whoami", headers={REQUEST_ID_HEADER: "trace-abc"})
        assert r.json()["rid"] == "trace-abc"
        assert r.headers[REQUEST_ID_HEADER.lower()] == "trace-abc"

    def test_missing_request_id_is_generated_and_echoed(self):
        client = TestClient(_build_app())
        r1 = client.get("/whoami")
        r2 = client.get("/whoami")
        assert r1.json()["rid"] and r1.json()["rid"] != r2.json()["rid"]

    def test_blank_header_treated_as_missing(self):
        client = TestClient(_build_app())
        r = client.get("/whoami", headers={REQUEST_ID_HEADER: "   "})
        assert len(r.json()["rid"]) == len(new_request_id())
