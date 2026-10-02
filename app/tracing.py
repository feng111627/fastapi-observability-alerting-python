"""请求链路标识中间件。

- 优先使用客户端传入的 X-Request-ID，缺失则生成 UUID；
- 同一请求在响应头与 request.state 中都能拿到同一标识；
- 不校验客户端格式，但去除首尾空白，空串视为缺失。
"""
from __future__ import annotations

import uuid

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

REQUEST_ID_HEADER = "X-Request-ID"


def new_request_id() -> str:
    return uuid.uuid4().hex


class TracingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        rid = request.headers.get(REQUEST_ID_HEADER, "").strip() or new_request_id()
        request.state.request_id = rid
        response = await call_next(request)
        response.headers[REQUEST_ID_HEADER] = rid
        return response


def get_request_id(request: Request) -> str:
    """从 request.state 取链路标识（中间件保证存在）。"""
    return getattr(request.state, "request_id", None) or new_request_id()
