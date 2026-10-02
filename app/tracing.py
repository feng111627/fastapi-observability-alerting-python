"""请求链路标识。

每次请求分配唯一 request_id，并在上下文变量中传递，
同时生成 trace_id 用于跨组件关联。
"""

import uuid
from contextvars import ContextVar, Token

_request_id: ContextVar[str | None] = ContextVar("request_id", default=None)
_trace_id: ContextVar[str | None] = ContextVar("trace_id", default=None)


def new_request_id() -> str:
    """生成一个新的全局唯一请求标识。"""
    # uuid4 在 CPython 下由 os.urandom 兜底，多线程/多协程并发也不会撞号。
    return uuid.uuid4().hex


def new_trace_id() -> str:
    """生成一个新的链路标识。"""
    return uuid.uuid4().hex


def set_context(request_id: str, trace_id: str) -> tuple[Token[str | None], Token[str | None]]:
    """绑定当前执行上下文的请求/链路标识，返回 token 对用于恢复。"""
    t1 = _request_id.set(request_id)
    t2 = _trace_id.set(trace_id)
    return t1, t2


def reset_context(tokens: tuple[Token[str | None], Token[str | None]]) -> None:
    """恢复上下文到绑定之前。"""
    t1, t2 = tokens
    _request_id.reset(t1)
    _trace_id.reset(t2)


def current_request_id() -> str | None:
    return _request_id.get()


def current_trace_id() -> str | None:
    return _trace_id.get()
