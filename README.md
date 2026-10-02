# FastAPI 可观测性与告警

一个本地可跑的 FastAPI 服务骨架，内置请求级指标、链路标识、时间窗口告警与
规则热更新能力；在并发/重复上报下保证指标与告警结论不重复、不丢失、不互相污染。

## 快速开始

```bash
uv sync --group dev
uv run uvicorn app.main:app --reload
```

## 测试

```bash
uv run pytest -q
```

测试日志按业务域分组输出（业务名、每条用例结果、各业务小计与合计），
覆盖：并发上报幂等、采样口径、告警去重/升级/收敛、规则热更新与降级、
HTTP 端到端一致性。

## 文档

- 指标口径、告警规则配置字段、接口清单与验证方法：
  [docs/observability.md](docs/observability.md)
- 规则示例：[config/rules.example.json](config/rules.example.json)
