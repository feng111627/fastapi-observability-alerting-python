# fastapi-observability-alerting

本地可跑的 FastAPI 可观测与告警服务：请求级指标与链路标识、基于时间窗口的
告警判定（去重 / 升级 / 收敛）、告警规则热更新与部分降级。

## 快速开始

```bash
uv sync --group dev
uv run python main.py            # http://127.0.0.1:8000/docs
uv run pytest                    # 34 个用例，按业务域分组
```

## 能力一览

- **请求级指标与链路标识**：每个请求由 `X-Request-ID` 唯一标识（缺省自动
  生成），关联耗时、状态码、错误信息；同 ID 重复上报幂等、采样开关切换
  不回溯，指标不重复计数、不漂移。
- **窗口告警与合并去重**：滑动窗口判定错误率 / 耗时 / 错误数；状态机
  PENDING→FIRING→RESOLVED，同异常窗口内一条告警、持续触发逐级升级、
  异常结束明确收敛；并发上报不重复、不漏报。
- **规则热更新与降级**：坏规则逐条拒绝并返回可区分原因码，不影响好规则；
  整批无效时保留上一版可用规则（HTTP 422）。

详见 [docs/observability.md](docs/observability.md)，
测试覆盖见 [tests/README.md](tests/README.md)。
