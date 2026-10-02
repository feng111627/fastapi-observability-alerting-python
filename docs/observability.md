# 可观测与告警能力说明

## 1. 总体结构

```
app/
  models.py    # 领域模型：RequestSample / AlertRule / Alert / 状态枚举
  tracing.py   # X-Request-ID 链路标识中间件
  metrics.py   # 并发安全的去重指标注册表 + 采样开关
  alerts.py    # 滑动窗口 + 告警状态机（触发/去重/升级/收敛）
  rules.py     # 规则校验、部分降级、热更新版本留痕
  service.py   # 门面：一次上报串行完成「指标登记 + 告警评估」
  main.py      # HTTP 接口 + 后台 1s 定时收敛任务
```

并发一致性的关键保证：

- 指标登记、告警评估、规则热更新各自持锁；`ObservabilityService.report`
  用一把服务级锁把「去重登记 + 告警评估」串行化，因此并发上报时指标计数与
  告警结论永远基于同一个决策：**不计数的样本绝不进入告警评估**。
- 重复 `request_id` 只有一个线程获胜，其余幂等丢弃。
- 规则热更新是「先校验整批、再原子切换」，上报过程中永远只会命中某一个
  完整的规则版本，不会看到半成品规则集。

## 2. 指标口径

| 指标 | 口径 |
| --- | --- |
| `total` | 已计数的**去重请求数**（按 `request_id` 唯一） |
| `error_count` | 已计数请求中 `status_code >= 400` 或 `error` 非空的数量 |
| `avg_duration_ms` | `sum(duration_ms) / total`，仅按已计数样本 |
| `sampling` | 当前采样开关；关闭期间的新样本不计入任何指标 |
| `last_request_id` | 最近一次被计数的请求标识 |

规则：

1. **链路标识**：请求带 `X-Request-ID` 则沿用（去除首尾空白），否则生成
   32 位 UUID；响应头原样回写，便于端到端关联。
2. **幂等**：同一 `request_id` 重复上报只计第一次，后续返回 `counted=false`，
   不改变任何指标（耗时、错误数都不漂移）。
3. **采样开关**：关闭期间的样本直接丢弃且**不写入去重集合**（关闭期间看到的
   ID 在重新开启后仍可正常计数）；切换开关不回溯历史样本。

接口：`GET /api/metrics`、`POST /api/report`。

## 3. 告警规则配置

### 3.1 规则字段

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `name` | 非空字符串 | 规则名，同时作为告警指纹（同名规则即同一异常） |
| `metric` | 枚举 | `error_rate` / `latency_ms` / `error_count` |
| `operator` | 枚举 | `>` `>=` `<` `<=` |
| `threshold` | 数值 | 阈值 |
| `window_s` | 正数 | 滑动统计窗口（秒） |
| `for_s` | >=0 | 持续越界多久后由 PENDING 升级为 FIRING；0 表示立即 FIRING |
| `cooldown_s` | >=0 | 合并去重/静默 reopen 窗口；也是 FIRING 后的升级周期 |
| `resolve_s` | 正数 | FIRING 后多久未再越界即判定收敛 |

指标在窗口内的取值：

- `error_rate`：窗口内错误请求占比（0~1）。
- `latency_ms`：窗口内**最大**单次耗时（ms）。
- `error_count`：窗口内错误请求数。

### 3.2 告警生命周期

```
首次越界 ──► PENDING ──持续 for_s──► FIRING(level=1)
              │                      │ 持续越界，每过一个升级周期
              │恢复（窗口内不再越界）  │ level+1（上限 5），不产生新告警
              ▼                      ▼
          静默清除              resolve_s 未再越界 ──► RESOLVED
                                     │
                          cooldown_s 内再次越界：静默 reopen（不重复发告警）
                          超过 cooldown_s：作为一条新告警重新 FIRE
```

- **合并去重**：告警指纹 = 规则名；同一异常在窗口/冷却期内始终只有一条
  告警，重复越界只刷新 `last_seen`/`count`。
- **升级**：持续触发只在原告警上累加 `level`，响应里通过 `upgraded` 区分。
- **收敛**：异常结束后明确转为 `resolved` 并带 `resolved_at`；应用内置
  后台任务每 1s 调用一次评估，无需人工触发。

### 3.3 热更新与降级

`POST /api/rules/reload`，请求体为规则数组：

- **至少一条有效**：原子切换到新版本（`version+1`），坏规则出现在响应
  `rejected` 中（键为规则名，值为可区分的原因码），HTTP 200，
  `last_error=partial_rejected`。坏规则不影响好规则生效。
- **整批无效**：HTTP 422，`last_error=no_valid_rules`，**上一版规则继续
  生效**，`version` 不变。

原因码（可用于告警/监控分类）：

`not_an_object`、`name_missing`、`name_invalid`、`metric_unknown`、
`operator_unsupported`、`threshold_invalid`、`window_invalid`、
`for_invalid`、`cooldown_invalid`、`resolve_invalid`、`duplicate_name`。

默认内置三条规则（见 `app/service.py:DEFAULT_RULES`）：
错误率 >50%（持续 10s 升级）、单次耗时 >1000ms 立即触发、60s 内错误数 ≥5。

## 4. 接口一览

| 方法/路径 | 作用 |
| --- | --- |
| `POST /api/report` | 上报一条请求指标（body 见下） |
| `GET /api/metrics` | 查询去重后的指标口径 |
| `GET /api/alerts?state=pending/firing/resolved` | 查询告警 |
| `POST /api/rules/reload` | 热更新规则（支持部分降级） |
| `GET /api/rules` | 当前规则版本与内容 |
| `GET /api/demo?fail=0&delay_ms=0` | 自带埋点的演示业务 |

`/api/report` body：

```json
{"request_id": "可选，缺省取 X-Request-ID",
 "duration_ms": 12.3, "status_code": 200, "error": null,
 "method": "GET", "path": "/api/demo"}
```

## 5. 本地运行与验证

```bash
uv sync --group dev          # 安装依赖
uv run python main.py        # 启动服务 http://127.0.0.1:8000 （文档 /docs）
uv run pytest                # 全量单测，-v 日志按业务域/用例分组展示
```

手工验证（并发去重 + 热更新降级）：

```bash
# 同一标识连发两次：只有第一次 counted=true
curl -s -X POST localhost:8000/api/report -H 'Content-Type: application/json' \
  -d '{"request_id":"t1","duration_ms":2000}'
curl -s -X POST localhost:8000/api/report -H 'Content-Type: application/json' \
  -d '{"request_id":"t1","duration_ms":9999}'
curl -s localhost:8000/api/metrics

# 热更新：一条好规则 + 一条坏规则
curl -s -X POST localhost:8000/api/rules/reload -H 'Content-Type: application/json' \
  -d '[{"name":"ok","metric":"latency_ms","operator":">","threshold":50},
       {"name":"bad","metric":"ghost"}]'

# 整批无效 -> 422，旧版规则保留
curl -s -i -X POST localhost:8000/api/rules/reload -H 'Content-Type: application/json' \
  -d '[{"name":"x","metric":"nope"}]'
```
