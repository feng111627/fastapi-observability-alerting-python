# 可观测性与告警说明

本服务在 FastAPI 上内置了一套本地可跑的请求级指标、链路标识、时间窗口告警
与规则热更新能力。设计目标是：**并发上报或多次上报同时发生时，指标统计与
告警结论始终对得上——不重复、不丢失、不互相污染。**

## 1. 模块结构

| 模块 | 职责 |
| --- | --- |
| `app/tracing.py` | `request_id` / `trace_id` 生成、基于 `contextvars` 的上下文绑定与恢复 |
| `app/metrics.py` | 请求级指标注册表：按 `request_id` 幂等去重、采样口径、一致快照 |
| `app/rules.py` | 告警规则校验、逐条隔离、不可变快照 + 原子热更新、整批失败降级 |
| `app/alerts.py` | 时间窗口告警引擎：合并去重、持续升级、到期收敛 |
| `app/evaluation.py` | 窗口指标聚合（错误率/错误数/平均/P95）与逐规则评估编排 |
| `app/main.py` | HTTP 中间件与接口装配、后台定时收敛循环 |

## 2. 链路标识

- 每个 HTTP 请求经过中间件时都会获得两个标识：
  - `X-Request-Id`：单次请求唯一标识（`uuid4`，`os.urandom` 兜底，并发无碰撞），
    客户端也可以通过请求头透传，服务端原样使用；
  - `X-Trace-Id`：链路标识，语义相同，用于跨组件关联。
- 两个标识通过 `contextvars` 传递，线程/协程之间互不串扰；
  请求结束时通过 token 恢复上下文。
- 响应头会回传 `X-Request-Id` / `X-Trace-Id`，便于排障关联日志、指标与告警。

## 3. 指标口径（重点）

指标按 `request_id` **幂等**记录：同一 `request_id` 的重复上报直接忽略
（first-write-wins），因此重复上报、采样开关切换都不会导致重复计数或指标漂移。

| 指标 | 口径 |
| --- | --- |
| `total` | 去重后的请求总数，**包含未采样记录**（用于核对“到底来了多少请求”） |
| `sampled` | 其中 `sampled=true`、进入统计口径的请求数 |
| `error_count` | 采样记录中，满足“显式带 `error` 字段”**或**“状态码 ≥ 500”的数量。4xx 不计错误 |
| `sum_duration_ms` / `avg_duration_ms` | 仅统计采样记录；无采样时均值为 0 |
| `error_rate` | `error_count / sampled`，取值 `[0, 1]`；无采样时为 0 |
| `by_status` | 采样记录按状态码分桶计数 |

要点：

1. **去重与采样解耦**：未采样记录仍占 `total`，但不进入任何统计口径，
   不会污染错误率、耗时与状态码分布。
2. **快照一致性**：`snapshot()` 在锁内完成全部聚合，并发上报下不会出现
   “total 已加、分桶还没加”的撕裂视图。
3. **记录不可变**：`RequestRecord` 与 `MetricSnapshot` 均为冻结对象，
   发布后外部无法篡改。

### 3.1 来源维度（source）

每条记录归属且仅归属一个**生效来源**，上报时通过 `source` 字段携带
（中间件自动指标则取请求头 `X-Source`）：

- **未携带/空白来源**：归一化为默认来源 `default`，与老版本行为完全一致，
  旧上报不需要任何改动；
- **归一化**：来源名去首尾空白、超长截断到 128 字符，非字符串按缺省处理；
- **对账不变式**：全局快照 = 各生效来源快照之和（`total` / `sampled` /
  `error_count` / `sum_duration_ms` / `by_status` 逐项成立）。每条记录只落
  一个桶，不会重复计数也不会漏算；同一 `request_id` 跨来源重复上报仍按
  first-write-wins，只计入第一次的来源；
- **查询口径**：`GET /metrics` 返回全局汇总；`GET /metrics/sources`
  返回每个来源各自的快照（状态码分布、错误率、耗时口径与全局一致）
  以及来源上限/溢出的元信息。

### 3.2 来源数量上限与溢出策略

来源是外部输入，不能无界增长。注册表对**生效来源数量**设上限
`max_sources`（默认 100，`create_app(max_sources=...)` 可配）：

- 上限内：每个新来源独立成桶；
- **超限后**：新来源归并到保留桶 `__overflow__`——**数据不丢弃**，
  该桶照常参与全局与各来源口径，因此对账不变式仍然成立；
  已存在的来源和已统计的数字不受溢出影响；
- **可观察**：`GET /metrics/sources` 的 `stats` 字段给出
  `max_sources` / `source_count` / `overflowed` / `overflow_records`
  （被归并的记录条数）/ `overflow_sources_seen`（被归并的不同来源名数），
  能明确看出是否发生了归并；
- 内存有界：生效来源桶数 ≤ `max_sources + 1`（保留桶），被归并来源名
  样本集合也有上限（1024 个），乱造来源不会拖垮服务。

## 4. 告警语义

### 4.1 支持的指标

`error_rate`、`error_count`、`latency_avg`、`latency_p95`
（P95 使用最近秩 nearest-rank 口径：`ceil(0.95 * n)`）。

### 4.2 生命周期

以**规则名**为 `fingerprint`（形如 `rule:<name>`），一条规则同一时间只保留
一条进行中的有效告警。评估按“轮”进行：每调用一次 `POST /alerts/evaluate`，
每条当前规则产生一次观测（后台收敛循环每 0.2s 执行一次 sweep，但不产生观测）。

**来源维度（`by_source: true`）**：规则按窗口内样本的生效来源分组，每个来源
独立计算指标、独立判定，`fingerprint` 形如 `rule:<name>|source:<source>`——
同一条规则在每个来源上各有完整的告警周期：一个来源持续触发正常合并去重和
升级，另一个来源同时触发不跟着计数也不被合并；某个来源窗口内不再有样本时
不产生观测，其在飞告警随窗口到期**独立收敛**，不收其它来源的告警也不漏自己的。
`by_source` 缺省为 `false`：老规则集（没有该字段）加载不失败，行为与之前
完全一致（全局口径、一条告警流、`fingerprint` 为 `rule:<name>`）。

1. `triggered`：某轮观测中窗口指标首次 `>= threshold`，产生一条 `firing` 告警，
   初始级别为规则配置的 `severity`（默认 `warning`），`count=1`。
2. `repeated`：后续轮次继续命中，只累加到同一条告警的 `count`（每轮 +1）
   并刷新 `last_seen`（窗口因此自动续期），**不会重复告警**。
3. `upgraded`：当 `count` 达到 `1 + upgrade_after` 时升级为 `critical`，
   且**只升级一次**；`upgrade_after=0` 表示不升级。
   即 `upgrade_after=2` 表示“首次触发后，再连续命中 2 轮则升级”。
4. `resolved`：距 `last_seen` 超过 `window_seconds` 没有新命中，
   由收敛扫描（每 0.2s）或下一轮评估前的 sweep 明确置为 `resolved`，
   并记录 `resolved_at`。
5. 收敛后再次越阈视为**新一轮异常**，产生新的告警周期（计数从 1 重新开始）。

设计取舍：单次“未越阈”样本不会立即关闭告警，异常是否结束**只以时间窗口
为准**，避免一次抖动样本导致告警反复开合（flapping）。

### 4.3 并发保证

`AlertEngine.evaluate` 的判定在同一把锁内串行完成：

- N 个并发命中只有 1 个调用得到 `triggered`，其余全部 `repeated`/`upgraded`，
  `count` 精确等于 N；
- 升级临界区同样串行，`upgraded` 恰好出现一次；
- 规则即使被热更新删除，在飞告警也会按创建时的 `window_seconds` 按时收敛。

### 4.4 规则热更新与降级

- 规则**整批替换**，逐条校验：坏规则单独跳过，错误信息形如
  `规则[<name>] <字段>: <原因>`，每条原因可区分，**不影响同批其它规则**；
- 只要至少一条合法，就原子切换为新版本（版本号 +1，读路径无锁）；
- 整批全部非法或为空时**保留上一版可用规则**，版本号不推进，
  返回 `ok=false` 与具体失败原因；
- 同批规则重名时，保留第一条，其余按坏规则拒绝并说明“重名”。

规则字段约束：

| 字段 | 约束 |
| --- | --- |
| `name` | 非空字符串，同批唯一 |
| `metric` | `error_rate` / `error_count` / `latency_avg` / `latency_p95` |
| `threshold` | 数值且非负；`error_rate` 必须 ≤ 1 |
| `window_seconds` | 正数 |
| `severity` | `warning` / `critical` |
| `upgrade_after` | 非负整数，持续命中多少次后升级（0 不升级） |
| `by_source` | 布尔，缺省 `false`；`true` 时按来源分别评估与告警 |

示例见 `config/rules.example.json`。

## 5. HTTP 接口

| 方法 路径 | 说明 |
| --- | --- |
| `POST /metrics/report` | 幂等上报一次请求指标（可带 `source`）；返回 `accepted/duplicate` 与实际生效来源 `source` |
| `GET  /metrics` | 全局指标一致快照（= 各来源之和） |
| `GET  /metrics/sources` | 按来源的一致快照 + 来源上限/溢出归并的可观察口径（`stats`） |
| `POST /alerts/evaluate` | 对当前规则集评估一轮（评估前先收敛），返回全部告警（含 `source` 字段） |
| `GET  /alerts` | 查看进行中告警与最近收敛历史 |
| `POST /rules/reload` | 整批热更新规则，返回加载/拒绝数与逐条失败原因 |
| `GET  /rules` | 查看当前规则版本与内容 |

## 6. 本地运行

```bash
uv sync --group dev
uv run uvicorn app.main:app --reload
# 或 uv run python -m app.main
```

快速手工验证：

```bash
# 1) 热更新规则（故意混入一条坏规则，观察隔离与原因）
curl -s localhost:8000/rules/reload -H 'content-type: application/json' -d '{
  "rules": [
    {"name":"err","metric":"error_count","threshold":1,"window_seconds":10,"upgrade_after":2},
    {"name":"bad","metric":"unknown","threshold":1,"window_seconds":10}
  ]
}'

# 2) 同一 request_id 上报两次，第二次 duplicate=true
curl -s localhost:8000/metrics/report -H 'content-type: application/json' \
  -d '{"request_id":"k1","duration_ms":12,"status_code":500,"error":"boom"}'
curl -s localhost:8000/metrics/report -H 'content-type: application/json' \
  -d '{"request_id":"k1","duration_ms":12,"status_code":500,"error":"boom"}'

# 3) 评估告警（首次 triggered，继续上报再评估会看到升级 critical）
curl -s -X POST localhost:8000/alerts/evaluate

# 4) 等待超过窗口（10s）后再次评估，告警变为 resolved
curl -s localhost:8000/alerts
```

来源维度手工验证：

```bash
# 1) 加载一条 by_source 规则（按来源分别告警）
curl -s localhost:8000/rules/reload -H 'content-type: application/json' -d '{
  "rules": [
    {"name":"err_by_src","metric":"error_count","threshold":1,
     "window_seconds":30,"upgrade_after":1,"by_source":true}
  ]
}'

# 2) 两个来源各自上报错误；一条不带来源（落入 default）
curl -s localhost:8000/metrics/report -H 'content-type: application/json' \
  -d '{"request_id":"w1","duration_ms":12,"status_code":500,"error":"boom","source":"web"}'
curl -s localhost:8000/metrics/report -H 'content-type: application/json' \
  -d '{"request_id":"a1","duration_ms":12,"status_code":500,"error":"boom","source":"app"}'
curl -s localhost:8000/metrics/report -H 'content-type: application/json' \
  -d '{"request_id":"d1","duration_ms":12,"status_code":200}'

# 3) 按来源查看指标（web/app/default 各自口径，合计等于全局）
curl -s localhost:8000/metrics/sources
curl -s localhost:8000/metrics

# 4) 评估：web 与 app 各自产生一条独立告警（source 字段区分）
curl -s -X POST localhost:8000/alerts/evaluate
```

## 7. 测试与验证

```bash
uv run pytest -q
```

测试按**业务域**组织，运行日志会分组打印“覆盖了哪些业务、每个业务下每条
用例的 PASS/FAIL 与小计”，由 `tests/bizlog.py` 插件输出：

| 测试文件 | 覆盖业务 |
| --- | --- |
| `tests/test_tracing_metrics.py` | 请求级指标与链路标识：唯一 id、上下文隔离、幂等、采样口径、并发同 id/不同 id/采样切换 |
| `tests/test_source_metrics.py` | 来源维度指标与上限归并：默认来源、归一化、全局与分来源对账、来源隔离、并发多来源、超限归并可观察 |
| `tests/test_alert_engine.py` | 窗口告警判定与合并去重：触发/重复/升级/续期/收敛/新周期，并发下只一条告警、升级恰好一次 |
| `tests/test_source_alerts.py` | 来源维度告警：by_source 各来源独立触发/升级/收敛、老规则兼容、并发评估计数精确、上报/热更新/评估混合并发 |
| `tests/test_rule_store.py` | 规则热更新与降级：逐条隔离、原因可区分、整批失败保留上一版、重名、并发热更新版本严格递增 |
| `tests/test_evaluation.py` | 窗口聚合与编排：四种指标口径、窗口边界、无样本不触发、评估前自动收敛 |
| `tests/test_api.py` | HTTP 端到端：链路响应头、上报幂等、接口级并发一致性、热更新接口、告警全生命周期 |
| `tests/test_source_api.py` | 来源维度 HTTP 端到端：上报携带来源、/metrics/sources 对账、X-Source 头、超限归并可观察、by_source 告警端到端、老规则集兼容 |

并发场景通过多线程 + `Barrier` 同时触发来验证“同一临界区只有一个赢家、
计数无丢失”；时间相关语义通过可注入时钟（`FakeClock`）做确定性验证，
不依赖真实睡眠。
