# Canary Release Controller（本地灰度发布编排）

一套可在本地运行的灰度发布编排能力：用本地内存/JSON 状态存储模拟制品库、
环境与观测指标，支持灰度推进、观测驱动的自动回滚、策略版本化与历史复现。
HTTP（FastAPI）、CLI 与测试共享同一个 `ReleaseService`，因此各入口下的
推进、回滚与历史结论保持一致。

## 1. 快速开始

```bash
uv sync                       # 安装依赖
uv run python -m pytest -q    # 运行全部单测（含并发/边界/复现）
uv run python main.py         # 启动 HTTP 服务: http://127.0.0.1:8000/docs
uv run python -m canary.demo  # 终端演示：灰度→超阈值→自动回滚→历史复现
```

默认策略（v1 缺省，稳定语义）：

| 字段 | 缺省值 | 含义 |
| --- | --- | --- |
| `failure_rate_threshold` | `0.05` | 失败率回滚阈值（**严格大于**才回滚） |
| `min_samples` | `10` | 观测窗口内最少样本数 |
| `stages` | `10%/60s → 50%/120s → 100%/180s` | 灰度台阶权重与每阶观察时长 |
| `min_observe_seconds` | `60` | 旧记录台阶缺失观察时长时的回退值 |

## 2. 发布状态机

```
                 register                promote            promote           promote
   (none) ───────────────────▶ PENDING ──────────▶ CANARY@0 ───────▶ CANARY@1 ───────▶ CANARY@2 ──promote──▶ PROMOTED
                                  │                    │                 │                  │            (终态)
                                  │ rollback           │ rollback/       │ rollback/        │ rollback/
                                  │                    │ auto_rollback   │ auto_rollback    │ auto_rollback
                                  ▼                    ▼                 ▼                  ▼
                                                           ROLLED_BACK（终态，权重=0）
```

- 权重严格递增，最后一阶必须是 `100`；从最后一阶再 `promote` 进入 `PROMOTED`。
- `PROMOTED` 与 `ROLLED_BACK` 是终态，任何后续推进/回滚都被拒绝。
- 进入每个灰度台阶时记录 `stage_entered_ts`，观测窗口从该时刻重新计时；
  推进后上一阶的样本不会泄漏进新窗口。

### 非法操作与可区分拒绝原因

所有拒绝都抛出 `CanaryError`，HTTP 统一映射为 `409`，响应体
`{"error": <code>, "reason": <说明>}`，code 可机读区分：

| code | 触发条件 |
| --- | --- |
| `release_not_found` | 发布单不存在 |
| `release_already_exists` | 同 ID 发布单已存在且非幂等重试 |
| `release_already_finished` | 对 `promoted/rolled_back` 终态的重复推进/回滚；从最后一阶重复全量 |
| `illegal_state` | 同一 service+environment 已有进行中发布；不可迁移状态 |
| `invalid_stage` | 显式目标台阶越界 |
| `stage_skipped` | 越级推进（目标不是紧邻的下一阶） |
| `observation_pending` / `healthy` | 未回滚时的非错误判定（体现在评估结论而非异常） |
| `policy_not_found` / `policy_already_exists` / `policy_migration_error` | 策略相关 |
| `invalid_payload` | 请求内容非法（如新旧版本相同） |

## 3. 健康与自动回滚判定规则

每次评估（`evaluate_health` / `auto_check`）对**当前台阶的观测窗口**
`[stage_entered_ts, now]` 内的样本执行纯函数 `canary.health.evaluate`，
判定顺序固定、可解释（结论写入 `verdict.detail.decision`）：

1. **观测窗口必须已满**：`elapsed >= observe_seconds`。
   窗口未满（哪怕 100% 失败）→ `OBSERVATION_PENDING`，**不回滚**；
   `elapsed == observe_seconds` 恰好算满（边界不误判）。
2. **样本量必须达标**：`sample_count >= min_samples`，否则
   `NOT_ENOUGH_SAMPLES`，**不回滚**；恰好相等算达标。
3. **失败率严格大于阈值**才回滚：`failure_rate > threshold`；
   `failure_rate == threshold` 视为健康（例如阈值 0.05 时 1/20 不回滚，2/20 回滚）。

`auto_check` 在同一把存储锁内完成“评估→迁移”，超阈值时原子地写入
`ROLLED_BACK` 与一条 `auto_rollback` 事件；窗口未满/样本不足/未越阈值时
只记录评估结论，不改状态。

## 4. 幂等与并发保证

- **单一粗粒度锁**：所有发布写操作（读-判-写）都在 `Store` 的同一把
  `RLock` 内完成，并发的推进/回滚/自动检查被**线性化**，不存在
  读-判-写之间的状态竞争。
- **幂等键**：`promote/rollback/register` 支持 `idempotency_key`。
  相同键的并发/重试请求只产生一次真实迁移，其余返回首次结果。
- **回滚风暴**：无键并发回滚时恰好一个请求生效，其余收到
  `release_already_finished`；不会重复回滚，也不会漏回滚（窗口满足条件时）。
- **推进 vs 自动回滚竞争**：锁决定唯一胜者——
  - 自动回滚先拿到锁：发布变 `ROLLED_BACK`，随后的推进被终态规则拒绝；
  - 推进先拿到锁：进入新台阶、窗口重置，`auto_check` 不会基于旧窗口误回滚，
    新台阶窗口满且仍超阈值时才回滚。
- 状态落盘采用“临时文件 + 原子 rename”（可选 JSON 文件存储）。

## 5. 策略版本化与历史复现

- 每个策略保存不可变的版本列表（version/definition/fingerprint/created_at）；
  内容相同的提交复用既有版本，不产生重复版本号。
- **发布单登记时固化策略快照**（完整定义 + policy_version + fingerprint），
  之后新建任何策略版本都不会影响已结束发布。
- **旧记录缺省迁移**（`policy.migrate_policy`）是确定性的纯函数：
  缺失字段永远按 v1 缺省补齐，台阶缺 `observe_seconds` 时回退到
  `min_observe_seconds`；无法识别的结构抛 `policy_migration_error`。
- **历史复现**：`GET /releases/{id}/history` 用发布单内固化的快照阈值/样本下限
  重建历史样本并重新调用同一个纯函数评估，逐次比较回滚结论，`reproduced`
  恒为 `true`。相同输入 + 相同策略重复评估必然得到相同结论。

## 6. HTTP 接口一览

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /releases` | 登记发布（可带 `policy_name/policy_version/idempotency_key`） |
| `GET /releases` / `GET /releases/{id}` | 列表 / 详情（含事件与评估历史） |
| `POST /releases/{id}/promote` | 推进一阶（可带 `target_stage`、幂等键） |
| `POST /releases/{id}/rollback` | 手动回滚（可带幂等键） |
| `GET /releases/{id}/health` | 只读健康评估并记录结论 |
| `POST /releases/{id}/auto-check` | 健康评估，超阈值则自动回滚 |
| `GET /releases/{id}/history` | 策略快照重放历史结论 |
| `POST /metrics` | 写入成功/失败观测信号（模拟监控） |
| `GET /policies` · `POST /policies/{name}/versions` · `GET /policies/{name}/versions/{v}` | 策略版本化 |

### curl 示例

```bash
curl -s -X POST localhost:8000/releases -H 'content-type: application/json' -d '{
  "release_id":"r1","service":"checkout","environment":"prod",
  "old_version":"1.2.0","new_version":"1.3.0"}'
curl -s -X POST localhost:8000/releases/r1/promote -d '{}' -H 'content-type: application/json'
for i in $(seq 1 20); do
  curl -s -X POST localhost:8000/metrics -H 'content-type: application/json' -d \
    '{"service":"checkout","environment":"prod","success":false}' >/dev/null
done
# 窗口未满：不回滚；60s 后再调用：自动回滚
curl -s -X POST localhost:8000/releases/r1/auto-check | python -m json.tool
curl -s localhost:8000/releases/r1/history | python -m json.tool
```

## 7. 代码结构

```
canary/
  models.py        领域模型（状态枚举、发布单、台阶、评估结论、序列化）
  errors.py        可区分错误码与 CanaryError
  clock.py         时钟抽象（FrozenClock 用于测试/复现）
  policy.py        缺省策略、确定性迁移、内容指纹
  statemachine.py  纯函数状态机：推进/回滚合法性
  health.py        纯函数健康评估（窗口/样本/阈值边界）
  metrics.py       观测信号注册表与窗口聚合
  store.py         线程安全内存存储 + 可选 JSON 原子落盘
  service.py       编排服务：登记/推进/回滚/自动检查/版本化/复现
  api.py           FastAPI 入口
tests/             状态机、边界、并发、策略复现、HTTP 单测（含日志）
```
