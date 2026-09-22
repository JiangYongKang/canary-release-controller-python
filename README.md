# Canary Release Controller（灰度发布编排）

一套**本地可运行**的灰度发布编排能力：用本地状态存储模拟制品登记、环境与观测指标，
驱动发布从旧版本逐步扩量到新版本，依据观测窗口内的失败信号自动回滚，并通过
**版本化策略快照 + 不可变历史台账**让发布结论在 HTTP 接口、引擎直调和磁盘重载
各入口下保持一致且可解释。

- 运行时：Python 3.14 + FastAPI（仅本地内存 / 本地 JSON 文件，无外部依赖）
- 包结构：

```
canary/
  errors.py   # 可区分的领域错误码（拒绝原因）
  models.py   # 状态机枚举、发布、观测点、结论等纯数据模型
  policy.py   # 版本化、不可变策略；旧记录缺省语义；确定性序列化
  store.py    # 本地状态存储 + 幂等台账 + 不可变历史（可选 JSON 持久化）
  engine.py   # 状态机、观测窗口与阈值判定、自动回滚、并发/幂等事务
  service.py  # 应用门面（HTTP / CLI / 测试共用同一入口）
  api.py      # FastAPI HTTP 入口
tests/        # 32 个单测：状态机、阈值边界、并发幂等、策略历史、HTTP
scripts/demo.py  # 本地端到端演示
```

---

## 1. 发布状态机

```
                 register                promote(进入第 1 步)
   <none>  ---------------->  PENDING  ----------------------->  CANARY @ step0
                                   ^                              |
                                   |     promote(健康窗口后逐级)   |
                                   |   -------------------------->+
                                   |                              |
                            rollback|  promote(最后一步窗口健康)   |
                                   |   ---------------------------v
                                   +------------------------>  COMPLETED   (终态)
                                                                  ^
                                   |                              |
                            rollback (PENDING/CANARY 任意非终态)    |
                                   +------------------------>  ROLLED_BACK (终态)
```

- 状态：`pending` / `canary` / `completed` / `rolled_back`，后两者为**终态**。
- 灰度步长由策略定义，如 `[10, 30, 50, 100]` 表示新版本流量 10%→30%→50%→100%。
- 每一步都有独立观测窗口（`observe_minutes` 分钟）。**所有步完全对称**：进入
  100% 最后一步后仍是 `canary`，其窗口判健康后再调用一次 `promote` 才 `completed`。
- 流量语义：当前步新版本占比 `new_version_percent`，其余流量仍在旧版本。

### 非法操作的拒绝原因（错误码可区分）

| 场景 | 错误码 `error.code` | HTTP |
| --- | --- | --- |
| 发布 / 策略不存在 | `release_not_found` / `policy_not_found` | 404 |
| 同一 (服务, 环境) 已有进行中的发布 | `release_already_exists` | 409 |
| 对已结束发布再次推进 / 回滚 / 上报观测 | `release_finished` | 409 |
| 在未进入的步上上报观测、同一分钟重复上报、request_id 复用于不同动作 | `illegal_state` | 409 |
| 越级推进（`step_index` 不是紧邻的下一步；重复推进当前步也算越级） | `step_skipping` | 409 |
| 观测窗口未满 / 样本不足时推进 | `observation_pending` | 425 |
| 入参非法（空字段、版本相同、失败数 > 总数、策略形态非法等） | `invalid_argument` / `invalid_policy` | 422 |

> 越级等**确定性入参错误**优先于窗口判定：即使窗口已满/未满，越级一律先被拒绝。

---

## 2. 健康与自动回滚判定规则

对**当前步**的观测窗口聚合判定：

1. **窗口必须完整**：观测点的 `minute_offset` 必须恰好覆盖
   `{0, 1, …, observe_minutes-1}`（不能少分钟，也不接受跳分钟）。
2. **样本量必须达标**：窗口内 `Σ total_requests >= min_requests`。
3. 以上任一不满足 ⇒ 抛出 `observation_pending`（HTTP 425），
   **不会产生任何结论，更不会误回滚**——哪怕此刻失败率是 100%。
4. 窗口完整且样本达标后计算：
   `fail_rate = Σ failed_requests / Σ total_requests`。
5. **阈值边界规则：`fail_rate >= failure_rate_threshold` 即不健康**（边界值触发回滚，
   是 `>=` 而不是 `>`）。不健康时，下一次 `promote` 会被转成**自动回滚**：
   返回 `rolled_back=true, to_status=rolled_back`，原因中带完整判定依据。
6. `fail_rate < threshold` 时健康，允许进入下一步（最后一步则完成发布）。
- `min_requests=0` 时退化为只要求窗口分钟完整。

每次判定都会记录结构化日志：`evaluate input={…}` 与
`judgement=healthy|unhealthy|pending reason=…`，可解释阈值、样本与窗口依据。

---

## 3. 策略版本化与历史复现

- 策略一经创建**不可变**并单调编号；登记发布时绑定一个 `policy_version`，
  发布结束时把该策略的**完整快照**（阈值、步长、观察时长等）写入历史结论。
- 事后再创建更严格 / 更宽松的新策略，**不会改写任何已结束发布的结论**；
  新发布按新策略评估。
- **旧记录缺省语义稳定**（永远取固定常量，而非"当前值"）：
  - 缺 `failure_rate_threshold` → `0.05`
  - 缺 `min_requests` → `100`
  - 步长为裸整数 `[10,100]` → 每步 `observe_minutes=5`
  - 缺 `steps` → `[10,30,50,100]`；缺 `name` → `"default"`
- `PromotionPolicy.from_dict(snapshot).to_dict()` 幂等且确定性：
  **相同输入 + 相同策略重复评估必得到相同结论**。

---

## 4. 幂等与并发保证

- 所有变更（推进 / 回滚 / 登记 / 上报）都在存储的**同一把可重入锁**内作为单个
  读—改—写事务执行，消除状态竞争。
- 幂等台账以 `(release_id, request_id)` 为键：
  - 相同 `request_id` + **相同动作** ⇒ 重放首次结论（`idempotent_hit=true`），
    不重复推进、不重复回滚、不重复写历史；
  - 相同 `request_id` + **不同动作**（先 promote 后 rollback）⇒ `illegal_state` 拒绝；
  - 不带 / 使用全新 `request_id` 操作终态发布 ⇒ `release_finished` 拒绝
    （与"幂等重放成功"明确区分）。
- 自动回滚同样受保护：并发的 promote（不健康）与 rollback 混发时，
  只有一个调用产生 `rolled_back` 结论和一条历史，其余拿到同一结论或
  `release_finished`，不会出现重复回滚、漏回滚或 `completed`/`rolled_back` 矛盾。
- 持久化为「写临时文件 + 原子 rename」；进程重启后状态、幂等台账与历史均可重载，
  旧 `request_id` 依然重放同一结论。

---

## 5. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/policies` | 创建新版本策略（`steps` 为百分比数组） |
| GET | `/api/policies` | 列出所有策略版本 |
| POST | `/api/releases` | 登记发布（服务/环境/旧新版本/可选 policy_version） |
| GET | `/api/releases/{id}` | 查询发布状态与当前流量 |
| POST | `/api/releases/{id}/observe` | 上报某一分钟观测点 |
| POST | `/api/releases/{id}/promote` | 推进（body 可带 `step_index`、`request_id`） |
| POST | `/api/releases/{id}/rollback` | 显式回滚（`reason`、`request_id`） |
| GET | `/api/releases/{id}/evaluate` | 只判定当前窗口（不迁移状态） |
| GET | `/api/history?service=` | 不可变历史结论（含策略快照） |
| GET | `/health` | 健康检查 |

启动服务：

```bash
uv sync --group dev
uv run uvicorn main:app --reload
```

---

## 6. 本地验证方法

```bash
# 安装依赖
uv sync --group dev

# 全量单测（32 个）：含并发推进/回滚、越级迁移、阈值边界、策略变更后的历史复现
uv run pytest -s          # -s 可看到输入、状态迁移与判定依据日志
uv run pytest             # 简洁模式

# 端到端演示（走真实 HTTP 调用，含健康完成 + 边界自动回滚 + 幂等 + 历史）
uv run python scripts/demo.py

# 手动起服务
uv run uvicorn main:app --reload
```

单测日志中每个用例都会打印 `INPUT` / `TRANSITION` / `JUDGEMENT` / `REJECT`，
引擎另输出 `register input`、`observe input`、`evaluate input`、`transition`、
`judgement`、`idempotent replay`、`history finalized` 等记录。

### 快速手动 curl 流程

```bash
curl -s -X POST localhost:8000/api/releases -H 'content-type: application/json' \
  -d '{"service":"demo","environment":"prod","old_version":"v1","new_version":"v2"}'
# -> 取 release_id
curl -s -X POST localhost:8000/api/releases/$RID/promote -H 'content-type: application/json' \
  -d '{"request_id":"enter"}'
# 窗口未满推进 -> 425 observation_pending
curl -s -X POST localhost:8000/api/releases/$RID/promote -H 'content-type: application/json' -d '{}'
```
