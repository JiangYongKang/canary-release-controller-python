# Canary Release Controller（灰度发布编排）

一套**本地可运行**的灰度发布编排能力：用本地状态存储模拟制品登记、环境与观测指标，
驱动发布从旧版本逐步扩量到新版本，依据观测窗口内的失败信号自动回滚，并通过
**版本化策略快照 + 不可变历史台账**让发布结论在 HTTP 接口、引擎直调、独立 CLI/脚本
进程和进程重启各入口下保持一致且可解释。

- 运行时：Python 3.14 + FastAPI（仅本地 JSON 文件 + 文件锁，无外部依赖）
- 包结构：

```
canary/
  errors.py   # 可区分的领域错误码（拒绝原因）
  models.py   # 状态机枚举、发布、观测点、结论等纯数据模型
  policy.py   # 版本化、不可变策略（由共享存储支撑）；旧记录缺省语义；确定性序列化
  store.py    # 跨进程本地存储：flock 事务 + 状态重载 + 崩溃安全落盘 + 幂等台账 + 历史
  engine.py   # 状态机、观测窗口与阈值判定、自动回滚、并发/幂等事务
  service.py  # 应用门面（HTTP / CLI / 测试共用同一入口）
  api.py      # FastAPI HTTP 入口
  cli/main.py # 命令行入口（canary）：独立进程操作同一份本地状态
tests/        # 单测：状态机、阈值边界、线程并发幂等、策略历史、HTTP、跨重启、多进程
scripts/demo.py             # 单进程端到端演示
scripts/multiprocess_demo.py# 多进程并发 + 跨重启续跑演示（真实子进程）
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

## 4. 幂等、并发与多进程保证

- 所有变更（推进 / 回滚 / 登记 / 上报 / 建策略）都在存储的**同一个事务**内作为单个
  读—改—写执行；事务可重入，线程间由可重入锁串行，**进程间由 `flock` 排他锁串行**。
- 每个顶层事务拿到锁后都会从磁盘重新加载状态，因此常驻服务与旁边的命令行 /
  后台脚本操作**同一个状态文件**时，任何入口都能立刻看到别人刚提交的发布进展、
  历史结论与可用策略版本，不会各算各的。
- 幂等台账以 `(release_id, request_id)` 为键，并且和状态一起落盘：
  - 相同 `request_id` + **相同动作** ⇒ 重放首次结论（`idempotent_hit=true`），
    不重复推进、不重复回滚、不重复写历史（跨进程、跨重启同样成立）；
  - 相同 `request_id` + **不同动作**（先 promote 后 rollback）⇒ `illegal_state` 拒绝；
  - 不带 / 使用全新 `request_id` 操作终态发布 ⇒ `release_finished` 拒绝
    （与"幂等重放成功"明确区分）。
- 自动回滚同样受保护：两个进程同时提交 promote（窗口不健康时会自动回滚）与 rollback，
  结果与单进程并发完全一致——版本占比**只逐档上升**、**回滚最多一次**、历史中
  **只有一条结论**，其余调用要么拿到同一条结论，要么收到 `release_finished`，
  不会出现重复回滚、漏回滚或 `completed`/`rolled_back` 互相矛盾，也不会因为抢写
  漏记台账。
- **崩溃安全写入**：事务内的修改先在内存累积，只在最外层事务结束时落盘一次，
  采用「写临时文件 → `fsync` → 原子 `rename` → `fsync` 目录」。进程在写入过程中
  被杀，状态文件只可能是**上一份完整状态或新的完整状态**，不会留下写一半的 JSON；
  临时文件是 `state.json.tmp.<pid>`，从不作为状态读取。重启后用同一个 `request_id`
  重试，幂等台账保证结论仍然只产生一次。
- 持久化文件路径：
  - `uvicorn main:app` 常驻服务默认使用当前目录 `./canary-state.json`，可用环境变量
    `CANARY_STATE=/path/to/file.json` 覆盖，设为 `CANARY_STATE=memory` 则退回纯内存；
  - CLI 默认同样使用 `./canary-state.json`，可用 `--state` 或 `CANARY_STATE` 指定；
  - 库内直接 `CanaryService()`（不传 `persist_path`）仍是纯内存，便于嵌入式使用与测试。

---

## 4a. 跨重启续跑与兼容范围

- **策略随状态一起持久化**：所有策略版本（阈值、最小样本量、每步观察时长、步长）
  都写入同一份状态文件。用自己登记的非默认策略跑到一半的发布，重启后仍按**原来的**
  `failure_rate_threshold` / `min_requests` / `observe_minutes` 判定，接着推进即可；
  重启后新建的更严格 / 更宽松策略不会影响它，也不会改写已结束发布的结论。
- **未结束的发布**：重启后状态、当前步、已上报的观测点、幂等台账原样恢复；观测窗口
  缺的分钟继续补，补满后照常推进或回滚。
- **已结束的发布**：`completed` / `rolled_back` 结论、`finish_reason` 与不可变历史
  不会因重启、换进程或重复评估而改变；历史中的策略快照可独立复现判定依据。
- **旧格式兼容（缺省含义稳定）**：读取旧状态文件时对缺失字段一律套用固定缺省值
  （与第 3 节一致，永远不是"当前值"）：
  - 文件中没有策略台账（旧版本写出的文件）⇒ 为发布引用到的每个策略版本补入
    `threshold=0.05, min_requests=100, steps=[10,30,50,100], observe_minutes=5`；
    发布自身保存的步长 / 观察时长不受影响；
  - 发布记录缺 `policy_version` ⇒ `1`，缺 `updated_at` 等时间字段 ⇒ 空串；
  - 历史记录缺 `reason` / `finished_at` ⇒ 空串，缺 `policy_snapshot` ⇒ 空对象，
    不影响终态结论的读取。
- 兼容范围：本地单机、同一状态文件路径、POSIX 文件锁（Linux/macOS）；不支持多机
  共享网络盘的场景。

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

启动常驻服务（默认文件后端 `./canary-state.json`，可用 `CANARY_STATE` 覆盖）：

```bash
uv sync --group dev
uv run uvicorn main:app --reload
```

---

## 5a. 命令行入口（独立进程共享同一状态）

`uv sync` 后即安装 `canary` 脚本；它和常驻服务操作同一个状态文件，可用
`--state PATH` 或 `CANARY_STATE` 指定路径（默认 `./canary-state.json`）。

| 命令 | 说明 |
| --- | --- |
| `canary policy-create --name n --steps 10,50,100 --threshold 0.1 --min-requests 50 --observe-minutes 1` | 创建新策略版本 |
| `canary policy-list` | 列出策略版本 |
| `canary register --service s --environment prod --old v1 --new v2 --policy-version 2` | 登记发布 |
| `canary get <id>` | 查询发布 |
| `canary observe <id> --step 0 --minute 0 --total 100 --failed 0` | 上报一分钟观测点 |
| `canary promote <id> [--step-index N] [--request-id R]` | 推进 / 评估 |
| `canary rollback <id> [--reason ...] [--request-id R]` | 回滚 |
| `canary evaluate <id>` | 只判定当前窗口 |
| `canary history [--service s]` | 不可变历史结论 |

成功输出 JSON 并返回码 0；领域错误输出 `{"error":{"code":...}}` 并返回码 1。

---

## 6. 本地验证方法

以下命令在仓库根目录依次执行即可，**不需要任何额外前置设置**（仅依赖 `uv`，
会自动使用仓库内的 `.venv`）：

```bash
# 安装依赖（同时装好 canary 命令行脚本）
uv sync --group dev

# 全量单测：状态机、阈值边界、线程内并发幂等、策略历史、HTTP、
#           跨重启续跑、真实子进程多进程并发
uv run pytest

# 单进程端到端演示（真实 HTTP 调用：健康完成 + 边界自动回滚 + 幂等 + 历史）
uv run python scripts/demo.py

# 多进程 + 跨重启演示：CLI 子进程间共享状态、12 进程并发抢同一发布、重启续跑
uv run python scripts/multiprocess_demo.py
```

### 常驻服务 + 另一个进程（CLI）共享与重启续跑

用一个临时状态文件把常驻服务和 CLI 指向同一份状态，复制整段即可跑通：

```bash
export CANARY_STATE="$(pwd)/canary-state.json"

# 终端 A：启动常驻服务
uv run uvicorn main:app --port 18080

# 终端 B：另一个独立进程，通过 CLI 操作同一份状态
RID=$(uv run canary register --service demo --environment prod \
  --old v1 --new v2 | python -c 'import json,sys;print(json.load(sys.stdin)["release_id"])')
uv run canary promote "$RID" --request-id enter
for m in 0 1 2 3 4; do
  uv run canary observe "$RID" --step 0 --minute "$m" --total 100 --failed 0
done
uv run canary promote "$RID" --request-id step1   # -> canary 30%
uv run canary get "$RID"                          # 任意进程看到一致状态

# 重启服务（Ctrl+C 后重新启动），未结束的发布按原策略继续：
#   uv run uvicorn main:app --port 18080
# 重启后用同一个 request_id 重试会重放同一结论，不会重复推进：
uv run canary promote "$RID" --request-id step1   # 仍是 step1
uv run canary history
```

并发正确性的本地压测（两个入口同时对同一发布提交推进和回滚；`&` 让多个进程真正
同时抢锁）：

```bash
export CANARY_STATE="$(pwd)/canary-state.json"
RID=$(uv run canary register --service race --environment prod \
  --old v1 --new v2 | python -c 'import json,sys;print(json.load(sys.stdin)["release_id"])')
uv run canary promote "$RID" --request-id enter
# 先填满 step 0 的 5 分钟观测窗口，失败率 20%（>= 5% 阈值，判不健康）
for m in 0 1 2 3 4; do
  uv run canary observe "$RID" --step 0 --minute "$m" --total 100 --failed 20
done
# 4 个推进先启动（不健康窗口会让赢家的 promote 自动转成回滚），
# 4 个显式回滚后启动，8 个进程真正同时抢同一把锁
for i in 1 2 3 4; do
  uv run canary promote  "$RID" --request-id "p$i" &
done
for i in 1 2 3 4; do
  uv run canary rollback "$RID" --request-id "r$i" &
done
wait
uv run canary get "$RID"        # status 必为 rolled_back
uv run canary history           # 该发布只有一条 rolled_back 结论
```

### 测试内容速览

- `tests/test_state_machine.py` / `test_observation.py`：状态机迁移、越级拒绝、
  窗口完整性、样本量与阈值边界（`>=`）。
- `tests/test_idempotency.py`：单进程内多线程屏障并发，推进 / 回滚 / 混合并发的
  唯一结论与幂等重放。
- `tests/test_policy_history.py`：策略不可变、旧记录稳定缺省、策略变更不改写历史。
- `tests/test_api.py`：HTTP 错误码与入口一致性。
- `tests/test_persistence.py`：同进程模拟重启——自定义策略续跑、终态/历史跨重启
  不变、台账跨重启重放、两个 service 共享一个文件、旧格式文件加载、写入中途被杀
  不留半成品且重启后重试只提交一次。
- `tests/test_multiprocess.py`：真实独立 OS 进程（CLI 子进程）并发——相同 / 不同
  request_id 的推进、推进+回滚混合、回滚只发生一次、重启后重试结论唯一。

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
