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
  policy.py   # 版本化、不可变策略；旧记录缺省语义；确定性序列化（落在共享状态文件）
  store.py    # 跨进程事务存储：flock + 事务起始重载 + 单次原子落盘；幂等/历史/策略
  engine.py   # 状态机、观测窗口与阈值判定、自动回滚、并发/幂等事务
  service.py  # 应用门面（HTTP / CLI / 测试共用同一入口）
  api.py      # FastAPI HTTP 入口（支持 CANARY_STATE_PATH 共享状态文件）
  cli.py      # 命令行入口（python -m canary.cli，独立进程操作同一状态文件）
tests/        # 43 个单测：状态机、阈值边界、并发幂等、策略历史、HTTP、多进程、重启续跑
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

## 4.5 多进程共享与跨重启续跑

典型用法：一台机器上常驻一个本地服务（HTTP），旁边再挂 CLI 或后台脚本操作
**同一份发布状态**；机器重启、进程重启是常态。所有入口都用同一个**本地状态文件**
（`persist_path` / `CANARY_STATE_PATH`），文件里同时保存发布、幂等台账、历史台账与
**全部策略版本**。

### 多入口看到的是同一份状态

- 每次操作都是一个**跨进程事务**：进入事务先拿进程内可重入锁，再对同目录的
  `.lock` 文件加 `flock` 排他锁（串行化不同进程），**并从磁盘重新加载状态文件**。
  因此常驻服务、CLI、后台脚本无论谁刚写过，别的入口下一次操作立刻看得到：
  发布走到哪一步、历史结论、有哪些策略版本完全一致，不存在各算各的或读到旧副本。
- **策略也持久化在状态文件里**：在 A 进程创建的自定义策略，B 进程和重启后的新进程
  都能列出并使用；策略版本号在跨进程并发创建时仍然连续不冲突（编号分配在事务内完成）。

### 跨进程并发结论跟单进程完全一致

两个进程同时对同一发布提交推进/回滚时，事务把整个读—改—写串行化：

- 版本占比只能一步一步往上爬：同一健康窗口上多个不同 `request_id` 的并发推进，
  恰好一个进入下一步，其余在新窗口上得到 `observation_pending`；
- 回滚最多发生一次：混发 promote（不健康自动回滚）与 rollback，恰好一个调用产出
  `rolled_back` 结论并写一条历史，其余得到 `release_finished`；
- 相同 `request_id` 跨进程重复提交，仍然重放同一结论，历史不重复；
- **写入中途进程被杀 / 机器掉电**：整个事务只在最外层成功结束时落盘一次，
  采用「唯一临时文件 → `fsync` → 原子 `os.replace` → 目录 `fsync`」，
  所以要么保留旧的完整快照、要么是新的完整快照，不会留下写一半的状态；
  事务中途抛错时，内存里的半成品会被丢弃并回滚到磁盘已提交快照，重试不会
  产生重复结论。遗留的 `.canary-state.*.tmp` 临时文件不影响加载，可随时删除。

### 跨重启续跑与兼容范围

- **未走完的发布可以接着推进**：重启后新进程从文件重建一切，包括发布绑定的
  `policy_version`；当初用自定义（非默认）策略登记的发布，继续按**该策略当时的**
  失败比例阈值、样本量要求、每步观察时长判定，不会被后来新建的策略影响。
- **已结束的发布结论永不改变**：重启、换进程、重复评估都不会改写终态或历史；
  历史里的 `policy_snapshot` 保持写入时的字节级内容。
- **旧格式文件缺字段的缺省含义保持稳定**：
  - 老文件没有 `policies` 段时，打开即透明补回默认策略 v1（固定默认
    阈值 0.05 / 样本 100 / 步长 `[10,30,50,100]` / 每步 5 分钟），
    引用 v1 的老发布可直接继续；
  - 步长缺 `observe_minutes` → 5；发布/历史记录缺
    `created_at` / `updated_at` / `finish_reason` / `reason` /
    `policy_snapshot` / `finished_at` → 空字符串或空对象，语义与第 3 节一致。
- 兼容范围说明：状态文件面向**同一台机器、可信本地进程**之间共享（Unix `flock`，
  Linux/macOS 可用）；不面向网络文件系统上的多机协作。不给 `persist_path` 时
  退化为纯内存模式（单进程，不落盘）。

### 三种入口指向同一状态文件

```bash
# 1) 常驻 HTTP 服务
export CANARY_STATE_PATH=/tmp/canary/demo-state.json
uv run uvicorn main:app --port 8000 &

# 2) CLI（默认就读 CANARY_STATE_PATH；也可用 --state 显式指定）
uv run python -m canary.cli register \
  --service checkout --environment prod --old v1 --new v2

# 3) 后台脚本同样用 CanaryService(persist_path=os.environ["CANARY_STATE_PATH"])
```

CLI 支持的子命令：`list-policies` / `create-policy` / `register` / `status` /
`promote` / `rollback` / `observe` / `evaluate` / `history`，输出为单行 JSON；
领域错误时进程退出码为 1 且带 `error.code`，便于脚本判断。

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

# 全量单测（43 个）：除原有场景外，新增真正起独立 OS 进程的多进程共享、
# 跨进程并发推进/回滚、跨重启续跑（含自定义策略）、终态不可变、旧格式缺省、
# 原子落盘与进程被杀等用例
uv run pytest -s          # -s 可看到输入、状态迁移与判定依据日志
uv run pytest             # 简洁模式
uv run pytest tests/test_multiprocess.py tests/test_restart.py  # 只跑新场景

# 端到端演示（走真实 HTTP 调用，含健康完成 + 边界自动回滚 + 幂等 + 历史）
uv run python scripts/demo.py

# 多进程 + 跨重启演示（每个动作都在独立 OS 进程里跑，自动用临时目录，无需设置）
uv run python scripts/multiprocess_demo.py

# 手动起服务（与 CLI 共享同一状态文件）
export CANARY_STATE_PATH=/tmp/canary/demo-state.json
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

### 一键验证多进程共享 + 跨重启续跑（无需任何前置设置）

下面整段复制到终端即可运行：每条 `python -m canary.cli` 都是**独立进程**，
全程只依赖仓库自带的 `.venv` 与一个临时状态文件。

```bash
cd "$(git rev-parse --show-toplevel 2>/dev/null || echo .)"
D=$(mktemp -d); export CANARY_STATE_PATH="$D/state.json"
CLI="uv run python -m canary.cli --state $CANARY_STATE_PATH"

# 进程 A：建一个自定义（非默认）策略并登记、进入第一步
$CLI create-policy --name relaxed --steps 20,60,100 --threshold 0.30 \
  --min-requests 250 --observe-minutes 3
RID=$($CLI register --service checkout --environment prod --old v1 --new v2 \
      --policy-version 2 | python3 -c 'import sys,json;print(json.load(sys.stdin)["release_id"])')
$CLI promote "$RID" --request-id enter

# 进程 B（全新进程）：立刻看得到 A 的策略、发布和进度
$CLI list-policies
$CLI status "$RID"

# 进程 B：上报部分窗口后，模拟"机器重启"——换一个全新进程继续判定/推进
$CLI observe "$RID" --step 0 --minute 0 --total 200 --failed 0
$CLI observe "$RID" --step 0 --minute 1 --total 0  --failed 0
$CLI evaluate "$RID"                 # 窗口仍不满 -> error.code=observation_pending, 退出码 1
$CLI observe "$RID" --step 0 --minute 2 --total 50 --failed 0
$CLI promote "$RID" --request-id after-restart   # 重启后续跑：进入 step1 @60%
$CLI history                         # 未结束 -> 无结论

# 跨进程幂等：新进程用旧 request_id 重放，得到同一结论、不重复推进
$CLI promote "$RID" --request-id after-restart

# 并发安全：16 个独立进程同时回滚（同一 request_id），只产生一次结论
for i in $(seq 1 16); do $CLI rollback "$RID" --request-id the-rb & done; wait
$CLI status "$RID"                   # status=rolled_back
$CLI history | python3 -c 'import sys,json;print("history entries:",len(json.load(sys.stdin)["history"]))'

rm -rf "$D"
```

预期：`list-policies` 能看到 v1+v2；重启后的 `evaluate` 仍按 **3 分钟 / 250 样本 /
阈值 0.30** 判定；`after-restart` 推进到 60%，其重放结论不变；16 个并发回滚后
`history entries: 1`。

