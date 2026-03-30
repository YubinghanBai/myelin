# myelin — 实施计划（第一性原理、分步迭代）

## 核心术语（Publication 与「审计 LSN」）

### Publication（发布）是什么？

**Publication** 是 PostgreSQL 里的一具「**允许被逻辑复制出去的一篮子对象**」的**命名清单**。你在库里执行例如：

```sql
CREATE PUBLICATION my_pub FOR TABLE workflow_events;
```

之后，`START_REPLICATION ... (publication_names 'my_pub')` 只会对 **列入该 publication 的表** 的变更生成 `pgoutput` 消息。

要点：

- **不在这张清单里的表**，逻辑解码**根本看不到**，从源头避免把「海量审计表」误打进 CDC。
- 一个实例里可有多个 publication；连接器通过启动参数**只订阅其中一个或几个**。

### 「审计 LSN」是什么？

这里不是指 PG 自带某一个叫「审计 LSN」的功能，而是指 **在工程上要把下面几件事查清楚、写进文档与代码审查清单**：

- **何时**调用复制客户端的 **`update_applied_lsn` / 等价反馈**（相对 `XLogData` 的 `wal_end`、`Commit` 的 `end_lsn` 的关系）。
- 是否与 **JetStream `PubAck`**（或你定义的持久化条件）**顺序一致**，有没有「双路径」各写各的导致语义难推理。
- 运维侧 **槽位滞后**（`pg_replication_slots`、`restart_lsn`、`confirmed_flush_lsn`）与 **磁盘/WAL** 的告警阈值。

目标：团队能**证明**「推进槽」与「下游已持久化」之间的因果关系，而不是靠口头约定。

## `pgoutput` 默认行为与 MVP 流处理（审查者结论 · 采纳）

在 **不显式开启 `streaming=on`（流式解码大事务）** 的前提下，主库对**未提交**变更的处理可以粗理解为：**不向你的 CDC 连接发送这些行级变更**；只有事务 **COMMIT** 后，你才会在协议里看到围绕该事务的 **`BEGIN` → DML… → `COMMIT`** 事件序列。**已回滚事务**在默认模式下不会让这类行变更出现在你的解码流里。

因此 **MVP 策略（采纳）**：

- **不必**为「回滚清空缓冲区」维护 `Vec<Event>` 式大事务缓冲；**收到 `Relation` 只更新元数据缓存**，**收到 `INSERT`（等）解完即可 Publish**——能收到即表示（在该默认模式下）对应变更已在主库提交一侧满足你当前协议假设。
- **待办**：若未来打开 **`streaming` 并行/大事务流式**，再单独设计「Stream Start/Stop/Abort + 缓冲或分段提交」；与当前 MVP 路径分离。

**WAL 撑满磁盘（审查者结论 · 采纳）**：连接器**挡不住**主库其他负载狂写 WAL；**主防线在 DBA/运维**：如 `max_slot_wal_keep_size`、磁盘与告警、**Publication 只含控制面小表**（例如仅 `workflow_events`），不把巨型日志表纳进同一复制集。

## 载荷策略（审查者结论 · 采纳）

- **不采用「截断 JSON 假装成功」**：避免下游因丢失路由字段而出现「幽灵卡死」；超大 payload 以 **明确错误 / 可观测形式**处理。
- **推荐实现方向（待代码落地，与当前 `PayloadTooLarge` Err 对齐）**：
  - **默认**：超上限 → **高级别 error 日志**，本批/本条 **不发送** 业务 subject，行为二选一需在配置中写死：**(A) 不推进 LSN**（卡住待人工，零静默丢数）**(B) 记入死信 subject 或跳过发送但推进 LSN**（保流水线不断，**接受该变更在业务通道不可见**，须配套审计与补偿）。
  - 开源版建议在文档中把 **A/B** 标成显式枚举，**禁止**静默截断正文。

## 投递语义（审查者结论 · 采纳）

- **不宣称**端到端 **Exactly-Once**；对外表述统一为 **At-Least-Once**，下游依赖 **业务幂等键**（如 `task_id` / `correlation_id` + `lsn_hex`）压制重复。
- JetStream **Msg-Id** 仅 **有限窗口** 去重，不能当作数学意义上的全局唯一契约。

## 还可先对齐的议题（开工前尽量拍板）

| 议题 | 为什么重要 | 建议默认 |
|------|------------|----------|
| **PostgreSQL 大版本** | `pgoutput` 消息细节、Publication 行为略有差异 | 固定一个主版本（如 16/17）写进验收环境 |
| **权限模型** | 逻辑复制需要 `REPLICATION` 角色属性或等价能力 | 专用复制用户，最小权限 |
| **`wal_level`** | 逻辑复制需 `logical` | 基础设施 checklist，部署脚本显式检查 |
| **槽与磁盘** | 消费者长期落后会导致 WAL 膨胀 | 监控 `pg_replication_slots`、磁盘配额与告警 |
| **初始 LSN** | 新槽从当前快照起 vs 历史回放 | 第一版：新槽 + 接受「上线后的变更」；历史回填另立需求 |
| **幂等键** | at-least-once 下下游必须去重或幂等 | 信封内带 `lsn`（及可选 `xid`），消费方以业务键 + LSN 去重 |
| **JetStream** | stream、subject、 retention、duplicate window | 先用单 stream + 固定 subject 前缀；后续再按表路由 |

## 严谨性与可行性评估

- **技术路径成立**：逻辑复制槽 + `pgoutput` + 复制流 + 向服务端反馈已应用 LSN，是 Postgres 官方支持的机制；用 Rust 实现 **瘦连接器** 完全可行。
- **主要风险在运维与语义，不在「能不能连上」**：槽膨胀、重复投递、版本差异、网络分区时「先 ack 还是先报 LSN」的顺序错误会导致数据信心问题。计划用 **显式阶段与验收** 约束这些点。
- **`tokio-postgres` 边界**：上游不实现 `START_REPLICATION` 流；当前用 **pgwire-replication** 承载复制 wire。这是依赖选择，不是协议错误；后续可替换为自研 CopyBoth 客户端，不影响上层「解析 → 投递 → 推进 LSN」的分层。
- **与 Java 生态（如 Debezium 等）**：不追求「连接器矩阵」第一；优先在 **Postgres → JetStream** 这一条链路上做到 **低延迟、低内存、可推理语义、易部署**。性能与功能对标可作为 **开源后的长期 benchmark 目标**，用相同硬件与近似 workload 压测对比（吞吐、p99、CPU、常驻内存）。

## 目标与非目标

**目标（核心闭环）**

1. 从逻辑复制流中稳定拿到 **INSERT**（及解析 **Relation** 元数据）。
2. 将行数据序列化为 **JSON**（或等价结构化载荷），投递 **JetStream**。
3. 仅在 **投递达到你定义的持久化条件**（默认：pub ack）后，**推进**服务端的 applied / flush 反馈，控制 WAL 保留与重复窗口。

**非目标（首版不做）**

- 多源数据库、整库 schema 演进自动化、exactly-once 端到端、通用可视化工作台。

## 分阶段计划与验收

### 阶段 0 — 环境与契约冻结

- **内容**：固定 PG 版本、复制用户、`wal_level`、测试库、`events`（或选定）表 DDL；Publication / slot 命名约定。
- **验收**：`SELECT` 能验证 publication 与 slot 存在；手工 `INSERT` 一条可见。

### 阶段 1 — 解析闭环（无 NATS）

- **内容**：完善 `Relation` 缓存；`Insert` → 列名/值解码（至少覆盖你事件表的类型）；日志或 stdout 输出结构化结果。
- **验收**：连续 `INSERT` 多条，解析结果与表内数据一致；进程重启后能从槽位续传（观察不丢变更或需接受重复，并记录行为）。
- **本地脚本**：`./scripts/e2e_local.sh`（默认 `USE_NATS=0`）会做 **3 条 INSERT → 停进程 → 再起 → 1 条 INSERT**，并断言日志里出现 **批次 3 条 + 重启后 1 条** 的 `cdc_envelope`。
- **语义（at-least-once 心理预期）**：逻辑复制 + 本实现在 **成功处理一批 WAL 消息后** 更新 applied LSN；若在「已处理、尚未调用 `update_applied_lsn`」之间崩溃，**重启后同一条变更可能再出现**。下游应用 **`correlation_id` + `lsn_hex`（或业务幂等键）** 去重；不要求「每条 INSERT 日志里只出现一次」。

### 阶段 2 — NATS 投递

- **内容**：引入 `async-nats`（或选定客户端），配置 JetStream stream；每条变更一条消息，**信封**含 `lsn`、表/库标识、payload。
- **验收**：消费者能收到消息；断连重连后行为符合 at-least-once 预期。
- **本地脚本**：Docker Compose 已起 `myelin-nats`（`4222` / 监控 `8222`）时执行 **`USE_NATS=1 ./scripts/e2e_local.sh`**（默认 `NATS_URL=nats://127.0.0.1:4222`）。脚本用 **`http://127.0.0.1:8222/jsz`** 读 JetStream **stream 消息数**，断言 **2 条 INSERT 后比基准至少 +2**、**kill 再起 + 1 条 INSERT 后比上一快照至少 +1**（需本机 **`jq` 或 `python3`**）。**说明**：槽位回放时一条 WAL 可能对应多条 JetStream 消息（如多条变更），故 **messages 与 INSERT 条数不必 1:1**，脚本只验单调「至少」增量。
- **手工 pull consumer**：脚本通过时会在尾部打印 `natsio/nats-box` + `nats consumer add/next` 示例；Docker Desktop 上若 `127.0.0.1` 不可达，将 `-s` 改为 **`nats://host.docker.internal:4222`**。

### 阶段 3 — LSN 与 ack 对齐（最关键）

- **内容**：收敛 **「推进槽」** 的触发点，使 **PubAck（或等价条件）与 `update_applied_lsn` 的因果关系**在代码里可读；梳理 **`ReplicationEvent::Commit` 与按 `XLogData` 更新**是否重复、是否会产生边角序；明确 **pub 失败** 时重试/退出策略（与 WAL 膨胀运维文档联动）。
- **实现结论（已写入 `src/pg/stream.rs` 模块注释）**：**保留两处 `update_applied_lsn`** —— `XLogData` 在 PubAck（或日志路径解完）后报 **`wal_end`**；**`Commit`** 单独报 **`end_lsn`**，因 `pgwire-replication` 可把 Commit 作为**无随行 XLogData** 的边界事件发出，且 **`end_lsn` 可大于上一段 `wal_end`**。客户端内部对 applied LSN 取单调最大，不是“双写竞态”。
- **验收（已脚本化，`USE_NATS=1`）**：`./scripts/e2e_local.sh` 在 Phase 2 之后跑 **Phase 3**（`E2E_PHASE3=0` 可跳过）。**3a**：`MYELIN_MAX_PAYLOAD_BYTES=2500` + **`stall`**，插入超大 `claim_meta` → 进程 **非 0 退出**，日志含 **PayloadTooLarge / exceeds max**；**3a-drain**：提高 `MYELIN_MAX_PAYLOAD_BYTES` 后重启，同一条 WAL **重放并成功发出**（日志可见 `p3a-huge` correlation_id，证明未静默跳过槽前变更）。**3b**：`dead_letter` + 小上限，超大行发出 **DLQ 形态日志**，**随后**小行 `p3b-after` 仍在业务信封中出现（管线不断、非静默整体卡死）。**at-least-once**：同 `correlation_id` 多条 `cdc_envelope` 日志可接受。
- **与审查者目标对齐**：默认 **streaming=off** 下 MVP 可保持「解一条发一条」的低位复杂度；**LSN 汇报点**以 `stream.rs` + `publish.rs` 头注释为 runbook 依据。

### 阶段 4 — 迭代增强（按需）

- **背压与观测**：复制滞后、槽 WAL 量、JetStream pending。已实现：**`MYELIN_METRICS_ADDR` → Prometheus 抓取**（计数器/直方图，见 README 表）、**`myelin::replication` 结构化 debug 日志**；槽滞后仍以 PG 侧查询与告警为主。
- **范围扩展**：**`UPDATE` / `DELETE`**（pgoutput 已解码 → 信封 `op`=`update`|`delete`；`update` 在带旧行/键时填 `old_row`）**；**`TRUNCATE` (T)** 仍显式未实现。多表 subject 路由、schema 变更策略等按需再加。
- **韧性**：可选 **mTLS**；**JetStream publish / PubAck** 已实现有限次指数退避（环境变量见 README）；**死信队列** subject；**优雅停机**：SIGINT + Unix **SIGTERM**，在「当前 recv 周期」之后退出（不在半段 `XLogData` 上强行打断）。

### 阶段 5 — 开源与性能（目标）

- **开源（GitHub）**：README（架构图、语义边界、运维清单）、Issue/PR 模板、**`SECURITY.md`**、MIT；CI（`fmt/clippy/test` + `cargo build --benches` + 可选 compose e2e）。
- **高性能叙事**：在文档中写明 **与 Java 连接器对比的维度**（单机吞吐、延迟、内存峰值），用**可复现**的 `INSERT` 基准脚本（如万级/十万级批量）+ 固定 PG/JetStream 版本；不夸大 「exactly-once」。

### 阶段 6 — 压测与断点续传（验收增强）

- **内容**：在 Compose 环境下 **批量 INSERT（如 1 万条）**；中途 **强杀** CDC 进程，再启动，**断言**槽位续传、下游收到条数与幂等策略一致（允许重复，不允许「少掉未声明的一大段」）。
- **验收**：脚本化、可进 CI（时长与资源受限时可降规模）。
- **本地**：默认 **`./scripts/e2e_local.sh`** 在 Phase 1/2 之后跑 **Phase 6**（`E2E_PHASE6=0` 跳过）。**`E2E_BULK_ROWS`** 默认 **2000**（常规门槛）；压测可设 5000+。等待时间随批量单调放宽（见脚本）。JetStream 路径下 Phase 6 会 **`export MYELIN_LOG_ENVELOPE=1`**（仅测试用），以便与 dry-run 相同用日志 grep 校验每条 `correlation_id`；生产勿开。

## 执行原则

- 每阶段结束必须 **可演示、可验收**，再进入下一阶段。
- 不过早抽象：trait 仅保留已出现第二个真实实现需求时再提取。
- 所有「推进 LSN」的路径在代码中 **集中在一处**，便于审计与复盘（与上文「审计 LSN」一致）。
