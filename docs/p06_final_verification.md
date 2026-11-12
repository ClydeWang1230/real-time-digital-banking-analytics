# P0.6 Final Verification Report

验证日期：2026-11-10。定位：**Kafka-based event ingestion with validated persistence and batch SQL analytics**。

结论：本地 MVP 的主要端到端路径与故障恢复验证通过，展示资料已整理。
这不等于生产环境认证，也不等于已验证远程仓库在全新操作系统上一键部署。

## 1. Repository Audit 与测试计划

检查了实际 producer、consumer、validation、storage、配置、SQL 模型/报告、分析与 Tableau 构建器、已有测试脚本及文档。
P0.1–P0.5 的核心功能确实存在，不只是 handoff 中的计划。

| 实际组件 | 职责与核实结果 |
|---|---|
| generator / batch / producer | seed、run_id、base_time 控制模拟；保存 JSONL/manifest；发布与确认计数 |
| consumer / validation | 顺序读取、校验、统计；数据库成功后同步提交 Kafka offset |
| storage | 一次事务保存有效事件或拒绝记录，同时记录 Kafka 消息位置；event_id 与内容摘要区分重复和冲突 |
| DuckDB | 单文件、单写入进程；历史库保留，旧 schema 不会被 consumer 自动改写 |
| SQL / run_analytics | 批次与会话粒度、按事件时间有序漏斗、KYC 通过口径、15 项质量检查、KPI、渠道分析、7 类 CSV |
| Tableau | CSV 快照，参数切换三个数据集；不持续订阅 Kafka，不实时刷新 |

审计发现的交付风险：Compose 没有自动 readiness/topic 初始化及明确的 named-volume 方案；默认历史库不能用于新 consumer 写入；默认 Tableau source-list 引用本机历史验证路径；核心代码仍有未跟踪文件。README 新的独立 demo 使用显式新 topic/group/database/snapshot，避免依赖历史文件。

测试顺序：保留文件哈希 → 原环境测试 → 隔离真实 Kafka 可靠性测试 → 新源码目录/新虚拟环境 → 双批次与直接 CLI 演示 → SQL/CSV 对账 → Tableau 原生打开与切换 → 文件保护与 Git 检查。
复用了现有 unittest、verify_ingestion、verify_simulation、verify_analytics 和 build_tableau，没有新增测试框架或业务模块。

## 2. Passed：实际执行并通过

| 检查 | 实际结果 | 可复核证据 |
|---|---|---|
| 原环境自动化回归 | 83 tests，18.329 秒，OK | [测试日志](evidence/p06-20261110/unit-tests.txt) |
| 新虚拟环境回归 | Python 3.11.9；83 tests，28.862 秒，OK | [新环境日志](evidence/p06-20261110/fresh-unit-tests.txt) |
| 依赖安装 | requirements 中三个固定版本安装成功，pip check 无损坏依赖 | [安装日志](evidence/p06-20261110/fresh-install.txt)、[pip check](evidence/p06-20261110/pip-check.txt) |
| Kafka 异常、重复、重启、重放 | 518 条消息完整处理，508 个唯一事件，3 次重复，7 条拒绝 | [完整 ingestion report](evidence/p06-20261110/ingestion.json) |
| 模拟批次可复现 | A=508，B=481；A 重发不增加 raw_events；最终 989 个唯一事件、200 个会话、1497 个消息位置 | [simulation report](evidence/p06-20261110/simulation.json) |
| SQL 与 CSV | 8 个快照、56 个 CSV；查询与导出一致、批次筛选一致、源文件哈希不变 | [analytics report](evidence/p06-20261110/analytics.json) |
| 新源码目录直接启动 | 5 客户、20 事件，从生成到 Kafka、消费、SQL、CSV、Tableau 全部完成 | [producer](evidence/p06-20261110/quickstart-producer.txt)、[consumer](evidence/p06-20261110/quickstart-consumer.txt)、[SQL](evidence/p06-20261110/quickstart-analytics.txt)、[Tableau 构建](evidence/p06-20261110/quickstart-tableau.txt)、[原生截图](evidence/p06-20261110/fresh_quickstart.png) |
| 原有数据保护 | 检查 120 个原有文件，未丢失；119 个未变，唯一变化是 Tableau 临时恢复 .twbr 文件 | [保护检查](evidence/p06-20261110/preservation.json) |

原数据库、CSV、正式 .twb/.twbx 和历史批次没有被覆盖。原有 Kafka topics 均仍存在；本轮只向新建隔离 topics 写入，不重置原 group 的进度。保留现有 container，未执行删除或重建。Tableau 切换未保存到原工作簿。

### 事件数量、异常与恢复的具体证据

真实 Kafka 可靠性测试发送 508 个合法唯一事件、3 条重复事件及 7 条异常消息：

`518 attempted = 518 queued = 518 acknowledged；failed = pending = 0`

1. **数据库已提交、Kafka 尚未提交时强制退出**：子进程退出码 73；数据库已有 1 条事件与 1 条处理记录；group 无已提交 offset。报告中的 `-1001` 是客户端“没有有效已提交 offset”的哨兵值。
2. **重启恢复**：received 518、inserted 507、duplicate 3、rejected 7、replayed 1、committed 518。加上崩溃前已保存的 1 条，数据库最终 508 条。
3. **完整重放**：新 group 从头读同一 topic，518 条均为 replayed，新增事件 0，数据库数量不变。
4. **写入失败**：注入已关闭数据库连接，收到 1 条但 committed 0；重试后完整处理 518 条。证明写入失败不会提前确认进度。
5. **异常隔离**：event_id_conflict、invalid_event_time、invalid_json、invalid_utf8、missing_fields、null_payload、unknown_event_type 各 1 条进入 rejected_events；Consumer 继续处理后续消息。

区分两种重复：同一 Kafka 位置重读是 `replayed`；同一 event_id 在新的 Kafka 位置再次发送是 `duplicate`。两者都不会重复计入业务事件。
提交失败、内容冲突、非法字段、空数据、乱序、缺失阶段、KYC 失败/重试等边界还由自动化测试覆盖；这不代表所有现实基础设施故障都已注入。

### Dashboard 核对

在 **Tableau Desktop 2026.1** 实际打开新生成的 demo 包和 `tableau/validated/Digital Banking Onboarding.twbx`，逐个切换并检查标题、事件数、KPI、漏斗、渠道、流失与质量状态。

| 数据集 | 事件 | Started | 有序七阶段会话数 | 开户率 | 首笔交易率 | 未完成 |
|---|---:|---:|---|---:|---:|---:|
| Historical Baseline | 505 | 100 | 100,89,84,72,66,56,38 | 66% | 38% | 62 |
| Simulation A / seed 42 | 508 | 100 | 100,97,86,71,63,54,37 | 63% | 37% | 63 |
| Simulation B / seed 7 | 481 | 100 | 100,94,79,69,58,51,30 | 58% | 30% | 70 |
| 新环境 demo / seed 42 / 5 customers | 20 | 5 | 5,4,4,2,2,2,1 | 40% | 20% | 4 |

三个保留批次的渠道首笔交易率（Branch / Mobile app / Web）：Historical 8/25、11/43、19/32；A 13/32、16/38、8/30；B 14/33、5/31、11/36。均与对应 SQL 输出一致。
所有四个视图均显示 15 项质量检查 PASS、excluded/orphans 为 0。

截图：[Historical](evidence/p06-20261110/historical_baseline.png)、[A](evidence/p06-20261110/simulation_a.png)、[B](evidence/p06-20261110/simulation_b.png)、[新 demo](evidence/p06-20261110/fresh_quickstart.png)。

口径：开户/首笔交易转换率均以 started sessions 为分母；每步流失率以上一步为分母；第 4 阶段要求 KYC passed；未完成只表示当前快照中尚未完成，不推断永久流失。渠道差异是描述性结果，不是因果结论。

## 3. Failed：实际执行但失败

本轮已完成的功能验收没有未解决的失败项。故意触发的进程退出和数据库异常属于恢复测试的预期结果，不计为验收失败。
执行中最初的 Docker 权限限制及 broker 尚未就绪的连接警告已解决，随后测试成功。未通过的操作不会被用作“通过”的证据。

## 4. Not Tested：尚未验证

- 全新操作系统安装 Python、Docker、Tableau、首次拉取镜像与离线安装。
- 远程 Git 仓库 fresh clone：目前 working tree 有未提交/未跟踪的核心文件。
- broker 丢盘、整机断电、真实磁盘写满、长时间网络分区、备份恢复；本轮数据库失败使用受控连接关闭注入。
- 多 broker 高可用、多 consumer 分区再均衡、大规模吞吐与长时间稳定性。
- Tableau 其他版本，以及 Public/Server/Cloud 发布。

## 5. Known Limitations

- 单机单 broker，无副本冗余；Compose 未提供明确的 durable named-volume/recovery 配置，不承诺容器重建后保留 Kafka 数据。
- Kafka 与 DuckDB 不是跨系统原子事务。先数据库 commit 后 Kafka commit 的间隙靠保留消息和幂等写入恢复，不能称为端到端 exactly-once。
- 新数据库不能盲目搭配已消费完的旧 group；恢复时必须保持正确的 topic/group/database 组合。消息超过 Kafka retention 或数据库丢失需要独立恢复策略。
- DuckDB 采用单写入进程；先结束消费，再启动分析。Tableau 是导出快照，需要重新分析/导出/构建以更新数据。
- 合成数据规模小，没有生产部署、真实性能提升或真实银行客户行为结论。
- 工作簿生成使用 Tableau XML，自动化检查不能替代目标 Desktop 版本的原生打开验证。

非关键 P1 建议仅记录：readiness 自动化、broker 持久化/恢复说明、跨平台安装检查。没有在本阶段扩展功能。

## 6. 复核命令与证据位置

从仓库根目录运行；真实 Kafka 测试要求 broker 已就绪，每次生成新 topic/group/database/artifacts，保留旧数据：

```powershell
.\.venv\Scripts\python.exe -B -m unittest discover -s tests -v
.\.venv\Scripts\python.exe -m pip check
.\.venv\Scripts\python.exe -B scripts/verify_ingestion.py --customers 100
.\.venv\Scripts\python.exe -B scripts/verify_simulation.py
.\.venv\Scripts\python.exe -B scripts/verify_analytics.py
```

最后一条在干净源码目录也能运行，使用已提供的 examples fixture。复核本机保留批次时使用：

```powershell
.\.venv\Scripts\python.exe -B scripts/verify_analytics.py --db-path banking_events_analytics.duckdb --db-path artifacts/verification/p03-20261108T110003Z-759b7c17/simulation.duckdb --db-path artifacts/verification/p06-20261110T031536Z/clean_workspace/artifacts/verification/p03-20261110T032022Z-8cbc607f/simulation.duckdb
```

上述历史路径只属于本机证据，不是 fresh checkout 的前提。完整直接启动命令见 [README Quick Start](../README.md#quick-start-new-data-kafka-to-tableau)。

本轮原始报告：

- `artifacts/verification/p02-20261110T031832Z-a45976a7/report.json`
- `artifacts/verification/p06-20261110T031536Z/clean_workspace/artifacts/verification/p03-20261110T032022Z-8cbc607f/report.json`
- `artifacts/verification/p04-20261110T032520Z-ab1434f5/report.json`
- `artifacts/verification/p06-20261110T031536Z/`：原日志、干净源码目录、虚拟环境、直接演示与保护检查。

可提交的小型证据副本放在 `docs/evidence/p06-20261110/`；绝对本机根路径替换为 `<repo>`，原报告没有修改。[provenance.json](evidence/p06-20261110/provenance.json) 记录原报告路径与 SHA256。数据库与完整临时环境继续由 .gitignore 排除。
文件保护的初始哈希清单见 [preservation_before.json](evidence/p06-20261110/preservation_before.json)。文档本地链接检查与 `git diff --check` 通过；Git 的 LF/CRLF 提示不是测试失败。

## 7. 面试现场能否从零完整复现？

**可以有条件地回答“可以”：完整源码、Python 3.11、已启动的 Docker 和已激活的 Tableau 就绪后，按 README 独立 demo 可以完成。** 本轮已在新的源码目录和全新 venv 中实际执行，20 条事件一路对账到原生 Tableau 看板。使用现有本机 Kafka container，命令用原 Compose 文件定位该 broker，避免第二个实例争用 9092。

**不能声称已经在完全干净的电脑或远程 fresh clone 上验证过。** 当前源码导出来自 working tree，不是已发布的 Git commit。现场演示应提前安装前置软件、确认镜像可用与 Tableau 激活；演示时创建新 topic/group/database，避免误用旧数据。历史 505 行不是仅凭新随机模拟保证可重建的数据，保留的三批次 workbook 用于展示该基线。

发布前检查清单：

1. 查看 `git status --short`、`git diff` 和 `git diff --cached`；已有 staged 与 working 文件内容不同，不能只提交旧 staging 内容。
2. 将完整源码（config、producer、consumer、sql、scripts、tests、examples、Tableau builder）、requirements/Compose、README/docs 与正式 validated 演示包纳入同一经过审阅的提交。
3. 排除 .venv、artifacts、大型数据库、编辑器设置、Tableau `~*.twbr` 临时恢复文件和已知无效 review 草稿。不要用未经审查的 `git add .`。
4. 在新的 clone 中执行 README Quick Start 与自动化测试，记录 commit ID；这一步完成后再声称 remote-clone reproducibility 已验证。

本阶段没有替用户 stage、commit、push 或删除任何历史资料。

## 8. 本阶段变更与复杂性

仅更新 README、PROJECT_STATUS、忽略临时恢复文件规则，新增本报告、展示说明、证据副本与截图；没有改动业务 Python、SQL、配置、依赖、数据库 schema 或工作簿生成逻辑。
不可避免的重点是：两个系统提交无法原子化，所以需要幂等和消息位置记录；漏斗要按 run/session 与阶段时间顺序计算；Tableau 快照需要独立原生检查。这些复杂性都有实际验证需求，没有引入通用平台抽象。
