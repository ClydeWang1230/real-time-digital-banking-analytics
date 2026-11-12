# P0.4 统一检阅指南

P0.4 已完成。先看结果，再看口径，最后阅读关键 SQL 即可，不必从所有辅助代码的第一行开始。

## 先看这三个结果

以下数据来自 2026-11-08 的只读验证，彼此是不同数据集。

| 数据集 | 事件数 | 开始会话 | 成功开户 | 首笔交易 | 首笔交易转化率 |
|---|---:|---:|---:|---:|---:|
| 历史基线 | 505 | 100 | 66 | 38 | 38.0% |
| P0.3 seed=42 | 508 | 100 | 63 | 37 | 37.0% |
| P0.3 seed=7 | 481 | 100 | 58 | 30 | 30.0% |

所有数据集的错误级质量检查都为零。这里是合成数据结果，不能用来推断真实银行客户的表现。

当前工作区保留了[审阅数据包入口](../artifacts/verification/p04-20261108T115412Z-17d45672/REVIEW.md)、
[分析结果目录](../artifacts/verification/p04-20261108T115412Z-17d45672/exports/)
和[验证记录](../artifacts/verification/p04-20261108T115412Z-17d45672/report.json)。数据包包含：

- `sample`：仓库内固定的 5 客户、20 事件样例。
- `source_1_all`：原来的 505 事件历史库。
- `source_2_all`：P0.3 两个批次，按 run_id 分开报告。
- `source_2_4a10a289-b783-462b-8614-8011bf8a929f`：单独筛选 seed=42 批次。
- `source_2_6fb434c1-139e-4075-9f7a-ea3c13d4552c`：单独筛选 seed=7 批次。

恢复工作时，已重新跑完 74 项测试，并将上述 35 个分析 CSV 与当前只读查询逐值核对。
已有快照完整，因此保留原文件；核对结果见
[续接验证记录](../artifacts/verification/p04-20261108T115412Z-17d45672/continuation_verification.json)。

另外补入了[原始事件明细](../artifacts/verification/p04-20261108T115412Z-17d45672/source_events/)：
历史 505 条、seed=42 的 508 条、seed=7 的 481 条，各自独立一个 CSV。
它们保留数据库中的全部列；JSON 字段仍为 JSON 文本，时间按 UTC 输出。
该目录的清单记录了来源、查询、行数和校验值，便于从具体事件追溯到分析结果。
这三份明细是审阅副本，与每组七个分析报告分开存放；历史数据库和已有导出均未修改。

这些本地数据文件没有提交到 Git。换一台机器可用验证脚本重新生成仓库自带的固定样例
及其分析报告；重现这里的历史结果需要相应源数据库。通用验证脚本生成分析快照，
不会自动生成本次额外补入的三份原始事件副本，其导出查询记录在该目录清单中。

每个目录里优先打开：

1. `04_onboarding_kpis.csv`：先看总体客户旅程结果。
2. `03_onboarding_funnel.csv`：看哪一步没有继续推进。
3. `05_channel_performance.csv`：看不同渠道的描述性差异。
4. `01_data_quality_checks.csv` 和 `06_session_journeys.csv`：核对数据质量与单个会话明细。

## 需要理解的三条口径

1. **按批次、按会话计数。** 相同 session_id 出现在不同批次时，不会合并成一个旅程。
2. **漏斗要求有序完成。** 开户事件必须发生在有效的 KYC 通过之后；时间相同允许，缺少前序阶段则无法继续。KYC 失败后有一次较晚的通过，可以继续。
3. **坏数据与流失分开。** 身份或字段冲突的会话会显示排除原因；合格但未走完的旅程才属于当前快照里未完成的会话。不能把快照里的未完成直接等同于永久流失。

`02_event_counts_by_type.csv` 统计“看到了什么事件”；漏斗统计“完成了什么有效进展”。两个结果在异常数据上可以不同。

## 代码阅读顺序

先读 [03_onboarding_funnel.sql](../sql/03_onboarding_funnel.sql)，理解转化率和前一阶段比较。
然后读 [00_analytics_models.sql](../sql/models/00_analytics_models.sql) 中的
`analytics_journeys`：它逐步寻找不早于前一步的最早事件。`analytics_session_quality`
说明哪些会话被排除，`analytics_events` 负责为旅程计算去除相同的重复记录。

最后按需读 [run_analytics.py](../run_analytics.py)，看参数、只读连接与导出流程。
完整指标定义在 [analytics_contract.md](analytics_contract.md)。测试位于
[test_analytics.py](../tests/test_analytics.py)，共新增 20 个测试场景；整个项目的 74 项测试通过。

## 可选的亲自验证

现在不要求重新操作。想验证时，在项目 PowerShell 终端执行：

```powershell
.\.venv\Scripts\python.exe -B -m unittest discover -s tests -v
.\.venv\Scripts\python.exe -B scripts/verify_analytics.py
```

第一条预期显示 `Ran 74 tests` 和 `OK`。第二条会创建新的测试库及导出目录，预期显示
`PASS` 和报告位置，不需要 Kafka。历史数据不会被改写。

下一阶段 P0.5 将使用这些 CSV 构建 Tableau 看板。当前尚未创建或验证 Tableau 工作簿。
