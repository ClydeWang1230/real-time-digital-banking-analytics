# P0.5 Tableau 看板：使用与验收

这一步将 P0.4 已核对的 SQL 结果制作成 Tableau 工作簿。
看板展示客户从开始开户到完成首笔交易的进展，支持切换历史数据和两个独立模拟批次。
它读取保存的 CSV 快照；Kafka 不需要运行，DuckDB 也不会被打开或写入。

## 交付文件与当前验证状态

已通过本机 Tableau Desktop 验收的工作簿位于：

[Digital Banking Onboarding.twbx](../tableau/validated/Digital%20Banking%20Onboarding.twbx)

`tableau/validated/` 同时保存 `.twb`、`Data/` 下三个 CSV、`manifest.json`、
`verification.json` 和三个批次的原生截图。
`.twbx` 将工作簿和这些 CSV 一起打包；迁移文件时优先使用这一份。
数据格式和打包方式参考 [Tableau 官方文件说明](https://help.tableau.com/current/pro/desktop/en-us/environ_filesandfolders.htm)。

- 已实现：三个数据源、统一批次选择参数、四张 KPI 卡片、漏斗、阶段未推进数量、渠道比较和质量状态。
- 已验证：源文件校验值、数量与比率对账、CSV 序列化、相对文件路径和打包后文件一致性。
- 已在 Tableau Desktop 2026.1 实际打开并切换三个数据集：卡片、漏斗、阶段减少人数、
  渠道比例、批次名称和文字观察与 SQL 快照一致，标题和标签可读。
- 自动化回归：83 项通过，其中 Tableau 专项 9 项；桌面验收记录与测试结果分别保存。

P0.5 已完成本地验收。原来的 `tableau/review/` 保留用于追溯，其 XML 格式属性错误会
导致打开失败，请使用上面的 `validated` 版本。修复了 `font-color`、`mark-size` 和
`source-width` 的错误用法，并调整了卡片换行、行标题宽度和图表空间。
这属于工作簿生成器的问题；不是 Python 天生不能生成 Tableau 文件，也不需要更换数据库。

Python/XML 检查与原生桌面验收仍是不同步骤。未来修改布局后，需要重新打开检查。

## 第一次打开

1. 本机已完成 Tableau 激活；首次在其他电脑使用时，先完成软件启用。
2. 打开上述 `.twbx` 文件，进入 `Onboarding Overview` 仪表板，并最大化窗口。
3. 默认选择 `Simulation A | seed 42`。
4. 使用右上角 `Select dataset` 切换三个数据集，确认所有图表和 KPI 一起变化。

看板为固定大小 1760 × 1160；窗口较小时会出现滚动条。
建议关闭右侧“数据指南”，使用工具栏的演示模式查看完整布局。

工作簿默认仅在本地使用；没有发布到 Tableau Public、Server 或 Cloud。
如果显示数据文件缺失，不要随意替换成另一个批次：先重新打开完整 `.twbx`，
或在 `.twb` 同级保持 `Data/` 目录及其中三个 CSV。

## 看板回答的业务问题

| 区域 | 业务含义 |
|---|---|
| Started sessions | 有效开始开户的会话数，作为漏斗分母 |
| Account opening | 成功开户会话 / 开始会话 |
| First transaction | 完成首笔交易会话 / 开始会话 |
| Not yet completed | 开始会话减去完成首笔交易会话；只描述当前快照 |
| Ordered onboarding funnel | 按时间顺序完成各阶段的会话数量，第 4 阶段要求 KYC 通过 |
| Sessions not progressing | 相邻阶段减少的会话数，和占前一阶段的比例 |
| First-transaction conversion by channel | 各渠道首笔交易会话 / 各渠道开始会话；标签同时展示分子与分母 |
| Observed bottleneck | 本批次绝对人数损失最大的相邻阶段，仅为描述性观察 |
| Data quality | 15 项检查的状态，以及排除会话、孤立会话数量 |

没有走完的会话未必永久流失。渠道表现来自合成数据，不能据此认定某渠道对转化率
产生了因果影响。看板也没有模拟实时刷新或真实银行的处理时延。

## 数据模型：三个粒度，不做物理连接

| 文件 | 一行代表什么 | 数据来源 |
|---|---|---|
| `Data/kpis.csv` | 一个批次 | 报告 04，并带入报告 01 的质量状态 |
| `Data/funnel.csv` | 一个批次的一个阶段 | 报告 03 |
| `Data/channels.csv` | 一个批次的一个渠道 | 报告 05 |

当前审阅数据共 3 批，所以三个 CSV 分别有 3、21、9 行。
每张工作表只查询其对应数据源，统一的 `Batch` 参数在三个数据源中通过
`[dataset_label] = [Parameters].[Batch]` 选择同一批次。
工作表均将该条件筛选为 True；批次参数没有“全部相加”选项。

这样不用把 7 个漏斗阶段和 3 个渠道连接成 21 行再重复累加 KPI。
新增数据集时也必须避免同时提供“全部批次快照”和其中一个重复的单批次快照，
构建脚本会拒绝重复 run_id。

P0.4 的百分比是 **0–100**。看板根据计数重新计算 **0–1** 比率，
再用 Tableau 的百分比格式显示。例如 `37 / 100 = 0.37`，显示为 `37.0%`。
不是先把 37.0 交给百分比格式，再得到错误的 3700%。零分母保留空值，KPI 显示 N/A。

## 桌面验收表

2026-11-09 已分别选择三个批次并核对以下结果。一次切换同时更新顶部 KPI、两个阶段图、
渠道图、数据集名称、run_id 和文字观察。

| 检查项 | 历史基线 | Simulation A / seed 42 | Simulation B / seed 7 |
|---|---:|---:|---:|
| 事件数 | 505 | 508 | 481 |
| 开始会话 | 100 | 100 | 100 |
| 成功开户 | 66 | 63 | 58 |
| 开户转化率 | 66.0% | 63.0% | 58.0% |
| 首笔交易 | 38 | 37 | 30 |
| 首笔交易转化率 | 38.0% | 37.0% | 30.0% |
| 尚未完成 | 62 | 63 | 70 |
| 漏斗人数 | 100,89,84,72,66,56,38 | 100,97,86,71,63,54,37 | 100,94,79,69,58,51,30 |
| 最大人数损失 | 卡激活 → 首笔交易：18 | 卡激活 → 首笔交易：17 | 卡激活 → 首笔交易：21 |
| 对应前一阶段占比 | 32.1% | 31.5% | 41.2% |

原生验收截图：[历史基线](../tableau/validated/screenshots/historical_baseline.png)、
[A 批次](../tableau/validated/screenshots/simulation_a.png)、
[B 批次](../tableau/validated/screenshots/simulation_b.png)。
完整证据见 [verification.json](../tableau/validated/verification.json)。

seed 42 的渠道结果：Branch `13/32 = 40.6%`，Mobile app `16/38 = 42.1%`，
Web `8/30 = 26.7%`。seed 7 分别为 `14/33 = 42.4%`、`5/31 = 16.1%`、
`11/36 = 30.6%`。渠道分子加总应等于顶部首笔交易会话数。

布局验收需确认：所有标题和标签可见、没有红色错误标记、数值没有意外变成百分之几千、
漏斗按 01–07 排序、阶段图不把起点算作一项流失、切换后没有保留前一批次的文字观察。
原始快照的详细质量检查保留在 P0.4 审阅包中；看板展示摘要。

## 重新构建

在项目根目录的 PowerShell 中执行：

```powershell
.\.venv\Scripts\python.exe -B scripts/build_tableau.py
```

默认从 `tableau/review_sources.json` 指定的本地 P0.4 快照构建三个数据集。
预期打印 `Built Tableau package:`、新的 `.twbx` 路径和数据验证 `PASS`。
每次使用新目录，不会覆盖已有看板或源数据。默认来源属于本机保留的验证材料，
并不随 Git 自动提供给另一台机器。

换一台机器或分析新批次时，先使用 P0.4 的验证脚本或分析命令生成快照，
再将其路径传给构建脚本：

```powershell
.\.venv\Scripts\python.exe -B scripts/verify_analytics.py
# 将下一行的路径换成上一条命令输出目录下的 exports/sample。
.\.venv\Scripts\python.exe -B scripts/build_tableau.py --snapshot artifacts/verification/p04-<实际运行目录>/exports/sample
```

`--snapshot` 可以重复，用于不重叠的批次。
如需自定义可读名称和默认批次，参照 `tableau/review_sources.json` 新建配置，
用 `--source-list <配置文件>` 指定。`--output-dir <新目录>` 可指定输出位置。
质量错误、被改动的 CSV、重复批次或无法对账的指标会阻止构建。

运行新测试：

```powershell
.\.venv\Scripts\python.exe -B -m unittest discover -s tests -p test_tableau.py -v
```

预期为 `Ran 9 tests` 和 `OK`。完整回归仍使用 `python -B -m unittest discover -s tests`。

## 建议阅读的两个实现文件

- `scripts/build_tableau.py`：从 `prepare_data()` 开始，重点看分母、粒度和拒绝错误输入的规则。
- `tableau/workbook.py`：从 `make_workbook()` 开始，看每张图使用哪个数据源，以及共享参数如何连到各工作表。

XML 是 Tableau 工作簿的存储格式；面试中更需要解释粒度、分母、批次隔离和验证依据，
不需要背出每个界面标签的 XML 写法。
