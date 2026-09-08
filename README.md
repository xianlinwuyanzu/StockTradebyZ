# Z哥战法的 Python 实现（更新版）

> **更新时间：2025-12-26** –
>
> 新增 **BigBullishVolumeSelector（暴力K战法）**：用于捕捉放量启动、贴近短线均值的强势阳线；

---

## 目录

* [项目简介](#项目简介)
* [快速上手](#快速上手)

  * [环境与依赖](#环境与依赖)
  * [准备 Tushare Token](#准备-tushare-token)
  * [准备 stocklist.csv](#准备-stocklistcsv)
  * [下载历史 K 线（qfq，日线）](#下载历史-k-线qfq日线)
  * [运行选股](#运行选股)
* [云端部署（仅上传代码）](#云端部署仅上传代码)
* [参数说明](#参数说明)

  * [数据下载参数](#数据下载参数)
  * [`select_stock.py`](#select_stockpy)
* [统一当日过滤 & 知行约束](#统一当日过滤--知行约束)
* [内置策略（Selector）](#内置策略selector)

  * [1. BBIKDJSelector（少妇战法）](#1-bbikdjselector少妇战法)
  * [2. SuperB1Selector（SuperB1战法）](#2-superb1selectorsuperb1战法)
  * [3. BBIShortLongSelector（补票战法）](#3-bbishortlongselector补票战法)
  * [4. PeakKDJSelector（填坑战法）](#4-peakkdjselector填坑战法)
  * [5. MA60CrossVolumeWaveSelector（上穿60放量战法）](#5-ma60crossvolumewaveselector上穿60放量战法)
  * [6. BigBullishVolumeSelector（暴力K战法）](#6-bigbullishvolumeselector暴力k战法)

* [项目结构](#项目结构)
* [常见问题](#常见问题)
* [免责声明](#免责声明)

---

## 项目简介

| 名称                    | 功能简介                                                                                                                                                                               |
| --------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`data_fetch/fetch_kline.py`**  | 仅使用 **Tushare** 抓取 **A 股日线（前复权 qfq）**。**股票池从 `stocklist.csv` 读取**，支持排除 **创业板/科创板/北交所**，并发抓取，**每次运行全量覆盖保存**（不做增量合并），输出 CSV 列：`date, open, close, high, low, volume`。 |
| **`select_stock.py`** | 加载 `./data` 目录内 CSV 行情与 `configs.json`，批量执行选择器（Selector）并输出结果到控制台与 `select_results.log`。                                                                                           |
| **`strategies/Selector.py`**     | 实现各类战法（选择器），由根目录选股入口动态加载；配置中的类名保持不变。 |

---

## 快速上手

### 环境与依赖

```bash
# Python 3.12 推荐（3.11 也可）
python3 -m venv .venv
source .venv/bin/activate

# 进入你的项目目录
cd /path/to/your/project

# 安装依赖（全部环境依赖统一在 requirements.txt）
python -m pip install --disable-pip-version-check --only-binary=:all: -r requirements.txt
```

> 关键依赖：`pandas`, `tqdm`, `tushare`, `numpy`, `scipy`, `yfinance`, `matplotlib`, `requests`, `lxml`。

### 准备 Tushare Token

1. 在系统环境中写入 `TUSHARE_TOKEN`：

```bash
# Windows (PowerShell)
setx TUSHARE_TOKEN "你的token"

# macOS / Linux (bash)
export TUSHARE_TOKEN=你的token
```

### 下载历史 K 线（qfq，日线）

```bash
python -m data_fetch.fetch_kline
```

* **数据源固定**：Tushare 日线，**前复权 qfq**。
* **保存策略**：每只股票**全量覆盖写入** `./data/XXXXXX.csv`。
* **并发抓取**：默认 6 线程；支持封禁冷却（命中「访问频繁/429/403…」将睡眠约 600s 并重试，最多 3 次）。

### 运行选股

```bash
python select_stock.py \
  --data-dir ./data \
  --config ./configs.json \
  --date 2025-09-10
  python select_stock.py --data-dir ./data/us_stocks     --config ./configs.json
```

默认配置同时启用“二波选股策略”。该策略使用最新可用日线判断一组连续上升波是否仍未结束：已经完成的上涨-回调波必须满足结构条件，最后一波可以继续运行；如果最新价格跌破当前起点、超过回撤上限或当前浪超出周期边界，则认为这组波浪结束。评分线默认是 `1.0`，结果写入：

```text
wave_selection_data/wave_selection_results.json
wave_selection_data/wave_selection_results.csv
wave_selection_data/daily/<CODE>.csv
```

其中每个入选股票的 `daily/<CODE>.csv` 保存最近 `70` 个交易日日线，并包含 `K`、`D`、`J` 三个 KDJ 指标字段；JSON 还内嵌同一份日线数据，前端可以选择读取索引文件或直接读取单票 CSV。

二波选股策略的 `structure_quality` 在页面显示为“模型匹配度”，只评价结构本身，不评价当前处于第几波；旧字段 `score` 继续保留作为兼容字段，数值与 `structure_quality` 相同。接口另写出 `score_raw`、`structure_match_raw_max` 和 `structure_match_percent`：百分比按 `score_raw / structure_match_raw_max × 100` 计算，当前理论原始满分为 `9.6`（含最高 1.0 分阳线缺口项），并限制在 `0%` 至 `100%`。它是固定模型口径，不按当日候选集合动态归一化，也不表示收益概率、收益率或胜率。一波仍使用原有评分口径。二波模型匹配度门槛内部仍默认为 `1.0`，报告同时提供对应的 `structure_match_threshold_percent`，页面按百分比显示；基础结构分为 `1.0`，其余分项根据实际数据在区间内连续变化；每条结果同时写出 `score_components` 和 `score_reasons`，便于前端和人工复核。

| 评分分项 | 最高分 | 计算依据 |
| --- | ---: | --- |
| `structure_base` | 1.00 | 至少完成一组上涨-回调波，并通过硬条件 |
| `low_structure_quality` | 1.00 | 低点相对前一低点的质量；允许小幅破低，但破低幅度越大分数越低 |
| `up_path_efficiency` | 1.50 | 上涨腿路径效率为主要部分，并保留完整波路径效率参考；越接近单向运行越高 |
| `up_direction_consistency` | 1.50 | 只看上涨段中上涨方向的比例，不要求每日步长均匀 |
| `pullback_path_efficiency` | 0.60 | 回撤段路径效率的低权重辅助项；越接近单向回撤越高 |
| `low_position` | 1.00 | `1起` 在起点前 120 个交易日价格区间中的相对低位，越靠近区间低点越高 |
| `start_price_quality` | 1.00 | `1起` 相对起点前高点的回落幅度，默认按回落 40% 达到满分 |
| `weekly_j_reset` | 1.00 | `1起` 前最近完整周的 J；优先奖励周线 J 曾高于 60 后回落到 10 以下 |
| `bullish_gap` | 1.00 | 上涨段阳线实体缺口、完整缺口及连续缺口，按下文公式封顶加分 |

`structure_base` 仍为固定的 `1.0`，表示结构硬条件通过，但它不携带当前波浪位置信息。上表分项是原始分，原始总分理论上最高约 `9.6`；展示用 `structure_quality` 仍保留历史标尺 `5.114` 映射为 `10.0` 分，不因新增缺口项重新校准，因此优秀结构可能超过 `10`。`current_wave_number`、已完成波数量、当前周期长度和最新日 J 不再直接给分，只作为结果说明或其他结构指标的输入。该分数在页面显示为模型匹配度，不是收益率、胜率或收益概率。

当前择时机会单独使用 `timing_score` 表示，页面显示为“择时”，范围通常为 `0` 至 `13`。二波只在最新价格已经从当前 `2顶` 回落后计算：先沿回落区间寻找满足动态 J 阈值（默认 5，有实体缺口为 10，有完整缺口为 20）的参考点；参考点形成前，J 向该阈值回落得越充分，择时越高，形成参考点后，以参考点之后的最低 J 为基准，当前 J 回升越多，择时越低。一波沿用已经通过硬条件确认的 `2起` 参考点，直接计算参考点之后的最低 J 与当前 J 的回升量，当前 J 越低且回升越少，择时越高。两种策略在当前 J 不高于 `10` 且当日实体占振幅不超过 `20%` 时，均可获得 `3` 分低 J 十字星加分。择时是当前阶段的技术状态分，不代表收益概率或确定买点；择时分本身不参与结构硬筛选，但一波参考点是否存在仍是硬条件。

二波结果按 `timing_score`（择时）从高到低排序，再按 `structure_quality`（模型匹配度）从高到低排序；一波入场策略页面也将结构评分显示为模型匹配度。

当前交易位置仍单独计算并输出 `structure_position_priority`（`0` 至 `100`），优先级越高越接近低位机会区；它综合当前浪价格进度、峰值回撤、当前 J 与前几波完整区间最低 J 的接近程度，以及 J 的下行状态。该字段目前作为历史报告和诊断字段兼容保留，不再作为二波页面的展示指标或排序条件；二波页面主排序为 `timing_score`（择时）、`structure_quality`（模型匹配度），再以股票代码稳定排序。`score > 10` 仅触发卡片的慢速闪烁提醒。

评分计算口径：

```text
up_path_efficiency
  = 1.5 × (
      0.25 × scale(平均完整波效率)
      + 0.75 × scale(平均上涨腿效率，包含当前上涨腿)
    )

up_direction_consistency
  = 1.5 × 平均(各上涨段方向一致性)
  方向一致性 = 上涨段中收盘价未下降的交易日比例；不使用每日步长规整度

pullback_path_efficiency
  = 0.6 × scale(平均回撤段路径效率)
  回撤段路径效率 = 回撤净变化 / 回撤期间每日绝对变化之和

low_structure_quality
  = min(已完成低点质量, 当前未完成浪低点质量)
  理想低点 >= 2% 抬高时为 1；
  统一允许低点最多破低 5%；
  在容忍范围内不淘汰，但下破越深分数越低；
  已确认低点或当前浪下破超过 5% 时，结束该波组

low_position
  = 1 - (1起价格 - 起点前区间最低价)
        / (起点前区间最高价 - 起点前区间最低价)

start_price_quality
  = min(1, (起点前最高价 - 1起价格) / 起点前最高价 / 40%)

weekly_j_reset
  = 0.45 × 周线 J 低位分
  + 0.30 × 周线 J 前高分
  + 0.25 × 周线 J 回落幅度分
```

周线 J 使用 `W-FRI` 聚合的完整周线，取 `1起` 之前最近一根已完成周线，向前看最多 26 周；`周线 J <= 10` 且此前峰值 `>= 60` 时记录为 `weekly_j_reset=true`。这个条件目前是软评分项，不是硬过滤。

一波入场策略要求第一上涨段通过涨幅、周期、路径效率和上涨不利波动等硬条件，随后在规定观察窗口内出现满足动态 J 阈值的参考点（无缺口仍为严格 `J < 5`）；最新收盘不能跌破 `1起`，回撤不能超过 `28%`，并且不能已经形成有效二波。通过硬条件后，`score_threshold` 默认仍为 `1.0`，评分只用于候选排序，不代表收益概率：

| 评分分项 | 最高分 | 计算依据 |
| --- | ---: | --- |
| `structure_base` | 2.00 | 1起、1顶、动态 J 阈值参考点及其他硬条件均通过 |
| `wave1_return` | 1.50 | 第一波涨幅从 6% 到 40% 线性计分 |
| `wave1_path_efficiency` | 2.00 | 第一波路径效率从 0.58 到 1.00 线性计分 |
| `wave1_smoothness` | 1.00 | 上涨方向一致性和日波动规整度 |
| `start1_support` | 1.50 | 回调期间最低收盘与 1起的距离 |
| `j2_oversold` | 1.50 | 参考 J 为负时按负值深度计分，J <= -30 达到满分；仅满足 `J < 5` 不会自动获得该项加分 |
| `rebound` | 1.00 | 参考点后回升幅度从 0% 到 12% 线性计分 |
| `signal_freshness` | 1.00 | 参考点后剩余观察期比例 |
| `start_price_drawdown` | 1.50 | 1起相对前高回落程度，默认回落 40% 达到满分 |
| `pullback_path_efficiency` | 0.00 | 回撤效率不足时最多扣 5 分 |
| `bullish_gap` | 1.00 | 第一上涨段内的阳线缺口，按下文公式封顶加分 |

一波结构评分理论最高为 `14.0` 分。硬条件决定是否入选，评分决定同一批候选的先后顺序；其中动态 J 阈值用于一波准入，而 `j2_oversold` 是独立的软加分项，放宽参考点不自动获得超卖加分。魔抓采用独立的二波评分口径，见下文。

一波在硬条件确认动态 J 阈值的 `2起` 参考点后，单独计算 `timing_score`（页面显示为“择时”）：取参考点到最新日之间的最低 J，定义 `j_rebound = max(0, 当前 J - 参考区间最低 J)`，并按 `10 × (1 - clip(j_rebound / 30, 0, 1))` 计分。因此当前 J 越低、较区间低点回升越少，择时越高；低 J 日线十字星可额外加 `3` 分。择时不并入上述结构评分，不改变 `score_threshold` 或硬性候选集合，也不表示收益概率或确定买点。一波结果与二波一样按择时降序、结构分降序、股票代码稳定排序。

### 魔抓策略

`MoZhuaSelector` 是以二波结构为基础的独立策略，配置别名“魔抓策略”，默认 `activate=true`，输出 `selection_branch=mozhua_two_wave`。必须已经有两段合格上涨及第二顶部，只在第二顶后的3起参考阶段入选，不在第一顶后的2起阶段入选。普通一波、二波的缺口评分及 J<5/10/20 保持不变。魔抓允许与普通二波同时入选，不再执行“排除已有二波”。

魔抓配置位于 `configs.json` 的独立条目，关闭该条目的 `activate` 即可停用，不影响一波。输出目录为 `mozhua_selection_data/`，包含 `wave_selection_results.json`、当前结果 CSV、`wave_selection_history.csv`、`mozhua_selection_history.json` 和 `daily/` 日线，主选股日志与策略交集仪表盘显示“魔抓策略”。

魔抓结果分为两部分：`results` 只保留本次交易日仍存在 3 起参考点的候选；`historical_results` 保留最近 10 个前置交易日曾入选的记录，按“股票代码 + 选中交易日”去重。历史记录中的结构、参考点和评分字段保持当日选中时的快照，但 `recent_daily_data`、`daily_data_end` 及对应 `daily/<CODE>.csv` 会在每次选股运行时用最新行情重新生成。首次启用时会用可用行情回填最近 10 个前置交易日；状态文件只保存选择元数据，不保存过期日线。发布脚本会将 `historical_results`、`historical_result_count`、`history_window_days` 和 `historical_trade_dates` 一并发送到魔抓接口。

- 第一上涨段单独决定2起 J 阈值；第二上涨段单独决定3起 J 阈值。无缺口 J<5，向上阳线实体缺口 J<40，完整缺口 J<50，连续强势推进 J<70；参数为 `mozhua_body_j_limit`、`mozhua_full_j_limit`、`mozhua_strong_j_limit`。这些是魔抓研究初始阈值，不是普通策略默认值。
- 魔抓的方向性缺口要求当天阳线，且开盘高于前一根实体上沿 `max(open, close)`，前日允许阴线；完整缺口额外要求当天最低价高于前日最高价。普通策略的双阳线缺口统计与评分定义不改。两种统计同时输出，可审计其差别。
- 强势推进仍要求顶部之前 3 至 6 个收盘变化间隔、全阳线、涨幅至少15%、效率至少0.90、至少两组双阳线实体缺口且有连续衔接。不要求两段都强势，不把第一段 J<70 借给第二段。
- 参考点须在对应顶部末端后第2至10根，同时位于对应起点后7至25根；收盘回撤5%至15%、回吐该上涨段涨幅不超过50%、不跌破对应起点，J 从对应顶部区间峰值下降至少30点。收盘采用一致价格口径。
- 首个合格点成为候选；后续仍靠近回调最低收盘（上方2%内）且J继续下降，可以更新参考点。首次反弹远离低位或J停止下降后不再后移。2起的搜索只使用2顶之前的数据；3起只使用选股日及以前的数据。
- 魔抓仍通过二波涨幅、周期、顶部抬高、回调、路径效率及不利波动等硬条件；不能单凭缺口与 J 放宽绕过二波结构。只有当天首次形成或继续更新3起参考点时才入选，要求3起参考点为输入历史的最后一根日线；输出 `reference_stage=3`、`reference_status=candidate`，不表示第三波已确认。
- 停止更新参考点后，不再输出该结构的后续观察日，不等待反弹15%或突破2顶才退出。同一轮搜索遇到J停止下降或价格远离低位后终止，后续J再次下降也不恢复该轮参考点更新；支撑、历史回撤15%及回吐50%的约束仍保留。IT仅在08-17、08-18入选，08-19不再入选；GEN在08-17至08-19更新参考点，08-20起不再入选。逐日扫描结果应满足 `scan_date == j3_date`。
- 结构评分复用二波评分，额外按两段强势推进质量的平均值乘 `impulse_quality_weight=1.0` 加分；原始满分由9.6变为10.6，展示缩放保留原标尺。择时改为3起参考点之后的J回升程度，低J十字星规则不变，不能把高J参考点称作绝对超卖。

IT 历史验证：结构1起仍为07-22、1顶07-29；第一段连续强势缺口支持2起08-03（J61.29<70）。2顶08-10；08-04虽前日为阴线，但当天阳线开盘越过前日实体上沿，因此第二段有一个方向性实体缺口，支持3起08-17（J34.11<40、J下降64.49、回撤7.24%、回吐33.57%）。08-03和08-14均不输出魔抓，08-17收盘后入选。此前 `analysis/runs/mozhua_it_20260803/` 的一波式魔抓验证为废弃口径，不代表当前策略；新验证输出位于 `analysis/runs/mozhua_it_20260817/`。全量胜率尚需回测。

### 阳线缺口与参考点放宽

两根相邻 K 线必须都满足 `close > open`。实体缺口要求 `open[t] > close[t-1]`；完整缺口额外要求 `low[t] > high[t-1]`。等于边界不算缺口，不要求最低缺口幅度或成交量。完整缺口包含在实体缺口组数中，仅增加强度奖励，不重复计组。

`bullish_gap.quality = min(1, 0.25 * body_count + 0.10 * full_count + 0.15 * consecutive_count)`。
其中连续衔接指同一上涨段中连续两个缺口相连，例如三根连续跳空阳线为两组缺口、一次衔接。独立组数和连续组数增加时加分递增，达到上限后不再增加。一组实体缺口质量为 0.25，一组完整缺口为 0.35，两组独立完整缺口为 0.70，两组连续完整缺口为 0.85。

两套策略均配置 `bullish_gap_weight=1.0`，原始缺口加分为该权重乘质量。一波直接加入结构分，二波沿用 `10 / 5.114` 展示缩放，原始满分分母同步增加。评分输出包含 `bullish_gap` 统计、`score_components` 加分和 `reference_j_limit` 实际参考阈值。一波只统计 `1起 -> 1顶`；二波统计已确认上涨段及当前起点到已观察最高收盘的上涨段，不计算回撤区间、不跨上涨段计算连续性。

参考点只依据对应上涨段的缺口放宽：一波看 `1起 -> 1顶`，二波看当前配对的起点到顶部，不借用其他波或后续 K 线。无缺口仍为 `J<5`；有实体缺口为 `J<10`；有完整缺口优先用 `J<20`。参数分别为 `body_gap_reference_j_limit` 和 `full_gap_reference_j_limit`，不会把已有更宽的基础 J 阈值收紧。

两套策略均从顶部区间结束后至少 2 根 K 线开始找，参考点相对该波起点需在第 7 至 25 根 K 线（索引差、含边界）内，选择首个合格点；参考收盘不得高于顶部、不得低于该波起点收盘，距顶部回撤不得超过 28%。一波的参考点是准入条件，二波的参考点用于择时，并非二波结构入选的新增硬条件。一波仍保留参考后最多观察 25 根、反弹达到 15% 等退出规则；缺口不会放宽周期、支撑或回撤约束。

顶部在内部表示为 `PivotZone`：原始收盘价局部峰值是单日点（`start=end`）；相近的高点及其间浅回落满足平台合并条件时，才合并为 `start` 到 `end` 的顶部区间。默认两顶收盘价差不超过 1%、中间低点相对较高顶回落不超过 6%、合并跨度不超过 15 根 K 线。顶价取其中较高的峰值收盘价，不一定出现在区间末日。参考点搜索下限是 `top.end + 2`：若末日为周一且无休市，周二不选，周三开始允许；不是从区间内最高价日计数，也不是等两天结束后从第三天才开始选。起点后的 7 至 25 根窗口仍须同时满足。

既有 `analysis/code/backtest_two_wave_target.py` 使用独立的顶后首个 `J<0` 目标价实验口径，本次未修改该实验或重跑历史结果。新评分与新参考点的效果需要重新回测，不能沿用旧胜率。

连续波浪不会限制为最多三波。前两波的顶部抬高仍要求至少 `2%`；从第 3 波开始，后一顶部不再要求明显创新高，只要不比前一顶部低超过 `1%`，且其他条件仍满足，就继续保留在同一组波浪中。只有明显低于前顶，或周期、底部抬高、回调、路径效率等条件失败时，才结束当前波浪组并从后续起点重新寻找新组。
当前正在运行的波浪期间，允许价格最多下破该浪起点 `5%`，但最新收盘必须重新站回起点上方；在容忍范围内仍保留该波组，同时使用最弱低点质量压低评分。因此 CLF 这类曾破低但尚未超过容忍区间的股票可以继续入选，但不会再获得无破低结构的满额低点分。

四个样例 HOOD、PONY、NOK、AJG 的复算和是否满足“高位回落后低位启动”特征，见 [`wave_sample_quality_analysis_20260822.md`](wave_sample_quality_analysis_20260822.md)。

> `--date` 可省略，默认取数据中的最后交易日。

### 美股数据源切换（yfinance / QuantDash / Yahoo Chart API）

`us_daily.sh` 通过 `DATA_SOURCE` 选择美股日线数据源，当前默认值为 `quantdash`。例如切换到 QuantDash：

```bash
cd /opt/sf
export DATA_SOURCE=quantdash
export QUANTDASH_API_KEY=你的真实key
bash us_daily.sh
```

每日流程支持 `PY_BIN` 指定解释器（默认 `./.venv/bin/python`），不再覆盖显式设置；只检查当前数据源和选股所需的依赖。`DATA_DIR` 默认 `./data/us_stocks`，拉取的 `--out` 与选股的 `--data-dir` 使用同一个值；`CONFIG_FILE` 默认 `./configs.json`，`TRADE_DATE` 可选，仅指定选股截止日，不改变拉取范围。QuantDash 的请求范围可用 `QD_DAYS=900`、`QD_COUNT=500` 调整，默认值保持不变。配置文件不存在时在下载前退出。

```bash
PY_BIN=/path/to/python DATA_DIR=./data/us_stocks CONFIG_FILE=./configs.json bash us_daily.sh
```

本地日更和 `deploy/run_daily_cloud.sh` 都在拉取、选股或推送命令返回非零时停止并返回失败；云端入口仍使用 Tushare，默认 `DATA_DIR=./data`，并同样支持 `PY_BIN`。设置 `STOCK_TRACKING_PUSH_ENABLED=0` 可跳过推送。选股导出在同一次运行内共享各股票的完整历史 KDJ、BBD、买点计算结果，再按各策略的展示天数截取；不跨运行缓存，不改变指标预热及历史截止日。

#### 日更性能与计时

两套日更入口在标准输出记录 `[timing]`：拉取、选股、推送及总运行的秒数和退出码。选股还记录CSV加载、各策略、各结构化输出、交集图、仪表盘及总耗时，默认写入 `logs/selection-timing-时间戳-进程号.json`；通过 `SELECT_TIMING_OUTPUT` 指定其他路径。直接调用选股时用 `--timing-output` 开启JSON报告，阶段日志始终保留。JSON报告仅在选股正常结束时写出，失败状态看Shell阶段日志。

KDJ、BBD仍用完整截止日历史计算；逐日买点只计算各策略实际展示窗口的并集，每个展示日的判断仍使用完整历史前缀。窗口前一日的黄金坑状态单独计算，保证窗口边界不会多出重复入场标记。不截短波段识别输入，不改策略参数和排序。

可选开关（默认均为1）：`SELECTION_VISUALIZATION_ENABLED=0` 跳过交集PNG及交集CSV，`SELECTION_DASHBOARD_ENABLED=0` 跳过HTML仪表盘；结构化策略JSON和日线CSV仍输出。关闭开关不会删除以前生成的图表，使用方需避免误读旧文件。

```bash
SELECTION_VISUALIZATION_ENABLED=0 bash us_daily.sh
python select_stock.py --timing-output ./logs/selection-timing.json
```

2026-09-08本地固定16股、截止2026-08-18、全部启用策略及图表的一次前后对照：CLI耗时45.612秒降到19.570秒（约2.33倍，减少57.1%）；结构化输出约31.015秒降到4.345秒。行情及配置相同，31个JSON/CSV/PNG/HTML产物哈希全部一致。该结果不含联网下载和推送，不代表全市场日更的固定加速比例。可用 `analysis/code/benchmark_daily_selection.py --label 新目录名` 复测，结果保存在 `analysis/runs/daily_performance/`，已有基准目录不会覆盖。

切换到直接 Yahoo Finance Chart API：

```bash
cd /opt/sf
export DATA_SOURCE=yahoo_chart
bash us_daily.sh
```

### SPX 指数单独拉取（FRED）

`us_daily.sh` 现在支持在个股数据源之外，单独拉取一份 SPX 指数文件（默认 FRED `SP500`）。

默认开启，输出到 `./data/indices/SPX_FRED.csv`。若 FRED 暂时不可用，默认只告警并继续后续选股流程。

```bash
export FRED_SPX_ENABLED=1
export FRED_SPX_SERIES_ID=SP500
export FRED_SPX_OUTPUT=./data/indices/SPX_FRED.csv
export FRED_SPX_TIMEOUT=20
export FRED_SPX_STRICT=0
bash us_daily.sh
```

说明：

* `FRED_SPX_STRICT=0`：FRED 拉取失败时不阻断日更（默认）。
* `FRED_SPX_STRICT=1`：FRED 拉取失败时整次日更失败退出。
* 该文件是指数序列的独立输出，不替代个股 K 线主数据源。

Yahoo Chart API 默认调用 `query1.finance.yahoo.com`，可通过以下环境变量调整请求行为：

```bash
export YAHOO_CHART_PROXY=http://127.0.0.1:7897
export YAHOO_CHART_REQ_INTERVAL=0.35
export YAHOO_CHART_MAX_RETRIES=3
export YAHOO_CHART_SKIP_FRESH_DAYS=0
```

仅测试 Yahoo Chart API 单票连通性：

```bash
./.venv/bin/python -m data_fetch.fetch_kline_yahoo_chart_us --smoke --smoke-symbol AAPL
```

当前仓库未包含旧版数据源基准脚本，不再提供其根目录启动命令。新增数据源比较脚本应放在 `analysis/code/`，结果写入 `analysis/runs/`。比较时先看成功率，再看耗时；Yahoo Chart API 与 yfinance 共用 Yahoo 基础设施，二者的限流风险并不独立。

仅做连通性与凭证可用性测试（不跑全量）：

```bash
cd /opt/sf
export QUANTDASH_API_KEY=你的真实key
./.venv/bin/python -m data_fetch.fetch_kline_quantdash_us --smoke --smoke-symbol AAPL.US --period 1m
```

说明：

* `QUANTDASH_API_KEY` 未配置或无效时，脚本会快速失败并提示鉴权错误。
* QuantDash 默认请求 `count=500` 根数据。每日先探测基准股的最新交易日，再对每个本地文件一次性读取日期和行数；仅日期已更新且历史行数充足时跳过，否则重新请求。当前写入方式是用接口返回数据覆盖该股票 CSV，不是增量合并；需要长期历史时应明确设置足够的 `QD_COUNT` / `--count`，并备份研究输入。
* QuantDash 批量权限可用时优先走 batch；批量权限不可用时回退到单票请求。`QD_SINGLE_WORKERS=1` 是保守稳定配置，可在确认限流余量后调高。
* QuantDash 默认输出到 `./data/us_stocks`，CSV 列与现有流程保持一致：`date, open, close, high, low, volume, sector, industry`。
* Yahoo Chart API 不需要 API key；它按股票逐票请求，容易受到 query1 的 IP 限流影响。

---

## 云端部署（仅上传代码）

目标：**不上传本地 `data/`，只上传代码；云端按 `requirements.txt` 还原环境并拉取数据。**

### 1) 本地打包代码（自动排除数据与产物）

```bash
cd /path/to/sf
bash deploy/package_code_only.sh
```

脚本会生成 `sf-code-only-YYYYMMDD-HHMMSS.tar.gz`。

### 2) 上传代码包到云服务器

```bash
scp sf-code-only-YYYYMMDD-HHMMSS.tar.gz user@your-server:/opt/
```

### 3) 云端解压并初始化环境（不使用 conda）

```bash
ssh user@your-server
mkdir -p /opt/sf
tar -xzf /opt/sf-code-only-YYYYMMDD-HHMMSS.tar.gz -C /opt/sf

cd /opt/sf
bash deploy/bootstrap_venv.sh /opt/sf
```

### 4) 配置 Token（示例）

```bash
cd /opt/sf
cp .env.example .env
# 编辑 .env，填入真实 TUSHARE_TOKEN
```

### 5) 云端首次拉取数据并执行选股

```bash
cd /opt/sf
bash deploy/run_daily_cloud.sh /opt/sf
```

### 6) 定时任务（可选）

```bash
crontab -e
```

添加：

```cron
30 18 * * 1-5 /opt/sf/deploy/run_daily_cloud.sh /opt/sf >> /opt/sf/logs/daily.log 2>&1
```

---

## 云端部署（仅上传代码）

目标：**不上传本地 `data/`，只上传代码；云端按 `requirements.txt` 还原环境并拉取数据。**

### 1) 本地打包代码（自动排除数据与产物）

```bash
cd /path/to/sf
bash deploy/package_code_only.sh
```

脚本会生成 `sf-code-only-YYYYMMDD-HHMMSS.tar.gz`。

### 2) 上传代码包到云服务器

```bash
scp sf-code-only-YYYYMMDD-HHMMSS.tar.gz user@your-server:/opt/
```

### 3) 云端解压并初始化环境（不使用 conda）

```bash
ssh user@your-server
mkdir -p /opt/sf
tar -xzf /opt/sf-code-only-YYYYMMDD-HHMMSS.tar.gz -C /opt/sf

cd /opt/sf
bash deploy/bootstrap_venv.sh /opt/sf
```

### 4) 配置 Token（示例）

```bash
cd /opt/sf
cp .env.example .env
# 编辑 .env，填入真实 TUSHARE_TOKEN
```

### 5) 云端首次拉取数据并执行选股

```bash
cd /opt/sf
bash deploy/run_daily_cloud.sh /opt/sf
```

### 6) 定时任务（可选）

```bash
crontab -e
```

添加：

```cron
30 18 * * 1-5 /opt/sf/deploy/run_daily_cloud.sh /opt/sf >> /opt/sf/logs/daily.log 2>&1
```

---

## 参数说明

### 数据下载参数

调用方式：`python -m data_fetch.fetch_kline`。

| 参数                 | 默认值               | 说明                                                                         |
| ------------------ | ----------------- | -------------------------------------------------------------------------- |
| `--start`          | `20190101`        | 起始日期，格式 `YYYYMMDD` 或 `today`                                               |
| `--end`            | `today`           | 结束日期，格式同上                                                                  |
| `--stocklist`      | `./stocklist.csv` | 股票清单 CSV 路径（含 `ts_code` 或 `symbol`）                                        |
| `--exclude-boards` | `[]`              | 排除板块，枚举：`gem`(创业板 300/301) / `star`(科创板 688) / `bj`(北交所 .BJ / 4/8 开头)。可多选。 |
| `--out`            | `./data`          | 输出目录（自动创建）                                                                 |
| `--workers`        | `6`               | 并发线程数                                                                      |

**输出 CSV 列**：`date, open, close, high, low, volume`（按日期升序）。

**抓取与重试**：每支股票最多 3 次尝试；疑似限流/封禁触发 **600s 冷却**；其它异常采用递进式短等候重试（15s×尝试次数）。

### `select_stock.py`

| 参数           | 默认值              | 说明       |
| ------------ | ---------------- | -------- |
| `--data-dir` | `./data`         | CSV 行情目录 |
| `--config`   | `./configs.json` | 选择器配置    |
| `--date`     | 数据最后交易日          | 选股交易日    |

---

## 内置策略（Selector）

> **提示**：文中“窗口”均指交易日数量。实际实现均已替换为最新代码逻辑。

### 1. BBIKDJSelector（少妇战法）

核心逻辑：

* **价格波动约束**：最近 `max_window` 根收盘价的波动（`high/low-1`）≤ `price_range_pct`；
* **BBI 上升**：`bbi_deriv_uptrend`，允许一阶差分在 `bbi_q_threshold` 分位内为负（容忍回撤）；
* **KDJ 低位**：当日 J 值 **< `j_threshold`** 或 **≤ 最近 `max_window` 的 `j_q_threshold` 分位**；
* **MACD**：`DIF > 0`；
* **MA60 条件**：当日 `close ≥ MA60` 且最近 `max_window` 内存在“**有效上穿 MA60**”；
* **知行当日约束**：**收盘 > 长期线** 且 **短期线 > 长期线**。

`configs.json` 预设（与示例一致）：

```json
{
  "class": "BBIKDJSelector",
  "alias": "少妇战法",
  "activate": true,
  "params": {
    "j_threshold": 15,
    "bbi_min_window": 20,
    "max_window": 120,
    "price_range_pct": 1,
    "bbi_q_threshold": 0.2,
    "j_q_threshold": 0.10
  }
}
```

### 2. SuperB1Selector（SuperB1战法）

核心逻辑：

1. 在 `lookback_n` 窗内，存在某日 `t_m` **满足 BBIKDJSelector**；
2. 区间 `[t_m, 当日前一日]` 收盘价波动率 ≤ `close_vol_pct`；
3. 当日相对前一日 **下跌 ≥ `price_drop_pct`**；
4. 当日 J **< `j_threshold`** 或 **≤ `j_q_threshold` 分位**；
5. **知行约束**：

   * 在 `t_m` 当日：**收盘 > 长期线** 且 **短期线 > 长期线**；
   * 在 **当日**：只需 **短期线 > 长期线**。

`configs.json` 预设：

```json
{
  "class": "SuperB1Selector",
  "alias": "SuperB1战法",
  "activate": true,
  "params": {
    "lookback_n": 10,
    "close_vol_pct": 0.02,
    "price_drop_pct": 0.02,
    "j_threshold": 10,
    "j_q_threshold": 0.10,
    "B1_params": {
      "j_threshold": 15,
      "bbi_min_window": 20,
      "max_window": 120,
      "price_range_pct": 1,
      "bbi_q_threshold": 0.3,
      "j_q_threshold": 0.10
    }
  }
}
```

### 3. BBIShortLongSelector（补票战法）

核心逻辑：

* **BBI 上升**（容忍回撤）；
* 最近 `m` 日内：

  * 长 RSV（`n_long`）**全 ≥ `upper_rsv_threshold`**；
  * 短 RSV（`n_short`）出现“**先 ≥ upper，再 < lower**”的序列结构；
  * 当日短 RSV **≥ upper**；
* **MACD**：`DIF > 0`；
* **知行当日约束**：**收盘 > 长期线** 且 **短期线 > 长期线**。

`configs.json` 预设：

```json
{
  "class": "BBIShortLongSelector",
  "alias": "补票战法",
  "activate": true,
  "params": {
    "n_short": 5,
    "n_long": 21,
    "m": 5,
    "bbi_min_window": 2,
    "max_window": 120,
    "bbi_q_threshold": 0.2,
    "upper_rsv_threshold": 75,
    "lower_rsv_threshold": 25
  }
}
```

### 4. PeakKDJSelector（填坑战法）

核心逻辑：

* 基于 `open/close` 的 `oc_max` 寻找峰值（`scipy.signal.find_peaks`）；
* 选择最新峰 `peak_t` 与其前方**有效参照峰** `peak_(t-n)`：要求 `oc_t > oc_(t-n)`，并确保区间内其它峰不“抬高门槛”；且 `oc_(t-n)` 必须 **高于区间最低收盘价 `gap_threshold`**；
* 当日收盘与 `peak_(t-n)` 的波动率 ≤ `fluc_threshold`；
* 当日 J **< `j_threshold`** 或 **≤ `j_q_threshold` 分位**；
* **知行当日约束**：**收盘 > 长期线** 且 **短期线 > 长期线**。

`configs.json` 预设：

```json
{
  "class": "PeakKDJSelector",
  "alias": "填坑战法",
  "activate": true,
  "params": {
    "j_threshold": 10,
    "max_window": 120,
    "fluc_threshold": 0.03,
    "j_q_threshold": 0.10,
    "gap_threshold": 0.2
  }
}
```

### 5. MA60CrossVolumeWaveSelector（上穿60放量战法）

核心逻辑：

1. 当日 J **< `j_threshold`** 或 **≤ `j_q_threshold` 分位**；
2. 最近 `lookback_n` 内存在**有效上穿 MA60**；
3. 以上穿日 `T` 到当日区间内 **High 最大日** 作为 `Tmax`，定义上涨波段 `[T, Tmax]`，其 **平均成交量 ≥ `vol_multiple` × 上穿前等长或截断窗口的平均量**；
4. `MA60` 的最近 `ma60_slope_days` 日 **回归斜率 > 0**；
5. **知行当日约束**：**收盘 > 长期线** 且 **短期线 > 长期线**。

`configs.json` 预设：

```json
{
  "class": "MA60CrossVolumeWaveSelector",
  "alias": "上穿60放量战法",
  "activate": true,
  "params": {
    "lookback_n": 25,
    "vol_multiple": 1.8,
    "j_threshold": 15,
    "j_q_threshold": 0.10,
    "ma60_slope_days": 5,
    "max_window": 120
  }
}
```

> **已移除**：`BreakoutVolumeKDJSelector（TePu 战法）`。

### 6. BigBullishVolumeSelector（暴力K战法）

核心逻辑：

1. **当日为长阳**：  
   当日涨幅 `(close / prev_close - 1)` **大于 `up_pct_threshold`**；

2. **上影线短**：  
   上影线比例  
   \[
   \frac{High - \max(Open, Close)}{\max(Open, Close)}
   \]
   **小于 `upper_wick_pct_max`**，用于过滤冲高回落型假阳线；

3. **放量突破**：  
   当日成交量  
   \[
   Volume_{today} \ge vol\_multiple \times \text{前 } n \text{ 日均量}
   \]

4. **贴近知行短线（不过热）**：  
   计算 `ZXDQ = EMA(EMA(C,10),10)`，要求  
   \[
   Close < ZXDQ \times close\_lt\_zxdq\_mult
   \]  
   用于过滤已经明显脱离短线均值、过度加速的股票。

5. （可选）**收阳约束**：`close ≥ open`。

该策略意在捕捉：
> **“刚刚放量启动的强势阳线，但尚未远离短期均线、仍具延续空间的个股”。**

---

`configs.json` 预设：

```json
{
  "class": "BigBullishVolumeSelector",
  "alias": "暴力K战法",
  "activate": true,
  "params": {
    "up_pct_threshold": 0.06,
    "upper_wick_pct_max": 0.02,
    "require_bullish_close": true,
    "close_lt_zxdq_mult": 1.15,
    "vol_lookback_n": 20,
    "vol_multiple": 2.5
  }
}


---

## 项目结构

```text
.
├── select_stock.py          # 批量选股启动入口
├── run_jxt_v7.py            # JXT 启动入口
├── us_daily.sh              # 美股数据更新、选股、推送入口
├── us_premarket_jxt.sh      # JXT 盘前入口
├── install_stock_tracking_cron.sh
├── data_fetch/              # Tushare、QuantDash、Yahoo、股票池下载
├── strategies/              # Selector、波段识别、板块轮动、价格筛选
│   └── jxt_v7/              # JXT 策略实现与说明
├── features/                # BBD、阳线缺口、强势回调、均线启动特征
├── reporting/               # 可视化与跟踪报告发布
├── utils/                   # 安全文件读写等公共工具
├── deploy/                  # 环境初始化、打包、云端启动辅助脚本
├── analysis/code/           # 本地研究、回测和回归测试
├── analysis/runs/           # 研究输出
├── configs.json             # 策略参数，其他 configs*.json 同样留在外层
├── data/                    # 行情，data/tools/ 保留股票池 CSV
├── jxt_v7/                  # 仅保留原 cache/、output/ 数据目录
└── stock_data_cache/        # 原有缓存目录
```

所有命令从仓库根目录执行。根目录仅保留启动入口，分类目录中的可执行模块用 `python -m 包名.模块名`，不要再使用旧根目录脚本路径，也不要直接执行 `python data_fetch/xxx.py`。Python 内部使用包导入，例如 `from strategies.Selector import MoZhuaSelector`。配置中的 `class` 仍写 `MoZhuaSelector`，不需要添加包前缀。

```bash
python -m data_fetch.fetch_kline_quantdash_us --help
python -m data_fetch.fetch_kline_yfince_us --help
python -m data_fetch.fetch_kline_yahoo_chart_us --help
python -m data_fetch.get_us_stocks_csv --help
python select_stock.py --config configs.json --data-dir data/us_stocks
python -m strategies.scan_three_wave --help
python -m strategies.find_stock_by_price_concurrent --help
python -m reporting.publish_stock_tracking_report --help
python run_jxt_v7.py --help
python -m unittest discover -s analysis/code -p 'test_*.py'
```

脚本分类不改变行情、缓存、配置和结果位置：股票池默认仍写入 `data/tools/stocklist_us.csv`，JXT 仍使用 `jxt_v7/cache/` 与 `jxt_v7/output/`。原有 `.env` 加载、日志和策略输出目录保留。`us_daily.sh`、`us_premarket_jxt.sh`、`deploy/run_daily_cloud.sh` 已同步新调用方式；已有调用这些 Shell 入口的 cron 不需要修改，外部直接调用旧 Python 路径的任务需改为上述模块命令。`analysis/` 仍按仓库约定由 Git 忽略，不随版本控制分发研究代码及结果。

---

## 常见问题

**Q1：为什么抓取会“卡住很久”？**
可能命中 Tushare 频控或网络封禁。脚本检测到典型关键字（如“访问频繁/429/403”）时，会进入\*\*长冷却（默认 600s）\*\*再重试。

**Q2：为什么不做增量合并？**
考虑采用增量更新会遇到前复权的问题，本版选择**每次全量覆盖写入**。

**Q3：创业板/科创板/北交所如何排除？**
运行时使用 `--exclude-boards gem star bj`，或按需选择其一/其二。

---

## 免责声明

* 本仓库仅供学习与技术研究之用，**不构成任何投资建议**。股市有风险，入市需谨慎。
* 数据来源与接口可能随平台策略调整而变化，请合法合规使用。
* 致谢 **@Zettaranc** 在 Bilibili 的无私分享：[https://b23.tv/JxIOaNE](https://b23.tv/JxIOaNE)
