# 二波交易策略比较实验

- 二波事件：315 个；策略版本：66 个。
- 名义窗口：2025-08-20 至 2026-08-20；结果是同一事件集上的样本内比较。
- 二波回调条件：价格回调达到 4% 或 J 下降达到 30，且价格回调不超过 28%。
- 该实验用于寻找当前样本中的候选高收益规则，不代表样本外最优；策略参数排名存在过拟合风险。
- `portfolio_return` 按总资金计算；分批策略未使用的资金保留现金。
- 未计手续费、滑点、税费；重叠事件未模拟账户持仓冲突。

## 推荐排名

排名按完整交易的组合收益率中位数，其次按平均值；同时查看胜率和最小单笔收益，避免只看均值。

| rank_by_median | strategy | complete_count | median_portfolio_return | mean_portfolio_return | win_rate | min_return | median_deployed_return |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | j_upturn_all_in_target_top20 | 88 | 3.55% | 2.21% | 71.59% | -26.71% | 3.55% |
| 2 | j_first_all_in_target_top20 | 165 | 3.00% | 1.71% | 69.09% | -27.35% | 3.00% |
| 3 | j_continue_all_in_target_top20 | 168 | 3.00% | 1.65% | 69.05% | -27.35% | 3.00% |
| 4 | j_upturn_all_in_target_breakout25 | 88 | 2.97% | 1.76% | 61.36% | -29.71% | 2.97% |
| 5 | j_upturn_scale3_target_breakout25 | 87 | 2.02% | 0.72% | 62.07% | -36.36% | 3.57% |
| 6 | j_continue_all_in_target_breakout25 | 168 | 1.83% | 0.73% | 58.33% | -38.00% | 1.83% |
| 7 | j_first_all_in_target_breakout25 | 165 | 1.82% | 0.73% | 58.18% | -38.00% | 1.82% |
| 8 | j_upturn_all_in_fixed8 | 88 | 1.71% | 1.26% | 60.23% | -16.98% | 1.71% |
| 9 | j_continue_all_in_fixed20 | 168 | 1.66% | 0.42% | 57.14% | -32.44% | 1.66% |
| 10 | j_first_all_in_fixed20 | 165 | 1.65% | 0.44% | 57.58% | -32.44% | 1.65% |
| 11 | j_upturn_scale3_target_top20 | 87 | 1.62% | 1.15% | 71.26% | -24.25% | 3.54% |
| 12 | j_first_scale3_target_top20 | 163 | 1.24% | 0.16% | 66.26% | -27.27% | 3.00% |

## 前后半段稳健性排名

按前后两个时间段中较低的收益中位数排序，作为比样本内最高值更保守的参考：

| rank_by_stable_half_median | strategy | early_count | late_count | early_median_return | late_median_return | min_half_median_return | half_median_gap |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | j_first_all_in_target_top20 | 81 | 84 | 3.81% | 2.70% | 2.70% | 1.11% |
| 2 | j_upturn_all_in_target_top20 | 45 | 43 | 4.87% | 2.56% | 2.56% | 2.30% |
| 3 | j_continue_all_in_target_top20 | 84 | 84 | 3.75% | 2.54% | 2.54% | 1.21% |
| 4 | j_continue_all_in_target_breakout25 | 84 | 84 | 2.20% | 1.76% | 1.76% | 0.44% |
| 4 | j_first_all_in_target_breakout25 | 81 | 84 | 2.56% | 1.76% | 1.76% | 0.80% |
| 6 | j_upturn_scale3_target_top20 | 45 | 42 | 1.96% | 1.23% | 1.23% | 0.72% |
| 7 | j_first_scale3_target_top20 | 81 | 82 | 2.05% | 1.09% | 1.09% | 0.97% |
| 8 | j_continue_scale3_target_top20 | 84 | 82 | 1.96% | 1.05% | 1.05% | 0.91% |
| 9 | j_upturn_all_in_fixed8 | 45 | 43 | 2.85% | 1.04% | 1.04% | 1.81% |
| 9 | j_upturn_all_in_target_breakout25 | 45 | 43 | 4.87% | 1.04% | 1.04% | 3.83% |

## 文件

- 全部策略排名：`strategy_comparison.csv`
- 全部逐笔交易：`strategy_trade_details.csv`
- 前 12 策略均值/中位数图：`strategy_comparison.png`
- 稳健性字段：`early_median_return`、`late_median_return`、`min_half_median_return`；前后半段分界日见 `split_date`。

## 解释

`j_first` 是当前实现的首个有效低 J；`j_continue` 会跳过第一个不满足周期/结构条件的低 J，继续找后续有效 J；`j_upturn` 额外要求 J 当日高于前一日。`breakout` 不使用 J，等第二顶部上方 2% 的收盘突破后次日开盘进入。

固定持有策略用于回答“持有多久更合适”；`target_top`/`target_breakout` 用价格目标退出；`ma5_trail` 用收盘跌破 5 日均线退出；`bullish_partial` 使用中阳线分批止盈。

最高收益策略只代表当前事件样本内的排名。正式采用前，应使用时间切分样本外验证，并优先关注中位数、最小收益和最大亏损，而不是只选平均收益最高的一行。
