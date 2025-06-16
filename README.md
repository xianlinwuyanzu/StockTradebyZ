# Z哥少妇战法与补票战法Python实战

## 目录
- [项目简介](#项目简介)
- [特性一览](#特性一览)
- [快速上手](#快速上手)
- [参数说明](#参数说明)
- [项目结构](#项目结构)
- [免责声明](#免责声明)

---

## 项目简介
本仓库提供两个核心脚本：

| 名称 | 作用 |
| ---- | ---- |
| **`fetch_csi300_kline.py`** | 从 AkShare 实时快照筛选出总市值 ≥ 指定阈值且排除创业板的股票，抓取其历史日 K 线并保存为 CSV。支持多线程、增量更新和今日快照自动补齐。|
| **`select_stock.py`** | 读取本地 CSV 行情，根据 `configs.json` 中定义的策略（Selector）进行批量选股，并把结果写入 `select_results.log` 和控制台。|

默认内置Z哥少妇战法和补票战法的选股策略，文件位于 `Selector.py` 中：  

- **BBIKDJSelector** （中文别名：少妇战法）  
- **BBIShortLongSelector** （中文别名：补票战法）

---

## 特性一览
- 🚀 **多线程** 抓取，200+ 股票约数分钟完成  
- ♻️ **增量更新**，只补增量数据，支持强制刷新今日快照  
- 🧩 **插件式 Selector**：策略统一放在 `Selector.py` ，配置通过 `configs.json` 热插拔  
- 🗂 **CSV 存档**：单股一文件，易于回测或可视化  
- 🔧 完整 **CLI 参数**，满足批量任务、定时任务场景  
- 📝 **日志落盘**，抓取写 `fetch.log`，选股写 `select_results.log`

---

## 快速上手

### 1. 安装依赖
```bash
# 建议 Python 3.10+，使用虚拟环境
pip install -r requirements.txt
````

> 依赖主要包括：`akshare`、`pandas`、`numpy`、`tqdm` 等。

### 2. 下载历史行情

```bash
python fetch_csi300_kline.py \
  --min-mktcap 2.5e10 \          # 市值阈值（默认 250 亿）
  --start 20050101 \             # 起始日期
  --end today \                  # 结束日期
  --out ./data \                 # 输出目录
  --workers 20                   # 并发线程数
```

### 3. 运行选股

```bash
python select_stock.py \
  --data-dir ./data \            # CSV 行情目录
  --config ./configs.json \      # 策略配置
  --date 2025-06-14              # 交易日（缺省=最新）
```

日志示例：

```
============== 选股结果 [少妇战法] ==============
交易日: 2025-06-14
符合条件股票数: 3
000651, 600519, 002714
```

---

## 参数说明

### 脚本参数

| 脚本                      | 关键参数              | 说明                                          |
| ----------------------- | ----------------- | ------------------------------------------- |
| `fetch_csi300_kline.py` | `--min-mktcap`    | 市值过滤阈值（元）。默认 2.5e10。 fileciteturn0file2  |
|                         | `--start / --end` | 日期范围，格式 `YYYYMMDD`；`--end today` 自动取当前日期    |
|                         | `--workers`       | 并发线程数，默认 20                                 |
| `select_stock.py`       | `--date`          | 选股所用交易日；缺省时自动取数据中最新日期 fileciteturn0file3 |
|                         | `--tickers`       | `all` 或逗号分隔股票代码列表，精细控制股票池                   |
|                         | `--config`        | Selector 配置文件路径，默认 `configs.json`           |

### 内置策略参数

> 以下参数来自 `configs.json`，可按需调整。 fileciteturn0file1

#### 1. BBIKDJSelector（少妇战法）

| 参数               | 示例值  | 说明                                                                    |
| ---------------- | ---- | --------------------------------------------------------------------- |
| `threshold`      | `-5` | 日线 **J 值上限**。当当天 J < threshold 时满足条件，阈值越低要求越严格。 fileciteturn0file4 |
| `bbi_min_window` | `10` | 用于检测 **BBI 单调上升** 的最短窗口长度（交易日数）。                                      |
| `bbi_offset_n`   | `8`  | 选定距今日 *n* 日的“锚点”作为 BBI 上升段的终点，可避免近期震荡。                                |
| `max_window`     | `60` | 计算技术指标时最多读取的 K 线天数，限制窗口大小防止性能下降。                                      |

#### 2. BBIShortLongSelector（补票战法）

| 参数               | 示例值  | 说明                                          |
| ---------------- | ---- | ------------------------------------------- |
| `n_short`        | `3`  | **短期 RSV** 的计算窗口 N1。滚动取近 N1 日最低 / 收盘价。      |
| `n_long`         | `21` | **长期 RSV** 的计算窗口 N2。                        |
| `m`              | `3`  | **检测区间长度**。在最近 m 个交易日内同时满足 RSV、BBI、DIF 等条件。 |
| `bbi_min_window` | `5`  | BBI 上升段的最短窗口。                               |
| `bbi_offset_n`   | `0`  | BBI 锚点距离当前日的偏移量。                            |
| `max_window`     | `60` | 读取历史 K 线最大长度。                               |


---

## 项目结构

```
.
├── appendix.json            # 额外自选股票池（会与市值筛选结果合并）
├── configs.json             # Selector 运行时配置
├── fetch_csi300_kline.py    # 历史行情抓取脚本
├── select_stock.py          # 批量选股脚本
├── Selector.py              # 策略实现
├── data/                    # CSV 数据目录（运行后生成）
├── fetch.log
└── select_results.log
```

---

## 免责声明  
- 本仓库代码仅供学习与技术研究之用，**不构成任何投资建议**。股市有风险，入市需谨慎。  
- 感谢师尊 **@Zettaranc**  https://b23.tv/JxIOaNE 的无私分享
---

