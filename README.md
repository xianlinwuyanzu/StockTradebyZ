# Z哥少妇战法·补票战法·TePu 战法 Python 实战

---

## 项目简介

本仓库提供两个核心脚本：

| 名称                    | 功能简介                                                                                                                    |
| --------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| **`fetch_kline.py`**  | *按市值筛选* A 股股票，并抓取其**历史 K 线**保存为 CSV。支持 **AkShare / Tushare / Mootdx** 三大数据源，自动增量更新、多线程下载。|
| **`select_stock.py`** | 读取本地 CSV 行情，依据 `configs.json` 中的 **Selector** 定义批量选股，结果输出到 `select_results.log` 与控制台。                                   |

内置三套经典选股策略（见 `Selector.py`）：

* **BBIKDJSelector**  → 「少妇战法」
* **BBIShortLongSelector**  → 「补票战法」
* **BreakoutVolumeKDJSelector**  → 「TePu 战法」

---

## 快速上手

### 安装依赖

```bash
# 建议使用 Python 3.10+ 并启用虚拟环境
pip install -r requirements.txt
```

> 关键依赖：`akshare`、`tushare`、`mootdx`、`pandas`、`tqdm` 等。


### 下载历史行情

```bash
python fetch_kline.py \
  --datasource mootdx      # 数据源：mootdx / akshare / tushare
  --frequency 4            # K 线频率编码（4 = 日线）
  --exclude-gem True       # 排除创业板 / 科创板 / 北交所
  --min-mktcap 5e9         # 最小总市值（元），默认 5e9
  --max-mktcap +inf        # 最大总市值（元），默认无限制
  --start 20200101         # 起始日期（YYYYMMDD 或 today）
  --end today              # 结束日期
  --out ./data             # 输出目录
  --workers 10             # 并发线程数
```

*首次执行* 下载完整历史；后续运行脚本时自动 **增量更新**（仅补充缺失交易日）。

### 运行选股

```bash
python select_stock.py \
  --data-dir ./data        # CSV 行情目录
  --config ./configs.json  # Selector 配置
  --date 2025-06-21        # 交易日（缺省 = 最新一个交易日）
```

控制台示例（TePu 战法）：

```
============== 选股结果 [TePu 战法] ===============
交易日: 2025-06-21
符合条件股票数: 1
600690
```

---

## 参数说明

### `fetch_kline.py`

| 参数                  | 默认值 | 说明                                     |
| ------------------- | -------- | -------------------------------------- |
| `--datasource`      | `mootdx` | 选用数据源：`tushare` / `akshare` / `mootdx` |
| `--frequency`       | `4`      | K 线频率编码（见下表）                           |
| `--exclude-gem`     | `True`     | 为True则排除创业板（300/301）、科创板（688）、北交所（4/8 开头）    |
| `--min-mktcap`      | `5e9`    | 最小总市值（元），含边界                           |
| `--max-mktcap`      | `+inf`   | 最大总市值（元），含边界                           |
| `--start` / `--end` | `today`  | 日期范围，`YYYYMMDD` 或 `today`              |
| `--out`             | `./data` | 输出目录                                   |
| `--workers`         | `10`     | 并发线程数                                  |

#### K 线频率编码

|  编码 |  周期  | Mootdx 关键字 | 用途      |
| :-: | :--: | :--------: | ------- |
|  0  |  5 分 |    `5m`    | 高频 / 分时 |
|  1  | 15 分 |    `15m`   | 高频      |
|  2  | 30 分 |    `30m`   | 高频      |
|  3  | 60 分 |    `1h`    | 波段      |
|  4  |  日线  |    `day`   | ★ 常用    |
|  5  |  周线  |   `week`   | 中长线     |
|  6  |  月线  |    `mon`   | 中长线     |
|  7  |  1 分 |    `1m`    | Tick    |
|  8  |  1 分 |    `1m`    | 同上      |
|  9  |  日线  |    `day`   | 备用      |
|  10 |  季线  |   `3mon`   | 长周期     |
|  11 |  年线  |   `year`   | 长周期     |

### `select_stock.py`

| 参数           | 默认值              | 说明                                 |
| ------------ | ---------------- | ---------------------------------- |
| `--data-dir` | `./data`         | CSV 行情目录，对应 `fetch_kline.py --out` |
| `--config`   | `./configs.json` | Selector 配置文件                      |
| `--date`     | 最新交易日            | 选股所用交易日                            |
| `--tickers`  | `all`            | 指定股票池（逗号分隔）；`all` 表示使用全部本地 CSV     |

> 其余参数请执行 `python select_stock.py --help` 查看。

### 内置策略参数

以下参数节选自 `configs.json`，可按需调整。

#### 1. BBIKDJSelector（少妇战法）

| 参数                | 默认值   | 说明                            |
| ----------------- | ----- | ----------------------------- |
| `threshold`       | `-6`  | 当日 J 值上限（J < threshold）       |
| `bbi_min_window`  | `17`  | BBI 持续上升的最短交易日数               |
| `bbi_offset_n`    | `2`   | 选取距今日 *n* 日的锚点，过滤震荡           |
| `max_window`      | `60`  | 技术指标最长窗口                      |
| `price_range_pct` | `100` | 最近 *max\_window* 内收盘价波动上限 (%) |

核心：**BBI 上行 + J 低位 + DIF>0**，辅以波动率过滤。

#### 2. BBIShortLongSelector（补票战法）

| 参数               | 默认值  | 说明          |
| ---------------- | ---- | ----------- |
| `n_short`        | `3`  | 短期 RSV 窗口   |
| `n_long`         | `21` | 长期 RSV 窗口   |
| `m`              | `3`  | 判别区间长度      |
| `bbi_min_window` | `5`  | BBI 上升段最短窗口 |
| `bbi_offset_n`   | `0`  | BBI 锚点偏移    |
| `max_window`     | `60` | 历史窗口        |

逻辑：短长 RSV 高位 + BBI 上升 + DIF>0，并要求短 RSV 曾跌破 20 形成“回抽”。

#### 3. BreakoutVolumeKDJSelector（TePu 战法）

| 参数                 | 默认       | 说明          |
| ------------------ | -------- | ----------- |
| `j_threshold`      | `1`      | 当日 J 值上限    |
| `up_threshold`     | `3.0`    | 放量长阳涨幅 (%)  |
| `volume_threshold` | `0.6667` | 缩量比例        |
| `offset`           | `15`     | 放量窗口 (日)    |
| `max_window`       | `60`     | 历史窗口        |
| `price_range_pct`  | `100`    | 收盘价波动上限 (%) |

---

## 项目结构

```
.
├── appendix.json            # 额外股票池（与筛选结果合并）
├── configs.json             # Selector 配置
├── fetch_kline.py           # 行情抓取脚本
├── select_stock.py          # 批量选股脚本
├── Selector.py              # 策略实现
├── data/                    # CSV 数据输出目录
├── fetch.log                # 抓取日志
└── select_results.log       # 选股日志
```

---

## 免责声明

* 本仓库仅供学习与技术研究之用，**不构成任何投资建议**。股市有风险，入市需审慎。
* 致谢 **@Zettaranc** 在 Bilibili 的无私分享：[https://b23.tv/JxIOaNE](https://b23.tv/JxIOaNE)
