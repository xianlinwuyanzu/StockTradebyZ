import time
import tushare as ts  # 行情数据接口（需自行注册获取token）
import pandas as pd

# 初始化行情接口（以tushare为例，实际可替换为聚宽、同花顺等合规接口）
ts.set_token("745e967a0e097ad5f40c3f665fd81133bb4fcfd0fcdcb1908cc8fd06")
pro = ts.pro_api()

# 打板策略参数
MAX_POSITION = 10000  # 单只股票最大持仓金额
MIN_BUY_VOLUME = 10000  # 最小买入委托量（手）
MIN_SEAL_AMOUNT = 5000  # 最小封单金额（万元）
STOCK_POOL = ["60开头", "00开头"]  # 股票池（排除ST、新股等）


def get_limit_up_stocks():
    """获取当前涨停股票列表"""
    # 获取当日所有股票行情（实际应使用实时行情接口）
    today = time.strftime("%Y%m%d")
    df = pro.daily(trade_date=today)  # 日线数据（实际需实时分钟级数据）
    
    # 筛选涨停股（涨幅>=9.8%，根据实际涨跌幅计算）
    df["pct_chg"] = (df["close"] - df["open"]) / df["open"] * 100
    limit_up_stocks = df[df["pct_chg"] >= 9.8]
    
    # 过滤股票池（排除ST、创业板注册制新股等）
    limit_up_stocks = limit_up_stocks[
        limit_up_stocks["ts_code"].str.startswith(tuple(STOCK_POOL))
    ]
    return limit_up_stocks


def check_seal_strength(stock_code):
    """检查封单强度（封单金额、封单量占比等）"""
    # 实际需调用实时盘口数据接口获取封单量、成交量等
    # 此处模拟数据
    seal_volume = 20000  # 封单量（手）
    total_volume = 50000  # 当日总成交量（手）
    seal_amount = seal_volume * 100 * 10  # 封单金额（元，假设股价10元）
    
    # 封单强度条件：封单金额达标，且封单量占成交量比例较高
    if seal_amount >= MIN_SEAL_AMOUNT * 10000 and seal_volume / total_volume > 0.3:
        return True
    return False


def buy_strategy(stock_code):
    """打板买入逻辑"""
    # 检查封单强度
    if not check_seal_strength(stock_code):
        print(f"股票{stock_code}封单强度不足，不满足买入条件")
        return
    
    # 获取当前股价（模拟）
    price = 10.0  # 涨停价
    # 计算买入数量（不超过最大持仓金额）
    buy_shares = int(MAX_POSITION / price / 100) * 100  # 取100的整数倍（A股最小单位）
    
    if buy_shares > 0:
        print(f"执行买入：股票{stock_code}，价格{price}元，数量{buy_shares}股，金额{buy_shares*price}元")
        # 实际交易需调用券商接口下单，此处仅模拟
    else:
        print(f"资金不足，无法买入股票{stock_code}")


def main():
    """主程序：实时监控并执行打板策略"""
    print("量化打板程序启动，开始监控涨停股...")
    while True:
        # 非交易时间退出（假设交易时间9:30-15:00）
        now = time.localtime()
        if not (9 <= now.tm_hour < 15) or (now.tm_hour == 15 and now.tm_min > 0):
            print("非交易时间，程序休眠")
            # time.sleep(3600)  # 休眠1小时
            # continue
        
        # 获取涨停股列表
        limit_up_stocks = get_limit_up_stocks()
        if limit_up_stocks.empty:
            print("当前无符合条件的涨停股，5分钟后重试")
            time.sleep(300)
            continue
        
        # 对每只涨停股执行买入策略
        # for _, stock in limit_up_stocks.iterrows():
        #     stock_code = stock["ts_code"]
        #     buy_strategy(stock_code)
        
        # 每30秒监控一次（实际需根据接口频率限制调整）
        time.sleep(30)


if __name__ == "__main__":
    main()