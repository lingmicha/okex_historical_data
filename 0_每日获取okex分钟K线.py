import ccxt
from datetime import datetime, timedelta
from pathlib import Path
import time
import pandas as pd
import pytz


data_path = Path(__file__).parent / 'data' / 'kline'
if not data_path.exists():
    data_path.mkdir()

headers = ['candle_begin_time','open','high','low','close','number_of_contracts','volume','quote_volume','confirm']
target = ['candle_begin_time','open','high','low','close','volume','quote_volume','trade_num','taker_buy_base_asset_volume','taker_buy_quote_asset_volume']


def get_usdt_margined_swap_markets(exchange):
    all_markets = exchange.markets
    usdt_swap_markets = [symbol for symbol in all_markets if all_markets[symbol]['swap'] and not all_markets[symbol]['inverse'] and all_markets[symbol]['quote'] == 'USDT']
    return usdt_swap_markets


def get_inactive_usdt_margined_swap_markets(exchange):
    all_markets = exchange.markets
    inactive_usdt_swap_markets = [symbol for symbol in all_markets if all_markets[symbol]['swap'] and not all_markets[symbol]['inverse'] and all_markets[symbol]['quote'] == 'USDT' and all_markets[symbol]['active'] != True]
    return inactive_usdt_swap_markets


def okex_fetch_minute_kline(exchange, symbol, date):

    # 限速：20次/2s
    id = exchange.market_id(symbol)
    # date = date.replace(hour=0, minute=0, second=0, microsecond=0)
    start = int(date.timestamp() * 1000) - 60*1000
    end = int((date + timedelta(days=1)).timestamp() * 1000)

    retry_count = 0
    kline = []
    till = end
    while start + 60*1000 < till:
        try:
            time.sleep(exchange.rateLimit / 1000)
            # 默认的fetch_ohlcv方法只返回ohlcv数据，用implied获取更多数据
            # ohlcv = exchange.fetch_ohlcv(symbol, timeframe='1m', since=since)
            ohlcv = ok_exchange.publicGetMarketHistoryCandles(
                {
                    'instId': id,
                    'bar': '1m',
                    'limit': 100,
                    'before': start,
                    'after': till,
                }
            )
        except Exception as e:
            print(f'获取 {symbol} K线数据失败: {e}')
            if retry_count > 3:
                print(f'反复获取 {symbol} K线数据失败,退出')
                exit(1)
            time.sleep(60)
            retry_count += 1
            continue

        assert ohlcv["code"] == "0", f'获取 {symbol} K线数据失败: {ohlcv["msg"]}'

        if len(ohlcv['data']) == 0:
            # 周期内无K线
            break

        kline += ohlcv['data']
        till = int(ohlcv['data'][-1][0])
        retry_count = 0

    return [line for line in kline if int(line[0]) < end]


if __name__ == '__main__':

    # 限速：20次/2s
    # GET /api/v5/market/history-candles
    # 20 requests per second for private endpoints with a 60-second timeout
    ok_exchange = ccxt.okex5(
        {
            'enableRateLimit': True,  # Enable rate limiting
            'rateLimit': 100,  # Number of requests per second
            'refetch': 60,  # Number of seconds before the order book is refetched (default: 60)
            'options': {
                'defaultType': 'swap',  # ←-------------- swap
            }
        }
    )

    # 获取所有交易对
    ok_exchange.load_markets()
    usdt_swap = get_usdt_margined_swap_markets(ok_exchange)
    print(f'U本位合约数量: {len(usdt_swap)}')
    # print(f'U本位合约: {usdt_swap}')

    inactive_usdt_swap = get_inactive_usdt_margined_swap_markets(ok_exchange)
    print(f'非活跃U本位合约数量: {len(inactive_usdt_swap)}')
    # print(f'非活跃U本位合约: {inactive_usdt_swap}')

    current_date = datetime.utcnow()
    current_date = current_date.replace(tzinfo=pytz.utc)
    print(f'UTC当前时间: {current_date}')

    fetch_date = current_date.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    print(f'获取K线数据时间: {fetch_date}')

    start_date = fetch_date
    end_date = fetch_date

    # 自定义起始日期和结束日期
    # start_date = datetime(2023, 11, 29, tzinfo=pytz.utc)
    # end_date = datetime(2023, 12, 11, tzinfo=pytz.utc)

    # 生成日期列表
    dates_to_process = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]

    for fetch_date in dates_to_process:

        print(f"处理日期: {fetch_date}")

        date_dir = data_path / fetch_date.strftime('%Y%m%d')
        if not date_dir.exists():
            date_dir.mkdir()
        else:
            print(f'文件夹已存在,清理文件夹: {date_dir}')
            for file in date_dir.glob('*'):
                file.unlink()

        retry_count = 0
        for symbol in usdt_swap:
            normal_symbol = symbol.split(':', 2)[0].replace('/', '-')
            print(f'获取 {symbol} K线数据')
            kline = okex_fetch_minute_kline(ok_exchange, symbol, fetch_date)
            if len(kline) == 0:
                continue
            # print(f'获取 {symbol} K线数据成功')
            # print(f'写入 {symbol} K线数据')
            df = pd.DataFrame(kline, columns=headers)
            if not (df['confirm'] == '1').all():
                print('有K线未闭合')
                exit(1)

            df.drop(columns=['confirm', 'number_of_contracts'], inplace=True)
            df['trade_num'] = 1
            df['taker_buy_base_asset_volume'] = 1
            df['taker_buy_quote_asset_volume'] = 1
            df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'], unit = 'ms')
            df.to_csv(date_dir / f'{normal_symbol}.csv', index=False)

            # with open(date_dir / f'{normal_symbol}.csv', 'w') as f:
            #     for line in kline:
            #         f.write(','.join([str(i) for i in line]) + '\n')
            # print(f'写入 {symbol} K线数据成功')

    print('获取K线数据完成')
    exit(0)
