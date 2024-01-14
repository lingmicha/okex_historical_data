import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

# 1. 遍历kline目录，获取所有币对的分钟K线
# 2. 读入分钟K线csv文件， 重采样成小时K线
# 3. 将每个币对最初的上线时间，最后的下线时间，以及没有成交的小时K线时间段，保存到csv文件中


def find_continuous_segments(timestamps):
    # 将时间戳字符串转换为 datetime 对象并按升序排序
    if len(timestamps) == 0:
        return []

    sorted_timestamps = sorted(timestamps)

    segments = []
    current_segment_start = sorted_timestamps[0]
    current_segment_end = sorted_timestamps[0]

    for timestamp in sorted_timestamps[1:]:
        # 如果当前时间戳与前一个时间戳连续，扩展当前时间段
        if timestamp == current_segment_end + timedelta(hours=1):
            current_segment_end = timestamp
        else:
            # 否则，结束当前时间段，开始一个新的时间段
            segments.append((current_segment_start, current_segment_end))
            current_segment_start = timestamp
            current_segment_end = timestamp

    # 处理最后一个时间段
    segments.append((current_segment_start, current_segment_end))

    results = []
    for i, (start, end) in enumerate(segments):
        if start == end:
            results.append(f"{start.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            results.append(f"{start.strftime('%Y-%m-%d %H:%M:%S')} - {end.strftime('%Y-%m-%d %H:%M:%S')}")

    return results


overrides = {
    'DOWNTIME': ['2022-12-18 07:00:00', '2022-12-18 17:00:00'],
    # 'BABYDOGE-USDT': ['2022-12-18 07:00:00', '2022-12-18 17:00:00'],
}

# kline_dir = Path(__file__).parent / 'data' / '币对分类K线'
kline_dir = Path(__file__).parent / 'data' / '币对分类K线_合成'
target_dir = Path(__file__).parent / 'data' / '币对上下线时间'
# print(f'kline目录: {kline_dir}')

if not target_dir.exists():
    target_dir.mkdir()

# 1. 遍历kline目录，获取所有币对的分钟K线
all_markets = set()
for file in kline_dir.iterdir():
    if file.suffix == '.csv':
        symbol = file.stem # 去除日期
        all_markets.add(symbol)

all_markets = sorted(list(all_markets))
print(f'共有{len(all_markets)}个交易对')

# 2. 读入分钟K线csv文件， 重采样成小时K线
result = []
for symbol in all_markets:
    df = pd.read_csv(kline_dir / f'{symbol}.csv', parse_dates=['candle_begin_time'], index_col=0)
    # df.sort_values(by=['candle_begin_time'], inplace=True, ascending=True)

    # 重采样成小时K线
    # df.set_index('candle_begin_time', inplace=True)
    df = df.resample('1H').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        'quote_volume': 'sum',
        'trade_num': 'sum',
        'taker_buy_base_asset_volume': 'sum',
        'taker_buy_quote_asset_volume': 'sum',
    })
    df.reset_index(inplace=True)
    df['candle_begin_time'] = df['candle_begin_time'].dt.tz_localize(None) # 去除时区信息

    min_time = df['candle_begin_time'].min()
    max_time = df['candle_begin_time'].max()
    _df = df.loc[df['volume'] == 0]

    # apply override
    for name, override in overrides.items():
        if name == 'DOWNTIME':
            start = override[0]
            end = override[1]
            _df = _df[ ~ ((_df['candle_begin_time'] >= start) & (_df['candle_begin_time'] <= end)) ]     
        elif name.endswith('USDT'):
            # 具体symbol的override
            if name == symbol:
                print(f'override {name} {override}')
                start = override[0]
                end = override[1]
                _df = _df[ ~ ((_df['candle_begin_time'] >= start) & (_df['candle_begin_time'] <= end)) ]     

    print(f'{symbol} 从 {min_time} 到 {max_time} 共有{df.shape[0]}条K线, {_df.shape[0]}条空K线')
    result.append([symbol, min_time, max_time, df.shape[0], _df.shape[0], find_continuous_segments(_df['candle_begin_time'].tolist())])

result = pd.DataFrame(result, columns=['symbol', 'min_time', 'max_time', 'total_kline_num', 'empty_kline_num', 'empty_kline_time'])
result.to_csv(target_dir / '币对上下线时间.csv', index=False)
