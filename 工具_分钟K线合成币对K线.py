from pathlib import Path
import pandas as pd


# 1. 从kline目录下获取所有存在K线的币对
# 2. 对某个币对，遍历所有日期的文件夹，检查是否连续的存在某个币的K线
# 3. 读取这个币对从最早到最近日期的所有kline文件，合并成一个文件
# 4. 检查是否有缺失的K线
# 5. 保存到`币对分类K线`目录中

target_dir = Path(__file__).parent / 'data' / '币对分类K线'
kline_dir = Path(__file__).parent / 'data' / 'kline'
# print(f'kline目录: {kline_dir}')

if not target_dir.exists():
    target_dir.mkdir()

# 1. 从kline目录下获取所有存在K线的币对
all_markets = set()
for date_dir in kline_dir.iterdir():
    for file in date_dir.iterdir():
        if file.suffix == '.csv':
            symbol = file.stem.split('.')[0] # 去除日期
            all_markets.add(symbol)

all_markets = sorted(list(all_markets))
print(f'共有{len(all_markets)}个交易对')
# print(all_markets)

# 2. 对某个币对，遍历所有日期的文件夹，检查是否连续的存在某个币的K线
for symbol in all_markets:
    print(f'处理 {symbol} ...')

    file_path = target_dir / f'{symbol}.csv'
    if file_path.exists():
        print(f'{symbol} 已经存在')
        continue

    kline_paths = []
    date_dirs = [date_dir for date_dir in kline_dir.iterdir() if date_dir.is_dir()]
    for date_dir in date_dirs:
        file = date_dir / f'{symbol}.csv'
        if file.exists():
            kline_paths.append(file.as_posix())

    print(f'{symbol} 共有{len(kline_paths)}个文件')

    # 3. 读取这个币对从最早到最近日期的所有kline文件，合并成一个文件
    kline_list = [pd.read_csv(file, parse_dates=['candle_begin_time']) for file in kline_paths]
    kline = pd.concat(kline_list, axis=0)

    # 此处只需要返回一个naive的datetime，不需要指明utc时区,写出的时候datetime也不指定时区
    # kline['candle_begin_time'] = pd.to_datetime(kline['candle_begin_time'], unit='ms')
    
    kline.sort_values(by=['candle_begin_time'], inplace=True, ascending=True)
    print(f'{symbol} 共有{kline.shape[0]}条K线')
    # df.drop_duplicates(inplace=True)
    # df.sort_values(by=['open_time'], inplace=True)
    # df.reset_index(drop=True, inplace=True)

    # 4. 检查是否有缺失的K线
    # 获取第1分钟和最后一分钟的时间戳
    start_time = kline['candle_begin_time'].min()
    end_time = kline['candle_begin_time'].max()

    # 生成完整的时间范围
    full_time_range = pd.date_range(start=start_time, end=end_time, freq='T')
    # 找到缺失的时间戳
    missing_timestamps = set(full_time_range) - set(kline['candle_begin_time'])

    if missing_timestamps:
        print(f"{symbol} 存在缺失的时间戳:", missing_timestamps)
        exit(1)
    # 5. 保存到`币对分类K线`目录中

    kline = kline.reset_index()
    header = ['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trade_num', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']
    kline = kline[header]

    file_path = target_dir / f'{symbol}.csv'
    if file_path.exists():
        file_path.unlink()
    kline.to_csv(target_dir / f'{symbol}.csv', index=False)

print('完成')
exit(0)
