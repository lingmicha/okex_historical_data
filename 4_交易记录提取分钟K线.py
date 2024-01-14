from pathlib import Path
import pandas as pd
import glob
from datetime import datetime, timedelta

# 1. 从aggtrades目录中提取所有下线的交易对名称
# 2. 对于每个交易对，用pandas读取所有的aggtrades文件，合并成一个文件
# 3. 利用pandas的重采样功能，resample成1分钟的K线
# 4. 保存到`币对分类K线`目录中

overrides = {
    'LUNA-USDT-SWAP': 'LUNA1-USDT-SWAP',
}

target_dir = Path(__file__).parent / 'data' / '币对分类K线_合成'
aggtrades_dir = Path(__file__).parent / 'data' / 'aggtrades'
# print(f'aggtrades目录: {aggtrades_dir}')

if not target_dir.exists():
    target_dir.mkdir()

# 1. 从aggtrades目录中提取所有下线的交易对名称
all_markets = set()
for date_folder in aggtrades_dir.iterdir():
    for file in date_folder.iterdir():
        if file.suffix == '.zip':
            if 'OLD' in file.stem:
                continue
            symbol = file.stem  # 去除日期
            symbol = symbol.split('-aggtrades-')[0] # 去除aggtrades
            all_markets.add(symbol)

# done = set(['ANC-USDT-SWAP', 'ASTR-USDT-SWAP', 'BABYDOGE-USDT-SWAP', 'BTM-USDT-SWAP', 'BTT-USDT-SWAP', 'BZZ-USDT-SWAP', 'CONV-USDT-SWAP', 'CQT-USDT-SWAP', 'DOME-USDT-SWAP', 'DORA-USDT-SWAP', 'EFI-USDT-SWAP'])
# all_markets = all_markets.difference(done)
all_markets = sorted(list(all_markets))
# all_markets = ['LUNA-USDT-SWAP']
print(f'共有{len(all_markets)}个交易对')
print(all_markets)
# exit(0)

# 2. 对于每个交易对，用pandas读取所有的aggtrades文件，合并成一个文件
for symbol in all_markets:
    print(f'处理 {symbol} ...')

    search_pattern = aggtrades_dir / '*' / f'{symbol}-aggtrades-*.zip'
    matching_files = glob.glob(search_pattern.as_posix())

    old_archive = [file for file in matching_files if 'OLD' in file]
    if len(old_archive) > 0:
        print(f'{symbol} 有OLD文件 {old_archive}')
        matching_files = [file for file in matching_files if 'OLD' not in file]

    matching_files.sort()
    print(f'{symbol} 共有{len(matching_files)}个文件')

    # 检查所有zip文件的日期是连续的
    check_dates = [(file.split('/')[-2]) for file in matching_files]
    check_dates = [datetime.strptime(date_str, '%Y%m%d') for date_str in check_dates]
    check_dates.sort()
    is_continuous = all(check_dates[i] + timedelta(days=1) == check_dates[i + 1] for i in range(len(check_dates) - 1))
    if not is_continuous:
        print(f'{symbol} 日期不连续')
        non_continuous_dates = []
        for i in range(len(check_dates) - 1):
            if check_dates[i] + timedelta(days=1) != check_dates[i + 1]:
                non_continuous_dates.append(check_dates[i] + timedelta(days=1))
        print(f'{symbol} 缺失日期: {non_continuous_dates}')
        # exit(1)

    # 将matching_files中的文件区分是否是20220101之前的
    # 20220101之前的文件没有header，之后的文件有header
    # 之前的文件需要手动加上header
    aggtrades_list = []
    columns = ['trade_id/撮合id','side/交易方向','size/数量','price/价格','created_time/成交时间']

    for file in matching_files:
        date_str = file.split('/')[-2]
        date = datetime.strptime(date_str, '%Y%m%d')
        if date < datetime(2022, 1, 1):
            aggtrades = pd.read_csv(file, compression='zip', encoding='gbk', header=None)
            aggtrades.columns = columns
            aggtrades_list.append(aggtrades)
        else:
            aggtrades = pd.read_csv(file, compression='zip', encoding='gbk')
            assert aggtrades.columns.tolist() == columns, f'{symbol} {file} 列名不匹配'
            aggtrades_list.append(aggtrades)

    aggtrades = pd.concat(aggtrades_list, axis=0)
    # print(aggtrades)

    # 3. 利用pandas的重采样功能，resample成1分钟的K线
    aggtrades['candle_begin_time'] = pd.to_datetime(aggtrades['created_time/成交时间'], unit='ms')
    aggtrades.sort_values('candle_begin_time', ascending=True, inplace=True)
    # 不能drop duplicates 因为有交易会同时发生
    # aggtrades.drop_duplicates(subset=['candle_begin_time'], keep='last', inplace=True)

    aggtrades['notional/金额'] = aggtrades['price/价格'] * aggtrades['size/数量']
    kline = aggtrades.resample('1min', on='candle_begin_time', closed='left' ).agg(
        open=('price/价格', 'first'),
        high=('price/价格', 'max'),
        low=('price/价格', 'min'),
        close=('price/价格', 'last'),
        volume=('size/数量', 'sum'),
        quote_volume=('notional/金额', 'sum'))

    # 检查k线是否完成，对于没有交易的时间点，填充为上一个时间的close
    kline['close'].ffill(inplace=True)
    kline['open'].fillna(kline['close'], inplace=True)
    kline['high'].fillna(kline['close'], inplace=True)
    kline['low'].fillna(kline['close'], inplace=True)

    assert kline['close'].isna().sum() == 0 \
        and kline['open'].isna().sum() == 0 \
        and kline['high'].isna().sum() == 0 \
        and kline['low'].isna().sum() == 0 \
        and kline['volume'].isna().sum() == 0 \
        and kline['quote_volume'].isna().sum() == 0, f'{symbol} K线数据不完整'

    kline['trade_num'] = 1
    kline['taker_buy_base_asset_volume'] = 1
    kline['taker_buy_quote_asset_volume'] = 1

    # 检查是否有缺失的K线
    # 获取第1分钟和最后一分钟的时间戳
    kline = kline.reset_index()
    start_time = kline['candle_begin_time'].min()
    end_time = kline['candle_begin_time'].max()

    # 生成完整的时间范围
    full_time_range = pd.date_range(start=start_time, end=end_time, freq='T')
    # 找到缺失的时间戳
    missing_timestamps = set(full_time_range) - set(kline['candle_begin_time'])

    if missing_timestamps:
        print(f"{symbol} 存在缺失的时间戳:", missing_timestamps)
        exit(1)

    # 4. 保存到`币对分类K线`目录中
    header = ['candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trade_num', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']
    kline = kline[header]

    if symbol in overrides.keys():
        print(f'{symbol} 重命名为 {overrides.get(symbol)}')
        symbol = overrides.get(symbol)

    symbol = symbol.replace('-SWAP', '')
    file_path = target_dir / f'{symbol}.csv'
    if file_path.exists():
        file_path.unlink()
    kline.to_csv(target_dir / f'{symbol}.csv', index=False)
