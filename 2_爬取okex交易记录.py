import os
import requests
import ccxt
import time
from pathlib import Path
from datetime import datetime

data_path = Path(__file__).parent / 'data' / 'aggtrades'
if not data_path.exists():
    data_path.mkdir()
# else:
#     print(f'目录已存在，清空目录: {data_path}')
#     for item in data_path.iterdir():
#         if item.is_file() or item.is_symlink():
#             item.unlink()
#         elif item.is_dir():
#             item.rmdir()


def fetch_offline_swap_aggtrades(active_ids):

    overrides = {
        # LUNA 20220513下线，20220528重新上线，需要下载重新上线前的历史数据
        'LUNA-USDT-SWAP': datetime.strptime('20220527', '%Y%m%d'),
    }

    # LUNA-USDT-SWAP需要下载重新上线前的历史数据
    for symbol in overrides.keys():
        if symbol in active_ids:
            active_ids.remove(symbol)

    base_url = "https://www.okx.com/priapi/v5/broker/public/v2/orderRecord"
    file_download_url = "https://static.okx.com/cdn/okex/traderecords/aggtrades/daily"

    # 获取所有有效日期
    date_response = requests.get(base_url, params={"path": "cdn/okex/traderecords/aggtrades/daily", "size": 1000})
    dates = date_response.json()["data"]['recordFileList']

    # 遍历日期
    for date_struct in dates:

        date = date_struct["fileName"]
        given_date = datetime.strptime(date, '%Y%m%d')

        # 选择只跑某个日期之前的文件
        # comparison_date = datetime.strptime('20211015', '%Y%m%d')
        # if given_date >= comparison_date:
        #     continue

        print(f'开始处理日期: {date}')
        # date_folder_url = f"{file_download_url}/{date}"

        # 获取该日期下的所有交易文件
        files_response = requests.get(base_url, params={"path": f"cdn/okex/traderecords/aggtrades/daily/{date}", "size": 1000})

        files = files_response.json()["data"]['recordFileList']

        # 遍历交易文件
        for file_struct in files:
            file_name = file_struct['fileName']

            # 检查是否是USDT swap
            if not ('USDT' in file_name and 'SWAP' in file_name):
                continue  # 跳过

            # 检查是否是active symbol
            if file_name.split('-aggtrades-')[0] in active_ids:
                continue

            # LUNA-USDT-SWAP需要下载重新上线前的历史数据
            if file_name.split('-aggtrades-')[0] in overrides.keys():
                override_date = overrides.get(file_name.split('-aggtrades-')[0])
                if given_date >= override_date:
                    continue

            # if file_name.split('-aggtrades-')[0] not in ['LUNA-USDT-SWAP']:
            #     continue

            file_url = f"{file_download_url}/{date}/{file_name}"
            # 构造本地保存路径
            local_path = os.path.join(data_path, f"{date}_{file_name}")

            if Path(local_path).exists():
                continue  # 跳过,不重新下载已经存在的文件

            # 下载文件
            retry_count = 0
            while True:
                try:
                    time.sleep(1)
                    file_response = requests.get(file_url)
                    break
                except Exception as e:
                    if retry_count > 5:
                        print(f'反复获取 {file_name} 失败,退出')
                        exit(1)
                    print(f'获取 {file_name} 失败重试: {e}')
                    time.sleep(30)
                    retry_count += 1
                    continue

            with open(local_path, 'wb') as file:
                # file_response = requests.get(file_url)
                file.write(file_response.content)

            print(f"Downloaded: {local_path}")

    print("Download complete.")


def get_usdt_margined_swap_markets(exchange):
    all_markets = exchange.markets
    usdt_swap_markets = [symbol for symbol in all_markets if all_markets[symbol]['swap'] and not all_markets[symbol]['inverse'] and all_markets[symbol]['quote'] == 'USDT']
    return usdt_swap_markets


if __name__ == '__main__':

    # 限速：20次/2s
    # GET /api/v5/market/history-candles
    # 20 requests per second for private endpoints with a 60-second timeout
    ok_exchange = ccxt.okex5(
        {
            'enableRateLimit': True,  # Enable rate limiting
            'rateLimit': 100,  # Number of requests per second
            'refetch': 60,  # Number of seconds before the order book is refetched (default: 60)
            # 'options': {
            #     'defaultType': 'swap',  # ←-------------- swap
            # }
        }
    )

    # 获取所有交易对
    ok_exchange.load_markets()
    usdt_swap = get_usdt_margined_swap_markets(ok_exchange)
    print(f'活跃的U本位合约数量: {len(usdt_swap)}')
    # print(f'U本位合约: {usdt_swap}')

    usdt_swap_id = [ok_exchange.market_id(symbol) for symbol in usdt_swap]
    # print(f'U本位合约: {usdt_swap_id}')
    fetch_offline_swap_aggtrades(usdt_swap_id)