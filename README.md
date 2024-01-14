# okex_historical_data
整理okex历史数据，方便回测使用


### 基本说明
- 本项目用于整理okex历史数据，方便回测使用
- 数据起始时间：2021-10-01 
- 数据截止时间：2023-11-22(第一次整理)/2024-01-14(第二次整理)


### 使用方式
1. 首次运行的时候，需要下载kline和aggtrades数据:
- `1_获取okex分数K线.py`:

    按照现有的交易对，下载这些交易对从开始到今天为止的所有分钟K线数据，保存在data/kline文件夹下

- `2_获取okex交易记录_并发.py`:

    下载不在现有交易对中的usdt永续合约的历史交易记录，保存在 data/aggtrades 文件夹下

- `3_分钟K线合成币对K线.py`:
    
    将1中下载的分钟K线数据，合成币对K线数据，保存在 data/币对分类K线 文件夹下

- `4_交易记录提取分钟K线`:

    将2中下载的交易记录，转换成币对分类的分钟K线数据,保存在 data/币对分类K线_合成 文件夹下；
    需要检查一下输出中是否有缺失的日期，和下文比对一下aggtrades缺失。

- `5_检查K线完整性.py`:

    大致检查一下aggtrades合成的K线中，没有成交的小时数

2. 设立pm2定时任务，每天运行获取当天交易永续合约分钟K线脚本：
-   `0_每日获取okex分钟K线.py`:

    获取当日活跃永续合约K线，存入data/kline文件夹下
-   `daily_kline.json`

    pm2的启动配置脚本，每天GMT+8 00:90:00运行`0_每日获取okex分钟K线.py`脚本

3. 之后合并增量数据，不需要再运行步骤1中的脚本，手动将增量的日期分类K线文件夹放入data/combine_kline文件夹下即可


### 数据说明
1. OKEX的交易数据，基本上从2023年开始可用:
    - 2023年以前尽管发送了大量的ticket，但是okex方面的回复经常是这个数据没有，或者干脆整个移除了某个交易对的存档；
    - 2022.12出现了阿里云宕机，导致okex有近3天交易不正常；
    - 网页存档的aggtrades数据，经常有缺失的情况，包括有交易对没有存档，以及存档的数据里面分钟K线不完整；
2. 下市又上市的币，下市前的名称需要更改成其他名称，目前有：
```
    overrides = {
        'LUNA-USDT-SWAP': 'LUNA1-USDT-SWAP',
    }
```
3. 也提供了合成币对K线的脚本，方便将起始-结束时间的K线合成币对K线，方便回测使用；但是为了方便回测增量导入，还是建议使用日期分类的K线数据；


### 数据整理记录
1. 2024-01-14 整理：
    - 分钟K线有165个交易对(从data/kline中的文件名提取)，包括了已经下线的`DASH-USDT-SWAP/XMR-USDT-SWAP/ZEC-USDT-SWAP/ZEN-USDT-SWAP`， K线都是完整和连续的(当然22年12月18日宕机的K线就无波动了);
    - 重新下载了所有aggtrades，有165个交易对，包括了已经下线的`DASH-USDT-SWAP/XMR-USDT-SWAP/ZEC-USDT-SWAP/ZEN-USDT-SWAP`
    - 

### 2023合约下线
```
DORA-USDT-SWAP    2023 年 8 月 24 日 16:00 (HKT)
ENJ-USDT-SWAP     2023 年 8 月 24 日 16:00 (HKT)
DOME-USDT-SWAP    2023 年 3 月 8 日 16:00 (HKT)
DODO-USDT-SWAP    2023 年 3 月 8 日 16:00 (HKT)
DASH-USDT-SWAP    2023 年 12 月 19 日 16:00 (HKT)
XMR-USDT-SWAP     2023 年 12 月 19 日 16:00 (HKT)
ZEC-USDT-SWAP     2023 年 12 月 19 日 16:00 (HKT)
ZEN-USDT-SWAP     2023 年 12 月 19 日 16:00 (HKT)
```


### aggtrades缺失
```
CQT-USDT-SWAP 缺失日期: [datetime.datetime(2022, 6, 29, 0, 0)]
LON-USDT-SWAP 缺失日期: [datetime.datetime(2022, 6, 29, 0, 0)]
WNXM-USDT-SWAP 缺失日期: [datetime.datetime(2022, 6, 29, 0, 0)]
BTT-USDT-SWAP 缺失日期: [datetime.datetime(2021, 12, 30, 0, 0)]
```