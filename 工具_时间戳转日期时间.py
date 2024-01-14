from pathlib import Path
import glob
from datetime import datetime, timedelta
import pandas as pd


root_path = Path(__file__).parent / "data" / "kline"
date_folders = sorted(glob.glob(str(root_path / "*")))

for date_folder in date_folders:
    files = sorted(glob.glob(str(Path(date_folder) / "*.csv")))
    sorted(files)
    for file in files:
        print(f"开始处理文件：{file}")
        df = pd.read_csv(file)
        df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'], unit='ms')
        df.to_csv(file, index=False)
