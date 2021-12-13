import os

# 加入你需要下载的币对，比如你要下载SOL和USDT 加入 “SOLUSDT”到symbol_list
symbol_list = ["SOLUSDT", "ETHUSDT", "BTCUSDT", "MINAUSDT"]
# 下载k线时间间隔，这里为1m，可以改成5m、15m、1h、1d等。
intervals = ["1m"]
# 需要下载数据等年份
years = ["2017", "2018", "2019", "2020", "2021"]
# 需要下载数据等月份
months = ["01", "02", "03", "04", "05",
          "06", "07", "08", "09", "10", "11", "12"]

# 以下配置不需要修改

cpus = os.cpu_count() * 2
filepath = r'./coin'


# format_data.py
candle_path = filepath  # ! h5 保存路径
feather_path = './feather'


# mongodb config
mongo_db_name = 'bianance_kline'
