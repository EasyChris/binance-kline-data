import os
# download.py
cpus = os.cpu_count() * 2
filepath = r'./coin'
symbol_list = ["SOLUSDT", "ETHUSDT", "BTCUSDT", "MINAUSDT"]
intervals = ["1m"]
years = ["2017", "2018", "2019", "2020", "2021"]
months = ["01", "02", "03", "04", "05",
          "06", "07", "08", "09", "10", "11", "12"]

# years = ["2021"]
# months = ["11", "12"]

# format_data.py
candle_path = filepath  # ! h5 保存路径
feather_path = './feather'


# mongodb config
mongo_db_name = 'bianance_kline'
