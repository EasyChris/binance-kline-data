import mongo
import datetime
from tools import retry_wrapper
import ccxt
import pandas as pd
import time

# def find_last_data(conn, collection_name):
#     return db.data.find().sort('timestamp', -1).limit(1)


@retry_wrapper()
def fetch_binance_olhc(symbol, interval, since):
    exchange = ccxt.binance()
    data = exchange.fetch_ohlcv(
        symbol, interval, exchange.parse8601(str(since)))
    df = pd.DataFrame(
        data, columns=['candle_begin_time', 'open', 'high', 'low', 'close', 'volume'])
    # 转化candle_begin_time为datetime格式
    df['candle_begin_time'] = pd.to_datetime(
        df['candle_begin_time'], unit='ms')
    # df remove first row
    df = df.iloc[1:]
    return df


def get_diff_time(collection_name):
    last_data = mongo.get_last_data(collection_name)
    last_time = last_data[0]['candle_begin_time']
    # python get now datetime
    now_time = datetime.datetime.now()
    # 计算时间差
    time_diff = now_time - last_time
    # 计算时间差的秒数
    time_diff_minute = time_diff.total_seconds() / 60
    return time_diff_minute

# 根据interval获取对应的秒数


def get_interval_secend(interval):
    interval_time = int(interval[:1])
    if interval.endswith('m'):
        next = datetime.timedelta(minutes=interval_time)
    if interval.endswith('h'):
        next = datetime.timedelta(hours=interval_time)
    if interval.endswith('d'):
        next = datetime.timedelta(days=interval_time)
    since_time = next.total_seconds()
    return since_time


def fetch_data_main(symbol, interval):
    collection_name = symbol + '_' + interval
    db = mongo.get_mongo_conn()[collection_name]
    time_diff_minute = get_diff_time(collection_name)
    # 如果时间大于等于500，那么需要3秒拉一次，尽快拉到最新的数据
    while True:
        since = mongo.get_last_data(collection_name)[0]['candle_begin_time']
        print('fetch data start time', symbol, since, "\n")
        olhc_data = fetch_binance_olhc(symbol, interval, since)
        if not olhc_data.empty:
            db.insert_many(olhc_data.to_dict('records'))
        time_diff_minute = get_diff_time(collection_name)
        if time_diff_minute >= 500:
            print(symbol, interval, 'sleep 1s')
            time.sleep(1)
        else:
            # 计算下一次需要拉取的时间,用现在的时间加上间隔，为了减少并发
            sleep_time = get_interval_secend(interval)
            print(symbol, interval, 'sleep time', sleep_time)
            print("next fetch data time", datetime.datetime.now() +
                  datetime.timedelta(seconds=sleep_time), "\n")
            time.sleep(sleep_time)
