from os import error
import mongo
import datetime
from tools import retry_wrapper
import ccxt
import pandas as pd
import time
from config import SHORT_SLEEP_TIME

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
    interval_time = int(interval[:-1])
    if interval.endswith('m'):
        next = datetime.timedelta(minutes=interval_time)
    if interval.endswith('h'):
        next = datetime.timedelta(hours=interval_time)
    if interval.endswith('d'):
        next = datetime.timedelta(days=interval_time)
    since_time = next.total_seconds()
    return since_time


def fetch_data_main(symbol, interval, flag):
    time.sleep(flag)
    collection_name = symbol + '_' + interval
    db = mongo.get_mongo_conn()[collection_name]
    time_diff_minute = get_diff_time(collection_name)
    # 如果时间大于等于500，那么需要3秒拉一次，尽快拉到最新的数据
    while True:
        try:
            since = mongo.get_last_data(collection_name)[
                0]['candle_begin_time']
            print('拉取时间', symbol, interval, since, "\n")
            olhc_data = fetch_binance_olhc(symbol, interval, since)
            # df count rows
            count = olhc_data.shape[0]
            print("new data count:", count)
            if not olhc_data.empty:
                db.insert_many(olhc_data.to_dict('records'))
            time_diff_minute = get_diff_time(collection_name)
            if count >= 499:
                sleep_time = int(flag)
                print('------------------------------------------------------ \n')
                print(symbol, interval, f"sleep {sleep_time}", "\n")
                print("补充数据 - NEXT fetch time:", datetime.datetime.now() +
                      datetime.timedelta(seconds=sleep_time), "\n")
                print('------------------------------------------------------')
                time.sleep(sleep_time)
            else:
                # 计算下一次需要拉取的时间,用现在的时间加上间隔，为了减少并发
                sleep_time = get_interval_secend(interval) + int(flag)
                print('------------------------------------------------------ \n')
                print(symbol, interval, 'sleep time', sleep_time)
                print("NEXT fetch time:", datetime.datetime.now() +
                      datetime.timedelta(seconds=sleep_time), "\n")
                print('------------------------------------------------------')
                time.sleep(sleep_time)
        except error:
            print('------------------------------------------------------ \n')
            print(symbol, interval, 'sleep time', SHORT_SLEEP_TIME)
            print(error)
            time.sleep(SHORT_SLEEP_TIME)
            continue
