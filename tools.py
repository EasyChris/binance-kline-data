import os

import pymongo
from pymongo import collection
import mongo
from config import intervals, feather_path
import pandas as pd
import time


def get_all_timestamp(df_data):
    # 获得 timestamp 列表 转换成数组
    return df_data['candle_begin_time'].values.tolist()


def filter_exsit_data_mult(collection, df_data):
    timestamp_list = get_all_timestamp(df_data)
    res = collection.find({"candle_begin_time": {"$in": timestamp_list}})
    res_list = list(res)
    # 没有找到数据，直接全部插入
    if len(res_list) == 0:
        return df_data
    else:
        # 找到数据，过滤掉已经存在的数据
        exist_timestamp_list = []
        for item in res_list:
            exist_timestamp_list.append(item['candle_begin_time'])
        df_data = df_data[~df_data['candle_begin_time'].isin(
            exist_timestamp_list)]
        return df_data

# 重试装饰器


def retry_wrapper(act_name='', sleep_seconds=3, retry_times=5):
    def wrapper(func):
        def inner(*args, **kwargs):
            for _ in range(retry_times):
                try:
                    result = func(*args, **kwargs)
                    return result
                except Exception as e:
                    print(act_name, '出错，重试中...', e)
                    time.sleep(sleep_seconds)
            else:
                raise Exception('重试次数超过限制')
        return inner
    return wrapper


def mkdir_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

# 过滤不存在数据库的collection，不存在数据库，才需要重新下载


def filter_new_collection(symbols):
    # 获取mongodb的collection name
    collection_list = mongo.get_mongo_collection_name()
    new_symbols = []
    symbol_name_list = []
    for symbol in symbols:
        for interval in intervals:
            symbol_name = symbol + '_' + interval
            if symbol_name not in collection_list:
                symbol_name_list.append(symbol_name)
                new_symbols.append(symbol)
    print("开始下载", symbol_name_list)
    return new_symbols, symbol_name_list


def filter_urls_collection(urls):
    # 获取mongodb的collection name
    collection_list = mongo.get_mongo_collection_name()
    # replace collection_list _ to -
    collection_list = [x.replace('_', '-') for x in collection_list]
    new_urls = []
    for url in urls:
        if url not in collection_list:
            new_urls.append(url)
    return new_urls


def filter_groups_collection(groups):
    # 获取mongodb的collection name
    collection_list = mongo.get_mongo_collection_name()
    collection_list = [x.replace('_', '-') for x in collection_list]
    print(collection_list)
    new_groups = []
    for group in groups:
        g = group.split('-')
        g_name = g[0]+'-'+g[1]
        if g_name not in collection_list:
            new_groups.append(group)
    return new_groups

# 清理数据


def clean_data(old_data):
    # remove null data
    old_data.dropna(inplace=True, how='any', axis=0)
    old_data.drop_duplicates(
        subset=['candle_begin_time'], keep='last', inplace=True)
    return old_data


# 导入csv数据到mongodb
def import_data_from_csv(df, file_name):
    # read pandas feaether file
    print("import csv data", file_name)
    collection_name = file_name.split('-')[0]+'_'+file_name.split('-')[1]
    db = mongo.get_mongo_conn()[collection_name]
    # pymongo is collection exist
    if collection_name not in mongo.get_mongo_collection_name():
        db.create_index(
            [('candle_begin_time', pymongo.ASCENDING)], unique=True)
    df = clean_data(df)
    df = filter_exsit_data_mult(db, df)
    print(df.shape)
    db.insert_many(df.to_dict('records'))
    print(r"csv file: %s import success" % file_name)


# 导入feather数据到mongodb


def import_data_main(symbol_list):
    # get feather folder file
    feather_files = os.listdir(feather_path)
    deal_list = []
    for symbol in symbol_list:
        for feather in feather_files:
            if symbol in feather:
                # import data to mongodb
                deal_list.append(feather)
    # read pandas feaether file
    for feather_file in deal_list:
        feather_list = feather_file.split('-')
        print("import data", feather_list)
        collection_name = feather_list[0]+'_'+feather_list[1]
        db = mongo.get_mongo_conn()[collection_name]
        # pymongo is collection exist
        if collection_name not in mongo.get_mongo_collection_name():
            db.create_index(
                [('candle_begin_time', pymongo.ASCENDING)], unique=True)
        feather_file_path = os.path.join(feather_path, feather_file)
        print(feather_file_path)
        df = clean_data(pd.read_feather(feather_file_path))
        print('ddd', df)
        insert_num = 10000
        for i in range(0, len(df), insert_num):
            df_slice = df.iloc[i:i+insert_num]
            print(df_slice)
            # df_slice.to_mongo(db=db, collection=feather_list[2], upsert=True)
            db.insert_many(df_slice.to_dict('records'))

        # db.insert_many(df.to_dict(orient='records'))
        # print(r"feather file: %s import success" % feather_file)
