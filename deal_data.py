from config import *
import time
import os
import pandas as pd
from tools import mkdir_dir, import_data_from_csv

# 修饰器


def cal_time(func):
    def _wrapper(*args, **kwargs):
        _start_time = time.time()
        result = func(*args, **kwargs)
        _end_time = time.time()
        print(f'{func.__name__}() 耗时 {round(_end_time - _start_time)}s\n')
        return result

    return _wrapper


def get_symbol_list(candle_path, symbol_list):
    """
    获取symbol_list
    """
    # 获取最新文件夹名
    folders = os.listdir(candle_path)
    # fitler with .csv
    folders = [folder for folder in folders if folder.endswith('.csv')]
    folders.sort()
    group_folder = []
    for symbol in symbol_list:
        folder_list = []
        for folder in folders:
            if symbol in folder:
                folder_list.append(folder)
        group_folder.append(folder_list)
    return group_folder


def merge_data(candle_path, feather_path, file_list):
    """
    合并文件
    """
    df_list = []
    for file in file_list:
        file_path = os.path.join(candle_path, file)
        df_data = pd.read_csv(file_path, parse_dates=['candle_begin_time'])
        import_data_from_csv(df_data, file)
    


@cal_time
def deal_data_main(symbol_list):
    print(f'开始处理数据...')
    mkdir_dir(feather_path)
    mkdir_dir(candle_path)
    group_list = get_symbol_list(candle_path, symbol_list)
    # remove empty list
    group_list = [x for x in group_list if x]
    for group in group_list:
        merge_data(candle_path, feather_path, group)
    print(f'处理数据完成...')


if __name__ == '__main__':
    deal_data_main()
