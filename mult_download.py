import tools
import mongo
from config import symbol_list as symbols, intervals
from download import *
from deal_data import *


def mult_download_data():
    print("启动下载批量数据")
    # 获取未存在数据库的kline需要新建的symbol
    symbol_list, symbol_list_name = tools.filter_new_collection(symbols)
    # 如果有新增的，就先执行下载
    if len(symbol_list) > 0:
        download_main(symbol_list)
        # 导入输入到mongodb
        # replat _ to -
        symbol_list_name = [x.replace("_", "-") for x in symbol_list_name]
        deal_data_main(symbol_list_name)
        print("批量历史数据下载完成")


if __name__ == '__main__':
    mult_download_data()
