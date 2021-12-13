from download import *
from deal_data import *
from config import symbol_list as symbols, intervals
import tools
import mongo
import fetch_data
from multiprocessing import Process


def mult_process_run():
    process_list = []
    for sym in symbol_list:
        for interval in intervals:
            # fetch_data.fetch_data_main(sym, interval)
            p = Process(target=fetch_data.fetch_data_main,
                        args=(sym, interval))
            process_list.append(p)
            print('start process:', sym, interval)
            p.start()
            time.sleep(2)
    for p in process_list:
        p.join()
    print("process close")


def main():

    # 获取未存在数据库的kline需要新建的symbol
    symbol_list = tools.filter_new_collection(symbols)
    # 如果有新增的，就先执行下载
    if len(symbol_list) > 0:
        download_main(symbol_list)
        # 处理数据为feather格式
        deal_data_main()
        # 数据导入mongoDB
        tools.import_data_main(symbol_list)
    # 处理增量数据
    mult_process_run()

    # 如果最后一条时间 跟 现在的时间相差大于5分钟，则每3s拉一次 500根数据大概是8H


if __name__ == '__main__':
    main()
