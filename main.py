from config import *
import fetch_data
from multiprocessing import Process
import time


def mult_process_run():
    process_list = []
    for sym in symbol_list:
        for interval in intervals:
            # fetch_data.fetch_data_main(sym, interval)
            p = Process(target=fetch_data.fetch_data_main,
                        args=(sym, interval))
            process_list.append(p)
            print('start process:', sym, interval)
            time.sleep(SHORT_SLEEP_TIME)
            p.start()
    for p in process_list:
        p.join()
    print("process close")


def main():
    # 处理增量数据
    mult_process_run()


if __name__ == '__main__':
    main()
