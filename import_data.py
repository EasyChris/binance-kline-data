import pandas as pd
import mongo
import os
from config import feather_path


def import_data_main():

    collection_list = mongo.get_mongo_collection_name()
    for index, collection, in enumerate(collection_list):
        print(index, collection)
    select_db = input("Select data to import: ")
    collection_name = collection_list[int(select_db)]
    c = mongo.get_mongo_conn()[collection_name]
    # mongo db find all
    data = c.find()
    df = pd.DataFrame(list(data))
    # # 去重、排序
    df.drop_duplicates(subset=['candle_begin_time'], keep='last', inplace=True)
    df.sort_values('candle_begin_time', inplace=True)
    df.reset_index(drop=True, inplace=True)
    df.drop(['_id'], axis=1, inplace=True)
    file_name = collection_list[int(select_db)] + '.feather'
    # # save to feather
    df.to_feather(os.path.join(feather_path, file_name))
    print(f"{file_name}导出成功完成")


if __name__ == '__main__':
    import_data_main()
