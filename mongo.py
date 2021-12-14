import pymongo
from pymongo import mongo_client
import datetime as dt
host = 'localhost'
port = 28018
username = 'admin'
password = 'admin'
database_name = 'binance_kline'
# client mongoclinet with username and password
mongo_client = pymongo.MongoClient(
    f"mongodb://{username}:{password}@localhost:{port}/", connect=False)


def get_mongo_conn(db=database_name):
    """
    use me like this:

    collection = get_mongo_conn()["collection_name"]
    collection.insert({})
    """
    return mongo_client[db]


def get_mongo_data(collection_name, limit=10):
    """
    use me like this:

    collection = get_mongo_conn()["collection_name"]
    collection.insert({})
    """
    collection = get_mongo_conn()[collection_name]
    # find timestamp > today
    today_start_timestamp = dt.datetime(
        year, month, day, tzinfo=dt.timezone(utc_offset)).timestamp()
    print(today_start_timestamp)
    return collection.find({"timestamp": {"$gt": today_start_timestamp}})


def get_mongo_collection_name():
    db = get_mongo_conn()
    collection = db.list_collection_names()
    collection_list = []
    for c in collection:
        collection_list.append(c)
    return collection_list
# db = client.test


def get_last_data(collection_name):
    """
    use me like this:

    collection = get_mongo_conn()["collection_name"]
    collection.insert({})
    """
    collection = get_mongo_conn()[collection_name]
    return list(collection.find().sort("candle_begin_time", -1).limit(1))


# collection = db.articles

# post = {
#     'title': 'MongoDB and Python',
#     "author": 'chris',
#     "id": 18,
#     "isshow": True,
#     "tags": ['mongodb', 'python', 'pymongo']
# }


# result = collection.insert_one(post)

# print(result)
