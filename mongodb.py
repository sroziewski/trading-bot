from pprint import pprint

from pymongo import MongoClient

from library import config

mongo_user = config.get_parameter('mongo_user')
mongo_pass = config.get_parameter('mongo_pass')

mongo_client = MongoClient(
    host='144.91.75.39:27017',  # <-- IP and port go here
    serverSelectionTimeoutMS=3000,  # 3 second timeout
    username=mongo_user,
    password=mongo_pass
)
