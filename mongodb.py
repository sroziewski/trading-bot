from pymongo import MongoClient

from config import config

mongo_user = config.get_parameter('mongo_user')
mongo_pass = config.get_parameter('mongo_pass')
mongo_ip = config.get_parameter('mongo_ip')

mongo_client = MongoClient(
    host='{}:27017'.format(mongo_ip),  # <-- IP and port go here
    serverSelectionTimeoutMS=3000,  # 3 second timeout
    username=mongo_user,
    password=mongo_pass
)
