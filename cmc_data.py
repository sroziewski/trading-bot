import datetime
import json
import traceback
from json import JSONDecodeError
from time import sleep

from bson import CodecOptions
from bson.codec_options import TypeRegistry
from pymongo.errors import PyMongoError
from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects

from config import config
from library import setup_logger, DecimalCodec, get_time
from mongodb import mongo_client

logger = setup_logger("Crypto-Market-Global-Metrics")
cmc_key = config.get_parameter('cmc_key')


def to_mongo(_data):
    _timestamp = datetime.datetime.strptime(data['status']['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ').timestamp()
    _btc_dominance = _data['data']['btc_dominance']
    _eth_dominance = _data['data']['eth_dominance']
    _defi_volume_24h = _data['data']['defi_volume_24h']
    _defi_24h_percentage_change = _data['data']['defi_24h_percentage_change']
    _derivatives_volume_24h = _data['data']['derivatives_volume_24h']
    _derivatives_24h_percentage_change = _data['data']['derivatives_24h_percentage_change']
    _total_market_cap = _data['data']['quote']['USD']['total_market_cap']
    _total_volume_24h = _data['data']['quote']['USD']['total_volume_24h']
    _altcoin_volume_24h = _data['data']['quote']['USD']['altcoin_volume_24h']
    _altcoin_market_cap = _data['data']['quote']['USD']['altcoin_market_cap']
    return {
        'timestamp': _timestamp,
        'datetime': get_time(_timestamp),
        'btc_dominance': _btc_dominance,
        'eth_dominance': _eth_dominance,
        'defi_volume_24h': _defi_volume_24h,
        'defi_24h_percentage_change': _defi_24h_percentage_change,
        'derivatives_volume_24h': _derivatives_volume_24h,
        'derivatives_24h_percentage_change': _derivatives_24h_percentage_change,
        'total_market_cap': _total_market_cap,
        'total_volume_24h': _total_volume_24h,
        'altcoin_volume_24h': _altcoin_volume_24h,
        'altcoin_market_cap': _altcoin_market_cap,
    }


def persist_kline(_kline, _collection):
    try:
        _collection.insert_one({'kline': to_mongo(_kline), 'timestamp': _kline.start_time})
    except PyMongoError as err:
        traceback.print_tb(err.__traceback__)
        logger.exception("{} {}".format(_kline['market'], err.__traceback__))
        sleep(5)
        persist_kline(_kline, _collection)


url = 'https://pro-api.coinmarketcap.com/v1/global-metrics/quotes/latest'
parameters = {
    'start': '1',
    'limit': '5000',
    'convert': 'USD'
}
headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': cmc_key,
}

session = Session()
session.headers.update(headers)

db = mongo_client.market_data
decimal_codec = DecimalCodec()
type_registry = TypeRegistry([decimal_codec])
codec_options = CodecOptions(type_registry=type_registry)
collection = db.get_collection("cmc", codec_options=codec_options)

logger.info("Starting global market data crawling...")

while 1:
    try:
        response = session.get(url)
        data = json.loads(response.text)
        now = datetime.datetime.now().timestamp()
        collection.insert_one({'data': to_mongo(data), 'timestamp': now})
    except Exception as e:
        logger.error(e)
    sleep(900)
