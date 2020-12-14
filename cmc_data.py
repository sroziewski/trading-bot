import datetime
import json
import traceback
from time import sleep, time

from bson import CodecOptions
from bson.codec_options import TypeRegistry
from pymongo.errors import PyMongoError
from requests import Session

from config import config
from library import setup_logger, DecimalCodec, get_time
from mongodb import mongo_client

from pytesseract import pytesseract

import cv2


logger = setup_logger("Crypto-Market-Global-Metrics")
cmc_key = config.get_parameter('cmc_key')


def get_trading_view_btdc():
    image = cv2.imread('/home/simon/btcd.png')
    image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    y=451
    x=188
    h=32
    w=100
    crop = image[y:y+h, x:x+w]
    pytesseract.tesseract_cmd = "tesseract"
    try:
        _btcd = float(pytesseract.image_to_string(crop))
    except ValueError as err:
        logger.exception(err.__traceback__)
        sleep(60)
        return get_trading_view_btdc()
    return _btcd if 0<_btcd<100 else None


def to_mongo(_data):
    _timestamp = datetime.datetime.strptime(_data['status']['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ').timestamp()
    _btc_dominance = round(_data['data']['btc_dominance'], 2)
    _eth_dominance = round(_data['data']['eth_dominance'], 2)
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
        'btc_dominance_trading_view': get_trading_view_btdc(),
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


def get_data(_url):
    response = session.get(_url)
    now = datetime.datetime.now().timestamp()
    collection.insert_one({'data': to_mongo(json.loads(response.text)), 'timestamp': now})


while 1:
    try:
        get_data(url)
    except Exception as e:
        logger.error(e)
        sleep(5)
        get_data(url)
    sleep(900)
