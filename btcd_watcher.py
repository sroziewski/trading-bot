import datetime
import json
import sys
import urllib
from time import sleep

from bson import CodecOptions
from bson.codec_options import TypeRegistry
from requests import Session

from config import config
from library import setup_logger, DecimalCodec, authorize, send_mail
from mongodb import mongo_client

logger = setup_logger("BTC-Dominance-Watcher")
cmc_key = config.get_parameter('cmc_key')



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

arguments = len(sys.argv) - 1
if arguments != 2:
    print("You have to specify type the level of BTC.D and type-of-break you want to watch...)")
    exit(0)
logger.info("Starting global market data crawling...")


def get_current_cmc_cap(_url):
    response = session.get(_url)
    _data = json.loads(response.text)
    return _data['data']['btc_dominance']


def get_data(_url):
    response = session.get(_url)
    now = datetime.datetime.now().timestamp()
    collection.insert_one({'data': get_current_cmc_cap(json.loads(response.text)), 'timestamp': now})


def notify_when_break_up(_url, _level):
    _btcd = round(get_current_cmc_cap(_url), 3)
    if _btcd > _level:
        send_mail(f"ZZZ BTC.D level {_level} BREAK UP ZZZ", f"Current BTC.D : {_btcd} > observed : {_level}")


def notify_when_break_down(_url, _level):
    _btcd = get_current_cmc_cap(_url)
    if _btcd < _level:
        send_mail(f"ZZZ BTC.D level {_level} BREAK DOWN ZZZ", f"Current BTC.D : {_btcd} < observed : {_level}")


def manage_notification(_url, _level, _type):
    if _type == "up":
        notify_when_break_up(_url, _level)
    elif _type == "down":
        notify_when_break_down(_url, _level)


def validate_args(_args):
    _level = float(sys.argv[1])
    _type = sys.argv[2]
    assert 10 < _level < 90
    assert _type == "up" or _type == "down"
    logger.info(f"All validations done : btcd : {btcd_level} type : {_type}")


def get_line(_btcd_open1, _btcd_open2, _dt):
    _b = _btcd_open1
    _a = (_btcd_open2 - _btcd_open1)/_dt
    return _a, _b


def break_line(_url, _btcd_open1, _btcd_open2, _dt, _type):
    _a, _b = get_line(_btcd_open1, _btcd_open2, _dt)
    _btcd = round(get_current_cmc_cap(_url), 3)

    _res = False
    if _type == "down":
        _res = True if 0 < _a * (_dt + 1) + _b - _btcd else False
    else:
        _res = True if 0 > _a * (_dt + 1) + _b - _btcd else False

    return _res


# authorize()



btcd_level = float(sys.argv[1])
breakout_type = sys.argv[2]

# break_line(url, 65.26, 65.41, 3, "down")
break_line(url, 65.37, 65.47, 8, "up")

validate_args(sys.argv)

logger.info(f"BTC.D level to watch : {btcd_level}")

while 1:
    try:
        manage_notification(url, btcd_level, breakout_type)
    except Exception as e:
        logger.error(e)
        sleep(5)
        manage_notification(url, btcd_level, breakout_type)
    sleep(1800)



