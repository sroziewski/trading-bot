import threading
from decimal import Decimal
from random import randrange
from time import sleep

from binance.client import Client as BinanceClient
from bson import CodecOptions, Decimal128
from bson.codec_options import TypeRegistry, TypeCodec

from library import get_binance_klines, get_binance_interval_unit, setup_logger
from mongodb import mongo_client


class DecimalCodec(TypeCodec):
    python_type = Decimal  # the Python type acted upon by this type codec
    bson_type = Decimal128  # the BSON type acted upon by this type codec

    def transform_python(self, value):
        return Decimal128(value)

    def transform_bson(self, value):
        """Function that transforms a vanilla BSON type value into our custom type."""
        return value.to_decimal()


logger = setup_logger("Kline Crawl Manager")

db = mongo_client.crypto
decimal_codec = DecimalCodec()
type_registry = TypeRegistry([decimal_codec])
codec_options = CodecOptions(type_registry=type_registry)


def to_mongo(_kline):
    return {
        'start_time': _kline.start_time,
        'opening': _kline.opening,
        'closing': _kline.closing,
        'highest': _kline.highest,
        'lowest': _kline.lowest,
        'volume': _kline.volume,
        'btc_volume': _kline.btc_volume,
        'time_str': _kline.time_str
    }


def persist_kline(_kline, _collection):
    _collection.insert_one({'kline': to_mongo(_kline), 'timestamp': _kline.start_time})


def get_last_db_record(_collection):
    # return collection.find_one(sort=[('_id', DESCENDING)])
    return _collection.find_one({"timestamp": 1594368000000})


def filter_current_klines(_klines, _collection_name, _collection):
    _last_record = get_last_db_record(_collection)
    logger.info(
        "{} : last kline : {} ".format(_collection_name, _last_record['kline']['time_str'] if _last_record else "None"))
    _out = None
    if _last_record:
        _out = list(filter(lambda x: x.start_time > _last_record['timestamp'], _klines))
    else:
        _out = _klines
        logger.info("{} : first kline : {} ".format(_collection_name, _out[0].time_str if _out else "None"))
    return _out


def persist_klines(_klines, _collection):
    return list(map(lambda x: persist_kline(x, _collection), _klines))


class Schedule(object):
    def __init__(self, _market, _collection_name, _ticker, _sleep):
        self.market = _market
        self.collection_name = _collection_name
        self.ticker = _ticker
        self.sleep = _sleep


def manage_crawling(_schedules):
    for _schedule in _schedules:
        _scheduler = threading.Thread(target=do_schedule, args=(_schedule,), name='_do_schedule : {}'.format(_schedule.collection_name))
        _scheduler.start()


def do_schedule(_schedule):
    market = _schedule.market
    ticker = _schedule.ticker
    collection_name = _schedule.collection_name
    collection = db.get_collection(collection_name, codec_options=codec_options)
    while True:
        if ticker == BinanceClient.KLINE_INTERVAL_30MINUTE or BinanceClient.KLINE_INTERVAL_30MINUTE:
            sleep(randrange(20))
        else:
            sleep(randrange(200))
        klines = get_binance_klines(market, ticker, get_binance_interval_unit(ticker))
        logger.info("Crawling to collection {} ".format(collection_name))
        current_klines = filter_current_klines(klines, collection_name, collection)
        persist_klines(current_klines, collection)
        sleep(_schedule.sleep)


def get_schedules(_asset):
    return [
        Schedule("{}BTC".format(_asset.upper()), '{}1d'.format(_asset), BinanceClient.KLINE_INTERVAL_1DAY, 60 * 60 * 23),
        Schedule("{}BTC".format(_asset.upper()), '{}12h'.format(_asset), BinanceClient.KLINE_INTERVAL_12HOUR, 60 * 60 * 11),
        Schedule("{}BTC".format(_asset.upper()), '{}8h'.format(_asset), BinanceClient.KLINE_INTERVAL_8HOUR, 60 * 60 * 7),
        Schedule("{}BTC".format(_asset.upper()), '{}4h'.format(_asset), BinanceClient.KLINE_INTERVAL_4HOUR, 60 * 60 * 3),
        Schedule("{}BTC".format(_asset.upper()), '{}1h'.format(_asset), BinanceClient.KLINE_INTERVAL_1HOUR, 60 * (60 - 15)),
        Schedule("{}BTC".format(_asset.upper()), '{}30m'.format(_asset), BinanceClient.KLINE_INTERVAL_30MINUTE, 60 * (30 - 20)),
        Schedule("{}BTC".format(_asset.upper()), '{}15m'.format(_asset), BinanceClient.KLINE_INTERVAL_15MINUTE, 60 * (15 - 5)),
    ]


schedules = get_schedules("coti")

manage_crawling(schedules)


