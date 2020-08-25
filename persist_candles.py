from decimal import Decimal
from json import JSONEncoder

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
    for schedule in schedules:
        market = schedule.market
        ticker = schedule.ticker
        klines = get_binance_klines(market, ticker, get_binance_interval_unit(ticker), _mongo=True)
        collection_name = schedule.ticker
        collection = db.get_collection(collection_name, codec_options=codec_options)
        logger.info("Crawling to collection {} ".format(collection_name))
        current_klines = filter_current_klines(klines, collection_name, collection)
        persist_klines(current_klines, collection)


schedules = [Schedule("ZRXBTC", 'zrx1d', BinanceClient.KLINE_INTERVAL_1DAY, 60 * 60 * 23),
             Schedule("ZRXBTC", 'zrx12h', BinanceClient.KLINE_INTERVAL_12HOUR, 60 * 60 * 11),
             Schedule("ZRXBTC", 'zrx8h', BinanceClient.KLINE_INTERVAL_8HOUR, 60 * 60 * 7),
             Schedule("ZRXBTC", 'zrx4h', BinanceClient.KLINE_INTERVAL_4HOUR, 60 * 60 * 3),
             Schedule("ZRXBTC", 'zrx1h', BinanceClient.KLINE_INTERVAL_1HOUR, 60 * (60 - 15)),
             Schedule("ZRXBTC", 'zrx30m', BinanceClient.KLINE_INTERVAL_30MINUTE, 60 * (30 - 20)),
             Schedule("ZRXBTC", 'zrx15m', BinanceClient.KLINE_INTERVAL_15MINUTE, 60 * (15 - 5)),
             ]

manage_crawling(schedules)

# thebytes = pickle.dumps(_klines[0])
# serverStatusResult = collection.command("serverStatus")
# _klines[0].time_str=_klines[0].time_str.encode('utf-8', 'strict')
# mp_rec1 = {
#         "kline":_klines[0],
#         "timestamp":_klines[0].time_str
#         }
# arr = []
# arr.append(mp_rec1)
# rec_id1 = collection.insert_many(arr)

i = 1
