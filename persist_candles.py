from decimal import Decimal
from json import JSONEncoder

from binance.client import Client as BinanceClient
from bson import CodecOptions, Decimal128
from bson.codec_options import TypeRegistry, TypeCodec

from library import get_binance_klines, get_binance_interval_unit
from mongodb import mongo_client


def _default(self, obj):
    return getattr(obj.__class__, "to_json", _default.default)(obj)


class DecimalCodec(TypeCodec):
    python_type = Decimal    # the Python type acted upon by this type codec
    bson_type = Decimal128   # the BSON type acted upon by this type codec
    def transform_python(self, value):
        return Decimal128(value)
    def transform_bson(self, value):
        """Function that transforms a vanilla BSON type value into our custom type."""
        return value.to_decimal()

_default.default = JSONEncoder().default
JSONEncoder.default = _default

mongo_client

market = "ZRXBTC"
ticker = BinanceClient.KLINE_INTERVAL_8HOUR
_klines = get_binance_klines(market, ticker, get_binance_interval_unit(ticker), _mongo=True)

db = mongo_client.crypto
# collection = db.crypto
decimal_codec = DecimalCodec()
type_registry = TypeRegistry([decimal_codec])
codec_options = CodecOptions(type_registry=type_registry)
collection = db.get_collection('zrx8h', codec_options=codec_options)


def to_mongo(_kline):
    return {
        'start_time' : _kline.start_time,
        'opening' : _kline.opening,
        'closing' : _kline.closing,
        'highest' : _kline.highest,
        'lowest' : _kline.lowest,
        'volume' : _kline.volume,
        'btc_volume' : _kline.btc_volume,
        'time_str' : _kline.time_str
    }


def persist_kline(_kline):
    collection.insert_one({'kline': to_mongo(_kline), 'timestamp': _kline.start_time})


def get_last_db_record():
    # return collection.find_one(sort=[('_id', DESCENDING)])
    return collection.find_one({"timestamp":1594368000000})


def filter_current_klines(_klines):
    _last_record = get_last_db_record()
    return list(filter(lambda x: x.start_time>_last_record['timestamp'], _klines))


def persist_klines(_klines):
    return list(map(lambda x: persist_kline(x), _klines))


last = get_last_db_record()
current_klines = filter_current_klines(_klines)
l = persist_klines(current_klines)

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

cursor = collection.find()
for record in cursor:
    print(record)

i = 1
