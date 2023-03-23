from bson.codec_options import TypeRegistry, CodecOptions
from mongodb import mongo_client

from library import setup_logger, DecimalCodec


db_klines = mongo_client.klines

def min_max_scanner(_market_info_collection):
    _market_info_cursor = _market_info_collection.find()
    _market_info_list = [e for e in _market_info_cursor]
    _tickers = ['4h', '6h', '8h', '12h', '1d', '3d', '1w']
    for _market_s in _market_info_list:  # inf loop needed here
        for _ticker in _tickers:
            _klines = []
            _collection = db_klines.get_collection("{}_{}_{}".format(_market_s['name'], _market_info_collection.name, _ticker), codec_options=codec_options)
            _cursor = _collection.find().sort("_id", -1)
            for _e in _cursor:
                _klines.append(_e)
                if len(_klines) > 399:
                    break


filename = "Binance-Min-Max-Finder"
logger = setup_logger(filename)

db_markets_info = mongo_client.markets_info
db_journal = mongo_client.journal

decimal_codec = DecimalCodec()
type_registry = TypeRegistry([decimal_codec])
codec_options = CodecOptions(type_registry=type_registry)

btc_markets_collection = db_markets_info.get_collection("btc", codec_options=codec_options)
usdt_markets_collection = db_markets_info.get_collection("usdt", codec_options=codec_options)
busd_markets_collection = db_markets_info.get_collection("busd", codec_options=codec_options)

min_max_scanner(usdt_markets_collection)