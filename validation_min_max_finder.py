import threading

from bson import CodecOptions
from bson.codec_options import TypeRegistry

from library import get_pickled, round_price, DecimalCodec
from library import ticker2num
from min_max_finder import extract_buy_entry_setup, SetupEntry, to_offline_kline
from mongodb import mongo_client


path = "E:/data/binance/klines/usdt/"
db_klines = mongo_client.klines
db_setup = mongo_client.setup
decimal_codec = DecimalCodec()
type_registry = TypeRegistry([decimal_codec])
codec_options = CodecOptions(type_registry=type_registry)


def extract_klines(_market, _type, _ticker):
    _klines_online = get_klines("{}{}".format(_market, _type).upper(), _ticker)
    _kline_collection = db_klines.get_collection("{}_{}_{}".format(_market, _type, _ticker), codec_options=codec_options)
    try:
        _kline_cursor = _kline_collection.find().sort("_id", -1)
    except Exception:
        pass

    _klines_offline = []

    for _e in _kline_cursor:
        _klines_offline.append(_e)
        if len(_klines_offline) > 399:
            break


    _klines_online.reverse()

    _ii = 0
    _diff = []
    for _k in _klines_online:
        if _k.start_time == _klines_offline[_ii]['kline']['start_time']:
            break
        _diff.append(_k)

    _diff = list(map(lambda x: to_offline_kline(x), _diff))

    return [*_diff, *_klines_offline]

class ProcessingEntry(object):
    def __init__(self, _market, _ticker):
        self.market = _market
        self.ticker = _ticker


def manage_validation_processing(_pe):
    _crawler = threading.Thread(target=validate, args=(_pe,),
                                name='validate : {}')
    _crawler.start()
    print("Thread for {}".format(_pe.ticker))
    return _crawler


def get_klines(_path, _market, _ticker):
    _fname = "{}_{}".format(_market, _ticker)
    _klines = get_pickled(_path, _fname)
    return _klines


def validate(_pe):
    _klines = list(map(lambda x: to_offline_kline(x), get_klines(path, _pe.market, _pe.ticker)))
    _klines.reverse()
    _length = len(_klines) - 400 - 1
    for _i in range(1100, _length):
        _data = _klines[_i:400+_i]
        _se: SetupEntry = extract_buy_entry_setup(_data, _pe.market, _pe.ticker)
        if _i % 1000 == 0:
            print("{} {}".format(_pe.ticker, _i))
        if _se:
            if abs(_data[0]['kline']['start_time']-_se.time) < 2*ticker2num(_se.ticker)*60*60*1000:
                print("{} {} {} {} {}".format(_pe.ticker, _i, _se.time_str, _data[0]['kline']['time_str'], round_price(_se.buy_price)))


_tickers = ['15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d']

for _ticker in _tickers:
    # if _ticker == '4h':
    _pe = ProcessingEntry("avaxusdt", _ticker)
    manage_validation_processing(_pe)
    # extract_buy_entry_setup()

