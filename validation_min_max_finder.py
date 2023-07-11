import sys
import threading
from timeit import default_timer as timer
from typing import List

from bson import CodecOptions
from bson.codec_options import TypeRegistry

from library import get_pickled, round_price, DecimalCodec, lib_initialize, get_time, logger_global
from library import ticker2num
from min_max_finder import extract_buy_entry_setup, SetupEntry, to_offline_kline, manage_entry_computing, \
    sell_signal_tickers, start_logger
from mongodb import mongo_client

mode = sys.argv[2]

if mode == "local":
    path = "E:/bin/data/klines/start/"
elif mode == "gpu1":
    path = "/home/sroziewski/store/start/"
else:
    path = "/home/0agent1/store/klines/start/"
# path = "E:/bin/data/klines/start/"


db_klines = mongo_client.klines
db_setup = mongo_client.setup
decimal_codec = DecimalCodec()
type_registry = TypeRegistry([decimal_codec])
codec_options = CodecOptions(type_registry=type_registry)
threads_n = 5


# def extract_klines(_market, _type, _ticker):
#     _klines_online = get_klines("{}{}".format(_market, _type).upper(), _ticker)
#     _kline_collection = db_klines.get_collection("{}_{}_{}".format(_market, _type, _ticker),
#                                                  codec_options=codec_options)
#     try:
#         _kline_cursor = _kline_collection.find().sort("_id", -1)
#     except Exception:
#         pass
#
#     _klines_offline = []
#
#     for _e in _kline_cursor:
#         _klines_offline.append(_e)
#         if len(_klines_offline) > 399:
#             break
#
#     _klines_online.reverse()
#
#     _ii = 0
#     _diff = []
#     for _k in _klines_online:
#         if _k.start_time == _klines_offline[_ii]['kline']['start_time']:
#             break
#         _diff.append(_k)
#
#     _diff = list(map(lambda x: to_offline_kline(x), _diff))
#
#     return [*_diff, *_klines_offline]


def extract_klines(_cse):
    # _klines_online = get_klines("{}{}".format(_market, _type).upper(), _market, _ticker)
    _klines_online = get_klines(path, "{}{}".format(_cse.market, _cse.type), _cse.ticker)
    _r = list(map(lambda x: to_offline_kline(x), _klines_online[-800:][0:len(_klines_online[-800:])-_cse.index]))
    # print("{} {}".format(_cse.ticker, _r[-1]))
    return _r
    _kline_collection = db_klines.get_collection("{}_{}_{}".format(_cse.market, _cse.type, _cse.ticker), codec_options=codec_options)


class ProcessingEntry(object):
    def __init__(self, _market, _ticker):
        self.market = _market
        self.ticker = _ticker


class ComputingSetupEntry(object):
    def __init__(self, _market, _type, _ticker, i):
        self.market = _market
        self.type = _type
        self.ticker = _ticker
        self.se = None
        self.klines = None
        self.index = i

    def set_klines(self, klines):
        self.klines = klines


def manage_validation_processing(_pe):
    _crawler = threading.Thread(target=validate, args=(_pe,),
                                name='validate : {}')
    _crawler.start()
    logger_global[0].info("Thread for {}".format(_pe.ticker))
    return _crawler


def get_klines(_path, _market, _ticker):
    _fname = "{}_{}".format(_market, _ticker)
    _klines = get_pickled(_path, _fname)
    return _klines


def process_computing(_cse: ComputingSetupEntry):
    _klines = extract_klines(_cse)
    _se: SetupEntry = extract_buy_entry_setup(_klines, "{}{}".format(_cse.market, _cse.type).upper(), _cse.ticker)
    # _klines.clear()
    # if _se:
    #     _cse.se = _se


# def process_computing(_cse: ComputingSetupEntry):
#     _klines = extract_klines(_cse.market, _cse.type, _cse.ticker)
#     _se: SetupEntry = extract_buy_entry_setup(_klines, "{}{}".format(_cse.market, _cse.type).upper(), _cse.ticker)
#     _klines.clear()
#     if _se:
#         _cse.se = _se


def validate(_pe):
    _klines = list(map(lambda x: to_offline_kline(x), get_klines(path, _pe.market, _pe.ticker)))
    _klines.reverse()
    _length = len(_klines) - 400 - 1
    for _i in range(1210, _length):
        _data = _klines[_i:400 + _i]
        _se: SetupEntry = extract_buy_entry_setup(_data, _pe.market, _pe.ticker)
        if _i % 1000 == 0:
            logger_global[0].info("{} {}".format(_pe.ticker, _i))
        if _se:
            if abs(_data[0]['kline']['start_time'] - _se.time) < 2 * ticker2num(_se.ticker) * 60 * 60 * 1000:
                logger_global[0].info("{} {} {} {} {} signal_strength: {} buys_count {}".format(_pe.ticker, _i, _se.time_str,
                                                                                _data[0]['kline']['time_str'],
                                                                                round_price(_se.buy_price),
                                                                                _se.signal_strength, _se.buys_count))


lib_initialize()

market = sys.argv[1]
# ticker = sys.argv[3]

_market = market
_type = "usdt"


start_logger(market)


def append(_processors, _el):
    if len(list(filter(lambda x: x.is_alive(), _processors))) >= threads_n:
        [x.join() for x in _processors]
    _processors.append(_el)


showed_setups = {}


def show_setups(_setups: List[SetupEntry], _i):
    global showed_setups
    for _setup in _setups:
        _val = "{}-{}".format(_setup.time, _setup.ticker)
        if not _val in showed_setups:
            _from = _setup.time - 21 * ticker2num(_setup.ticker) * 60 * 60
            if _setup.buy_price == -1:
                logger_global[0].info("i: {} {} {} from ({} {}) to ({} {}) {} {} vfi: {}".format(_i, _setup.market, _setup.ticker, _from, get_time(_from), _setup.time, _setup.time_str, _setup.buys_count, _setup.buy_price, _setup.sell_vfi))
            else:
                logger_global[0].info(
                    "i: {} {} {} at ({} {}) {} {}".format(_i, _setup.market, _setup.ticker, _setup.time, _setup.time_str,
                                                                       _setup.buys_count, _setup.buy_price))
            showed_setups[_val] = 1


def extract_sell_setups(_setups_dict):
    _out = []
    for _ticker, _setup in _setups_dict.items():
        if _ticker in sell_signal_tickers:
            _out.append(_setup)
    return _out


#  new Approach

i1w = i3d = i1d = i12h = i8h = i6h = i4h = i2h = i1h = i30m = i15m = 0

# i15m = 10270
# i30m = 5159
# i1h = 2579
# i2h = 1289
# i4h = 644
# i6h = 430
# i8h = 322
# i12h = 215
# i12h = 215
# i1d = 107
# i3d = 35
# i1w = 15

_start = timer()

setups_dict = {}

# for i8h in range(0, 15*4*24*7*50):  # 10 weeks
#     _cses = []
#     _processors = []
#     _cse = ComputingSetupEntry(_market, _type, ticker, i8h)
#     _cses.append(_cse)
#     append(_processors, manage_entry_computing(_cse))
#     # process_computing(_cse)
#     [x.join() for x in _processors]
#     _setups = list(map(lambda y: y.se, filter(lambda x: x.se, _cses)))
#     show_setups(_setups, i8h)


for i in range(i15m, 15*4*24*7*80):  # 10 weeks
    _cses = []
    _processors = []
    if i % 672 == 0:
        _cse = ComputingSetupEntry(_market, _type, '1w', i1w)
        _cses.append(_cse)
        append(_processors, manage_entry_computing(_cse))
        # process_computing(_cse)
        i1w += 1

    if i % 288 == 0:
        _cse = ComputingSetupEntry(_market, _type, '3d', i3d)
        _cses.append(_cse)
        append(_processors, manage_entry_computing(_cse))
        # process_computing(_cse)
        i3d += 1

    if i % 96 == 0:
        _cse = ComputingSetupEntry(_market, _type, '1d', i1d)
        _cses.append(_cse)
        append(_processors, manage_entry_computing(_cse))
        # process_computing(_cse)
        i1d += 1
        logger_global[0].info("Computation time reported every 1d candle: {} min".format(round((timer() - _start) / 60)))

    if i % 48 == 0:
        _cse = ComputingSetupEntry(_market, _type, '12h', i12h)
        _cses.append(_cse)
        append(_processors, manage_entry_computing(_cse))
        # process_computing(_cse)
        i12h += 1

    if i % 32 == 0:
        _cse = ComputingSetupEntry(_market, _type, '8h', i8h)
        _cses.append(_cse)
        append(_processors, manage_entry_computing(_cse))
        # process_computing(_cse)
        i8h += 1

    if i % 24 == 0:
        _cse = ComputingSetupEntry(_market, _type, '6h', i6h)
        _cses.append(_cse)
        append(_processors, manage_entry_computing(_cse))
        # process_computing(_cse)
        i6h += 1

    if i % 16 == 0:
        _cse = ComputingSetupEntry(_market, _type, '4h', i4h)
        _cses.append(_cse)
        append(_processors, manage_entry_computing(_cse))
        # process_computing(_cse)
        i4h += 1

    if i % 8 == 0:
        _cse = ComputingSetupEntry(_market, _type, '2h', i2h)
        _cses.append(_cse)
        append(_processors, manage_entry_computing(_cse))
        # process_computing(_cse)
        i2h += 1

    if i % 4 == 0:
        _cse = ComputingSetupEntry(_market, _type, '1h', i1h)
        _cses.append(_cse)
        append(_processors, manage_entry_computing(_cse))
        # process_computing(_cse)
        i1h += 1

    if i % 2 == 0:
        _cse = ComputingSetupEntry(_market, _type, '30m', i30m)
        _cses.append(_cse)
        append(_processors, manage_entry_computing(_cse))
        # process_computing(_cse)
        i30m += 1

    _cse = ComputingSetupEntry(_market, _type, '15m', i)
    _cses.append(_cse)
    append(_processors, manage_entry_computing(_cse))
    # process_computing(_cse)

    [x.join() for x in _processors]
    _setups = list(map(lambda y: y.se, filter(lambda x: x.se, _cses)))
    show_setups(_setups, i)

    # if _setups:
    #     _sell_setups = extract_sell_setups(setups_dict)
    #     _setups = filter_by_sell_setups([*_setups, *_sell_setups], setups_dict)
    #     _setups = define_signal_strength([*_setups, *list(filter(lambda x: x.buy_price > 0, _sell_setups))])
    #     show_setups(_setups, i)





















