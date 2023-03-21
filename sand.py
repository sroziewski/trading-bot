import datetime

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from bson.codec_options import TypeRegistry, CodecOptions

from library import DecimalCodec, save_to_file, get_pickled
from mongodb import mongo_client


def nz(x, y=None):
    '''
    RETURNS
    Two args version: returns x if it's a valid (not NaN) number, otherwise y
    One arg version: returns x if it's a valid (not NaN) number, otherwise 0
    ARGUMENTS
    x (val) Series of values to process.
    y (float) Value that will be inserted instead of all NaN values in x series.
    '''
    if isinstance(x, np.generic):
        return x.fillna(y or 0)
    if x != x:
        if y is not None:
            return y
        return 0
    return x


def compute_adjustment(_open, _close, _high, _low, _volume):
    _r = []
    for _kk in range(len(_open)):
        _e = 0 if _close[_kk] == _high[_kk] and _close[_kk] == _low[_kk] or _high[_kk] == _low[_kk] else ((2 * _close[_kk] - _low[_kk] - _high[_kk]) / (
                    _high[_kk] - _low[_kk])) * _volume[_kk]
        _r.append(_e)
    return _r


def compute_whale_money_flow(_adjustment, _volume, _money_strength):
    _whf = []
    for _ii in range(len(_money_strength)):
        _whf.append(np.sum(_adjustment[_ii:10+_ii]) / np.sum(_volume[_ii:10+_ii]) + _money_strength[_ii])
    return _whf


def rsi(_upper, _lower):
    _r = []
    for _ii in range(len(_upper)):
        if _lower[_ii] == 0:
            _r.append(100.0)
        elif _upper[_ii] == 0:
            _r.append(0.0)
        else:
            _r.append(100.0 - (100.0 / (1.0 + _upper[_ii] / _lower[_ii])))
    return _r


def compute_money_strength(_close, _volume):
    _upper0 = []
    _lower0 = []
    _upper = []
    _lower = []
    for _ii in range(len(_close) - 1):
        _upper0.append(_volume[_ii] * (0 if _close[_ii] - _close[_ii+1] <= 0 else _close[_ii]))
        _lower0.append(_volume[_ii] * (0 if _close[_ii] - _close[_ii+1] >= 0 else _close[_ii]))

    for _ii in range(len(_upper0)):
        _upper.append(np.sum(_upper0[_ii:14 + _ii]))
        _lower.append(np.sum(_lower0[_ii:14 + _ii]))

    return rsi(_upper, _lower)


df = pd.read_csv('D:\\bin\\data\\BINANCE_AVAXUSDT_240.csv')

db_klines = mongo_client.klines
decimal_codec = DecimalCodec()
type_registry = TypeRegistry([decimal_codec])
codec_options = CodecOptions(type_registry=type_registry)
avax_usdt_collection = db_klines.get_collection("avax_usdt_4h", codec_options=codec_options)

# avax_usdt_cursor = avax_usdt_collection.find().sort("_id", -1)
#
# avax_klines = []
#
# for _e in avax_usdt_cursor:
#     avax_klines.append(_e)
#     if len(avax_klines) > 499:
#         break
#
# save_to_file('D:\\bin\\data\\', "avax_usdt_4h", avax_klines)

avax_klines = get_pickled('D:\\bin\\data\\', "avax_usdt_4h")

open = list(map(lambda x: x['kline']['opening'], avax_klines))
close = list(map(lambda x: x['kline']['closing'], avax_klines))
high = list(map(lambda x: x['kline']['highest'], avax_klines))
low = list(map(lambda x: x['kline']['lowest'], avax_klines))
volume = list(map(lambda x: x['kline']['volume'], avax_klines))
times = list(map(lambda x: x['kline']['time_str'], avax_klines))

adjustment = compute_adjustment(open, close, high, low, volume)
money_strength = compute_money_strength(close, volume)
whale_money_flow = compute_whale_money_flow(adjustment, volume, money_strength)

k = 1
