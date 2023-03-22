import datetime

import numpy as np
import pandas as pd

from library import get_pickled

df = pd.read_csv('D:\\bin\\data\\BINANCE_AVAXUSDT_240.csv')

def nz(_x, _y=None):
    '''
    RETURNS
    Two args version: returns x if it's a valid (not NaN) number, otherwise y
    One arg version: returns x if it's a valid (not NaN) number, otherwise 0
    ARGUMENTS
    x (val) Series of values to process.
    y (float) Value that will be inserted instead of all NaN values in x series.
    '''
    if isinstance(_x, np.generic):
        return _x.fillna(_y or 0)
    if _x != _x:
        if _y is not None:
            return _y
        return 0
    return _x


def n1(_v):
    _v1 = _v.copy()
    for _i in range(len(_v1)):
        if _i > 0:
            _v1[_i] = _v[_i - 1]
    return _v1


def f(a,b):
    df['l0'] = (1 - b) * a
    df['l0_1'] = df['l0'].shift(1)

    return df['l0'] + b*df['l0_1']


def compute_tr(_data):
    return _data['high']-_data['low']


def get_crossup(_data, _lower_threshold_of_approximability2): # +1
    return np.logical_and(_data[1:]['low'] > _lower_threshold_of_approximability2[:-1], _data.iloc[:-1, :]['low'] <= _lower_threshold_of_approximability2[:-1])


def get_crossdn(_data, _upper_threshold_of_approximability2):
    return np.logical_and(_data[1:]['high'] < _upper_threshold_of_approximability2[:-1], _data.iloc[:-1, :]['high'] >= _upper_threshold_of_approximability2[:-1])


def smooth(_scalars, _weight=0.8):  # Weight between 0 and 1
    _l0 = np.zeros(len(_scalars))
    _l1 = np.zeros(len(_scalars))
    _l2 = np.zeros(len(_scalars))
    _l3 = np.zeros(len(_scalars))

    for _kk in range(len(_scalars)):
        if _kk == 0:
            _l0[_kk] = (1 - _weight) * _scalars[_kk]
            _l1[_kk] = -_weight * _l0[_kk]
            _l2[_kk] = -_weight * _l1[_kk]
            _l3[_kk] = -_weight * _l2[_kk]
        else:
            _l0[_kk] = (1 - _weight) * _scalars[_kk] + _weight * _l0[_kk - 1]
            _l1[_kk] = -_weight * _l0[_kk] + _l0[_kk - 1] + _weight * _l1[_kk - 1]
            _l2[_kk] = -_weight * _l1[_kk] + _l1[_kk - 1] + _weight * _l2[_kk - 1]
            _l3[_kk] = -_weight * _l2[_kk] + _l2[_kk - 1] + _weight * _l3[_kk - 1]

    return (_l0 + 2*_l1 + 2*_l2 + _l3)/6


def lele(_open, _close, _high, _low, _val, _strength):
    _bindex = np.zeros(len(_open))
    _sindex = np.zeros(len(_open))
    _ret = np.zeros(len(_open))
    for _ll in range(len(_open)):
        if _ll > 0:
            _bindex[_ll] = _bindex[_ll-1]
            _sindex[_ll] = _sindex[_ll-1]
        if _ll > 3:
            if _close[_ll] > _close[_ll-4]:
                _bindex[_ll] = _bindex[_ll] + 1
            if _close[_ll] < _close[_ll-4]:
                _sindex[_ll] = _sindex[_ll] + 1

        if _bindex[_ll] > _val and _close[_ll] < _open[_ll] and _high[_ll] >= np.max(_high[_ll-10:_ll]):
            _bindex[_ll] = 0
            _ret[_ll] = -1
        if _sindex[_ll] > _val and _close[_ll] > _open[_ll] and _low[_ll] <= np.min(_low[_ll-10:_ll]):
            _sindex[_ll] = 0
            _ret[_ll] = 1
    return _ret


def find_indices(_list_to_check, _item_to_find):
    _indices = []
    for _idx, _value in enumerate(_list_to_check):
        if _value == _item_to_find:
            _indices.append(_idx)
    return _indices


def compute_adjustment(_open, _close, _high, _low, _volume):
    _r = []
    for _kk in range(len(_open)):
        _e = 0 if _close[_kk] == _high[_kk] and _close[_kk] == _low[_kk] or _high[_kk] == _low[_kk] else ((2 * _close[_kk] - _low[_kk] - _high[_kk]) / (
                    _high[_kk] - _low[_kk])) * _volume[_kk]
        _r.append(_e)
    return _r


def compute_whale_money_flow(_adjustment, _volume, _money_strength):
    _wmf = []
    for _ii in range(len(_money_strength)):
        _wmf.append(np.sum(_adjustment[_ii:10+_ii]) / np.sum(_volume[_ii:10+_ii]) + _money_strength[_ii])
    return _wmf


def rsi(_upper, _lower):
    _r = []
    for _ii in range(len(_upper)):
        if _lower[_ii] == 0:
            _r.append(100.0)
        elif _upper[_ii] == 0:
            _r.append(1.0)
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


def compute_calculations(_open, _close, _high, _low, _volume, _ohlc=True):
    _adjustment = compute_adjustment(_open, _close, _high, _low, _volume)
    _trend_strength = []
    _toptrend0 = []
    _lower_trend0 = []
    _toptrend = []
    _lower_trend = []
    _ohlc4 = []
    for _ii in range(len(_open)):
        _trend_strength.append(np.sum(_adjustment[_ii:1 + _ii]) / np.sum(_volume[_ii:1 + _ii]))
        if _ohlc:
            _ohlc4.append((_open[_ii] + _close[_ii] + _high[_ii] + _low[_ii]) / 4)
        else:
            _ohlc4.append(_close[_ii])

    for _ii in range(len(_ohlc4) - 1):
        _toptrend0.append(_volume[_ii] * (0 if _ohlc4[_ii] - _ohlc4[_ii + 1] <= 0 else _ohlc4[_ii]))
        _lower_trend0.append(_volume[_ii] * (0 if _ohlc4[_ii] - _ohlc4[_ii + 1] >= 0 else _ohlc4[_ii]))

    for _ii in range(len(_toptrend0)):
        _toptrend.append(np.sum(_toptrend0[_ii:8 + _ii]))
        _lower_trend.append(np.sum(_lower_trend0[_ii:8 + _ii]))

    _trendline = rsi(_toptrend, _lower_trend)

    return _trend_strength, _trendline


def compute_trend_exhaustion(_open, _close, _high, _low, _volume):
    _trend_strength, _trendline = compute_calculations(_open, _close, _high, _low, _volume)
    _trend_strength2, _trendline2 = compute_calculations(_open, _close, _high, _low, _volume, _ohlc=False)
    _te = []
    for _ii in range(len(_trendline)):
        _te.append(_trendline[_ii] + _trend_strength2[_ii] + _trend_strength[_ii] / _trendline2[_ii])
    return _te


conjectures = list(map(lambda x: smooth(df['open'], x), np.arange(0.1, 1.0, 0.05)))
amlag = np.mean(conjectures, axis=0)
tr = compute_tr(df)
inapproximability = np.mean(list(map(lambda x: smooth(tr, x), np.arange(0.1, 1.0, 0.05))), axis=0)

Upper_Threshold_of_Approximability1 = amlag + inapproximability*1.618
Upper_Threshold_of_Approximability2 = amlag + 2*inapproximability*1.618
Lower_Threshold_of_Approximability1 = amlag - inapproximability*1.618
Lower_Threshold_of_Approximability2 = amlag - 2*inapproximability*1.618

crossup = get_crossup(df, Lower_Threshold_of_Approximability2)
crossdn = get_crossdn(df, Upper_Threshold_of_Approximability2)

_out = lele(df['open'], df['close'], df['high'], df['low'], 2, 20)
indexes =  find_indices(_out, True)

times =[]
for i in indexes:
    time_s = datetime.datetime.fromtimestamp(df['time'].iloc[i]).strftime('%d %B %Y %H:%M:%S')
    times.append((i, time_s))

j=0
for c in crossup:
    if c and Lower_Threshold_of_Approximability2[j]:
        time_s = datetime.datetime.fromtimestamp(df['time'].iloc[j]).strftime('%d %B %Y %H:%M:%S')
    j = j + 1


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
