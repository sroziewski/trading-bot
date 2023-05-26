from math import log

import numpy as np
import pandas as pd

from library import get_pickled
from min_max_finder import create_from_offline_df, to_offline_kline

length = 130
coef = 0.2
vcoef = 2.5
signalLength = 5
smoothVFI = False


path = "D:/bin/data/klines/"


def get_klines(_path, _market, _ticker):
    _fname = "{}_{}".format(_market, _ticker)
    _klines = get_pickled(_path, _fname)
    return _klines


def compute_vinter(_df_dec):
    _typical = (_df_dec['close'] + _df_dec['high'] + _df_dec['low'])/3
    _inter = []
    for _ii in range(len(_typical)-1):
        _inter.append(log(_typical[_ii]) - log(_typical[_ii+1]))

    _std_dev = []
    for _ii in range(len(_inter)):
        _std_dev.append(np.std(_inter[_ii:_ii+30]))
    return _std_dev


def compute_vcp(_df_dec, _vinter):
    # cutoff = coef * _vinter * close
    _cutoff = []
    for _ii in range(len(_vinter)):
        _cutoff.append(coef * _df_dec['close'][_ii] * _vinter[_ii])
    _vave = _df_dec['volume'].iloc[::-1].rolling(length).mean().iloc[::-1].drop(axis=0, index=0).reset_index(drop=True)
    _vmax = _vave * vcoef
    _vc = []
    for _ii in range(len(_vmax)):
        _vc.append(_df_dec['volume'][_ii] if _df_dec['volume'][_ii] < _vmax[_ii] else _vmax[_ii])
    _typical = (_df_dec['close'] + _df_dec['high'] + _df_dec['low']) / 3
    _typical_1 = _typical.drop(axis=0, index=0).reset_index(drop=True)
    _mf = _typical - _typical_1

    # vcp = iff( mf > cutoff, vc, iff ( mf < -cutoff, -vc, 0 ) )
    _vcp = []
    for _ii in range(len(_cutoff)):
        if _mf[_ii] > _cutoff[_ii]:
            _vcp.append(_vc[_ii])
        elif _mf[_ii] < -_cutoff[_ii]:
            _vcp.append(-_vc[_ii])
        else:
            _vcp.append(0)
    return _vcp, _vave


def _compute_vfi(_vcp, _vave):
    _sum = []
    for _ii in range(len(_vcp)):
        _sum.append(np.sum(_vcp[_ii:_ii+length]) / _vave[_ii])
    return _sum


def compute_vfi(_klines_dec):
    _df_dec = create_from_offline_df(_klines)
    _vinter = compute_vinter(_df_dec)
    _vcp, _vave = compute_vcp(_df_dec, _vinter)

    return _compute_vfi(_vcp, _vave)

market = "adausdt"
ticker = '4h'
_klines = get_klines(path, market, ticker)
_klines = list(map(lambda x: to_offline_kline(x), get_klines(path, market, ticker)))
_klines.reverse()

_vfi = compute_vfi(_klines)

sdf = 1

