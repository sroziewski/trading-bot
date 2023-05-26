import threading
import traceback
from math import log
from random import randrange
from time import sleep
from timeit import default_timer as timer

import numpy as np
import pandas as pd
from bson.codec_options import TypeRegistry, CodecOptions

from library import setup_logger, DecimalCodec, get_pickled, get_time_from_binance_tmstmp, try_get_klines, \
    get_binance_interval_unit, get_binance_klines, Kline, lib_initialize, get_time, round_price, ticker2num
from mongodb import mongo_client
from tb_lib import compute_tr, smooth, get_crossup, get_crossdn, lele, get_strong_major_indices, get_major_indices, \
    compute_adjustment, compute_money_strength, compute_whale_money_flow, compute_trend_exhaustion

db_klines = mongo_client.klines
db_setup = mongo_client.setup
decimal_codec = DecimalCodec()
type_registry = TypeRegistry([decimal_codec])
codec_options = CodecOptions(type_registry=type_registry)

filename = "Binance-Min-Max-Finder"
logger = setup_logger(filename)


def create_from_online_df(_klines):
    _open = list(map(lambda x: x.opening, _klines))
    _close = list(map(lambda x: x.closing, _klines))
    _high = list(map(lambda x: x.highest, _klines))
    _low = list(map(lambda x: x.lowest, _klines))
    _volume = list(map(lambda x: x.volume, _klines))
    _time = list(map(lambda x: x.start_time, _klines))
    _time_str = list(map(lambda x: x.time_str, _klines))

    return pd.DataFrame(list(zip(_open, _close, _high, _low, _volume, _time, _time_str)),
                      columns=['open', 'close', 'high', 'low', 'volume', 'time', 'time_str'])


def extract_volume(_kline):
    if 'volume' in _kline:
        return _kline['volume']
    if 'quantity' in _kline:
        return _kline['quantity']


def create_from_offline_df(_klines):
    _open = list(map(lambda x: x['kline']['opening'], _klines))
    _close = list(map(lambda x: x['kline']['closing'], _klines))
    _high = list(map(lambda x: x['kline']['highest'], _klines))
    _low = list(map(lambda x: x['kline']['lowest'], _klines))
    _volume = list(map(lambda x: extract_volume(x['kline']), _klines))
    _time = list(map(lambda x: x['kline']['start_time'], _klines))
    _time_str = list(map(lambda x: x['kline']['time_str'], _klines))

    return pd.DataFrame(list(zip(_open, _close, _high, _low, _volume, _time, _time_str)),
                      columns=['open', 'close', 'high', 'low', 'volume', 'time', 'time_str'])


def to_offline_kline(_kline: Kline):
    return {
        'kline': {
            'opening': _kline.opening,
            'closing': _kline.closing,
            'highest': _kline.highest,
            'lowest': _kline.lowest,
            'volume': _kline.volume,
            'start_time': int(_kline.start_time/1000),
            'time_str': _kline.time_str,
        }
    }


def filter_buys_trend_exhaustion(_trend_exhaustion, _buys):
    _r = []
    for _buy in _buys:
        _ind = len(_trend_exhaustion) - _buy
        if _trend_exhaustion[_ind] < 20.0 or any(filter(lambda x: x < 5.0, _trend_exhaustion[_ind:_ind + 20])):  # 20 bars before
            _r.append(_buy)
    return _r


def filter_buys_whale_money_flow(_whale_money_flow, _buys):
    _r = []
    for _buy in _buys:
        _ind = len(_whale_money_flow) - _buy
        if _whale_money_flow[_ind] < 40.0 or any(filter(lambda x: x < 20.0, _whale_money_flow[_ind:_ind + 20])):
            _r.append(_buy)
    return _r


def get_time_buys(_buys, _df_inc):
    return list(map(lambda x: _df_inc['time'][x], _buys))


def extract_order_price(_buys, _df_inc):
    return (_df_inc['open'][_buys[-1]]+_df_inc['close'][_buys[-1]]+_df_inc['high'][_buys[-1]]+_df_inc['low'][_buys[-1]])/4 if len(_buys) > 0 else False


class SetupEntry(object):
    def __init__(self, _market, _buy_price, _buys_count, _ticker, _time):
        self.market = _market
        self.buy_price = round_price(_buy_price)
        self.buys_count = _buys_count
        self.ticker = _ticker
        self.time = int(_time)
        self.time_str = get_time(_time)
        self.signal_strength = None
        self.sell_signal = {}  # (ticker, index)

    def set_signal_strength(self, _signal_strength):
        self.signal_strength = _signal_strength


def to_mongo(_se: SetupEntry):
    return {
        'market': _se.market.upper(),
        'buy_price': round_price(_se.buy_price),
        'buys_count': _se.buys_count,
        'ticker': _se.ticker,
        'signal_strength': _se.signal_strength,
        'time': int(_se.time/1000),
        'time_str': get_time(_se.time),
    }


def get_klines(_market, _ticker):
    try:
        _klines = try_get_klines("binance", _market, _ticker,
                                get_binance_interval_unit(_ticker, "strategy"))
    except Exception as err:
        traceback.print_tb(err.__traceback__)
        logger.exception("{} {} {}".format(_market, _ticker, err.__traceback__))
        sleep(randrange(30))
        _klines = get_binance_klines(_market, _ticker, get_binance_interval_unit(_ticker, "strategy"))

    return _klines


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


def get_delta_t(_ticker):
    if _ticker == '15m':
        return 0.25*60*60
    if _ticker == '30m':
        return 0.5*60*60
    if _ticker == '1h':
        return 60*60
    if _ticker == '2h':
        return 2*60*60
    if _ticker == '4h':
        return 4*60*60
    if _ticker == '6h':
        return 6*60*60
    if _ticker == '8h':
        return 8*60*60
    if _ticker == '12h':
        return 12*60*60
    if _ticker == '1d':
        return 24*60*60
    if _ticker == '3d':
        return 3*24*60*60
    if _ticker == '1w':
        return 7*24*60*60


def define_signal_strength(_setups):
    if len(_setups) == 0:
        return False
    _setups.reverse()
    _setups_dict = {}
    for _setup in _setups:
        _setups_dict[_setup.ticker] = _setup

    for _ii in range(len(_setups)):
        _signal_strength = 0
        _dt = 2 * get_delta_t(_setups[_ii].ticker)
        if _setups[_ii].ticker == '1d':
            _signal_strength = 24
            if '1w' in _setups_dict:
                _s = _setups_dict['1w']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 7*24
            if '3d' in _setups_dict:
                _s = _setups_dict['3d']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 3*24
            if '12h' in _setups_dict:
                _s = _setups_dict['12h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 12
            if '8h' in _setups_dict:
                _s = _setups_dict['8h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 8
            if '6h' in _setups_dict:
                _s = _setups_dict['6h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 6
            if '4h' in _setups_dict:
                _s = _setups_dict['4h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 4
            if '2h' in _setups_dict:
                _s = _setups_dict['2h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 2
            if '1h' in _setups_dict:
                _s = _setups_dict['1h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 1
            if '30m' in _setups_dict:
                _s = _setups_dict['30m']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 0.5
            if '15m' in _setups_dict:
                _s = _setups_dict['15m']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 0.25
        if _setups[_ii].ticker == '12h':
            _signal_strength = 12
            if '1w' in _setups_dict:
                _s = _setups_dict['1w']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 7*24
            if '3d' in _setups_dict:
                _s = _setups_dict['3d']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 3*24
            if '1d' in _setups_dict:
                _s = _setups_dict['1d']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 24
            if '8h' in _setups_dict:
                _s = _setups_dict['8h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 8
            if '6h' in _setups_dict:
                _s = _setups_dict['6h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 6
            if '4h' in _setups_dict:
                _s = _setups_dict['4h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 4
            if '2h' in _setups_dict:
                _s = _setups_dict['2h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 2
            if '1h' in _setups_dict:
                _s = _setups_dict['1h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 1
            if '30m' in _setups_dict:
                _s = _setups_dict['30m']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 0.5
            if '15m' in _setups_dict:
                _s = _setups_dict['15m']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 0.25
        if _setups[_ii].ticker == '8h':
            _signal_strength = 8
            if '1w' in _setups_dict:
                _s = _setups_dict['1w']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 7*24
            if '3d' in _setups_dict:
                _s = _setups_dict['3d']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 3*24
            if '1d' in _setups_dict:
                _s = _setups_dict['1d']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 24
            if '12h' in _setups_dict:
                _s = _setups_dict['12h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 12
            if '6h' in _setups_dict:
                _s = _setups_dict['6h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 6
            if '4h' in _setups_dict:
                _s = _setups_dict['4h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 4
            if '2h' in _setups_dict:
                _s = _setups_dict['2h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 2
            if '1h' in _setups_dict:
                _s = _setups_dict['1h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 1
            if '30m' in _setups_dict:
                _s = _setups_dict['30m']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 0.5
            if '15m' in _setups_dict:
                _s = _setups_dict['15m']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 0.25
        if _setups[_ii].ticker == '6h':
            _signal_strength = 6
            if '1w' in _setups_dict:
                _s = _setups_dict['1w']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 7*24
            if '3d' in _setups_dict:
                _s = _setups_dict['3d']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 3*24
            if '1d' in _setups_dict:
                _s = _setups_dict['1d']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 24
            if '12h' in _setups_dict:
                _s = _setups_dict['12h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 12
            if '8h' in _setups_dict:
                _s = _setups_dict['8h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 8
            if '4h' in _setups_dict:
                _s = _setups_dict['4h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 4
            if '2h' in _setups_dict:
                _s = _setups_dict['2h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 2
            if '1h' in _setups_dict:
                _s = _setups_dict['1h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 1
            if '30m' in _setups_dict:
                _s = _setups_dict['30m']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 0.5
            if '15m' in _setups_dict:
                _s = _setups_dict['15m']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 0.25
        if _setups[_ii].ticker == '4h':
            _signal_strength = 4
            if '1w' in _setups_dict:
                _s = _setups_dict['1w']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 7*24
            if '3d' in _setups_dict:
                _s = _setups_dict['3d']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 3*24
            if '1d' in _setups_dict:
                _s = _setups_dict['1d']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 24
            if '12h' in _setups_dict:
                _s = _setups_dict['12h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 12
            if '8h' in _setups_dict:
                _s = _setups_dict['8h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 8
            if '6h' in _setups_dict:
                _s = _setups_dict['6h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 6
            if '2h' in _setups_dict:
                _s = _setups_dict['2h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 2
            if '1h' in _setups_dict:
                _s = _setups_dict['1h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 1
            if '30m' in _setups_dict:
                _s = _setups_dict['30m']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 0.5
            if '15m' in _setups_dict:
                _s = _setups_dict['15m']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 0.25
        if _setups[_ii].ticker == '2h':
            _signal_strength = 2
            if '1w' in _setups_dict:
                _s = _setups_dict['1w']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 7 * 24
            if '3d' in _setups_dict:
                _s = _setups_dict['3d']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 3 * 24
            if '1d' in _setups_dict:
                _s = _setups_dict['1d']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 24
            if '12h' in _setups_dict:
                _s = _setups_dict['12h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 12
            if '8h' in _setups_dict:
                _s = _setups_dict['8h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 8
            if '6h' in _setups_dict:
                _s = _setups_dict['6h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 6
            if '4h' in _setups_dict:
                _s = _setups_dict['4h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 4
            if '1h' in _setups_dict:
                _s = _setups_dict['1h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 1
            if '30m' in _setups_dict:
                _s = _setups_dict['30m']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 0.5
            if '15m' in _setups_dict:
                _s = _setups_dict['15m']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 0.25
        if _setups[_ii].ticker == '1h':
            _signal_strength = 1
            if '1w' in _setups_dict:
                _s = _setups_dict['1w']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 7 * 24
            if '3d' in _setups_dict:
                _s = _setups_dict['3d']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 3 * 24
            if '1d' in _setups_dict:
                _s = _setups_dict['1d']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 24
            if '12h' in _setups_dict:
                _s = _setups_dict['12h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 12
            if '8h' in _setups_dict:
                _s = _setups_dict['8h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 8
            if '6h' in _setups_dict:
                _s = _setups_dict['6h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 6
            if '4h' in _setups_dict:
                _s = _setups_dict['4h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 4
            if '2h' in _setups_dict:
                _s = _setups_dict['2h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 2
            if '30m' in _setups_dict:
                _s = _setups_dict['30m']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 0.5
            if '15m' in _setups_dict:
                _s = _setups_dict['15m']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 0.25
        if _setups[_ii].ticker == '30m':
            _signal_strength = 0.5
            if '1w' in _setups_dict:
                _s = _setups_dict['1w']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 7 * 24
            if '3d' in _setups_dict:
                _s = _setups_dict['3d']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 3 * 24
            if '1d' in _setups_dict:
                _s = _setups_dict['1d']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 24
            if '12h' in _setups_dict:
                _s = _setups_dict['12h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 12
            if '8h' in _setups_dict:
                _s = _setups_dict['8h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 8
            if '6h' in _setups_dict:
                _s = _setups_dict['6h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 6
            if '4h' in _setups_dict:
                _s = _setups_dict['4h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 4
            if '2h' in _setups_dict:
                _s = _setups_dict['2h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 2
            if '1h' in _setups_dict:
                _s = _setups_dict['1h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 1
            if '15m' in _setups_dict:
                _s = _setups_dict['15m']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 0.25
        if _setups[_ii].ticker == '15m':
            _signal_strength = 0.25
            if '1w' in _setups_dict:
                _s = _setups_dict['1w']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 7 * 24
            if '3d' in _setups_dict:
                _s = _setups_dict['3d']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 3 * 24
            if '1d' in _setups_dict:
                _s = _setups_dict['1d']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 24
            if '12h' in _setups_dict:
                _s = _setups_dict['12h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 12
            if '8h' in _setups_dict:
                _s = _setups_dict['8h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 8
            if '6h' in _setups_dict:
                _s = _setups_dict['6h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 6
            if '4h' in _setups_dict:
                _s = _setups_dict['4h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 4
            if '2h' in _setups_dict:
                _s = _setups_dict['2h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 2
            if '1h' in _setups_dict:
                _s = _setups_dict['1h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 1
            if '30m' in _setups_dict:
                _s = _setups_dict['30m']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 0.5
        _setups[_ii].signal_strength = _signal_strength
    return _setups


def manage_market_processing(_pe, _ii):
    _crawler = threading.Thread(target=process_markets, args=(_pe,),
                                name='process_markets : {}'.format(_ii))
    _crawler.start()

    return _crawler


def min_max_scanner(_market_info_collection, _threads):
    _market_info_cursor = _market_info_collection.find()
    _market_info_list = [e for e in _market_info_cursor]
    _market_info_parts = np.array_split(_market_info_list, _threads)
    _ik = 0
    _crawlers = []
    for _part_list in _market_info_parts:
        sleep(randrange(10))
        _pe = ProcessingEntry(_market_info_collection, _part_list)
        _c = manage_market_processing(_pe, _ik)
        _crawlers.append(_c)
        _ik += 1
        # break
    for _c in _crawlers:
        _c.join()


class ProcessingEntry(object):
    def __init__(self, _market_info_collection, _market_info_list):
        self.market_info_collection = _market_info_collection
        self.market_info_list = _market_info_list


class ComputingSetupEntry(object):
    def __init__(self, _market, _type, _ticker):
        self.market = _market
        self.type = _type
        self.ticker = _ticker
        self.se = None


def manage_entry_computing(_cse: ComputingSetupEntry):
    _crawler = threading.Thread(target=process_computing, args=(_cse,),
                                name='process_computing : {}{}_{}'.format(_cse.market, _cse.type, _cse.ticker))
    _crawler.start()

    return _crawler


def process_computing(_cse: ComputingSetupEntry):
    _klines = extract_klines(_cse.market, _cse.type, _cse.ticker)
    _se: SetupEntry = extract_buy_entry_setup(_klines, "{}{}".format(_cse.market, _cse.type).upper(), _cse.ticker)
    _klines.clear()
    if _se:
        _cse.se = _se


def filter_sell_setups(_setups):
    _sell_signals = list(filter(lambda x: x.sell_signal, _setups))
    _1w_sell = _3d_sell = _1d_sell = _12h_sell = None
    for _s_0 in _sell_signals:
        _s = _s_0.sell_signal
        if '1w' in _s:
            _1w_sell = _s['1w']
        if '3d' in _s:
            _3d_sell = _s['3d']
        if '1d' in _s:
            _1d_sell = _s['1d']
        if '12h' in _s:
            _12h_sell = _s['12h']
    _f = list(filter(lambda x: x, [_1w_sell, _3d_sell, _1d_sell, _12h_sell]))
    _out = []
    for _setup in _setups:
        if all(filter(lambda x: _setup.time > x, _f)):
            _out = _setup
    return _out


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
    _length = 130
    _coef = 0.2
    _vcoef = 2.5

    _cutoff = []
    for _ii in range(len(_vinter)):
        _cutoff.append(_coef * _df_dec['close'][_ii] * _vinter[_ii])
    _vave = _df_dec['volume'].iloc[::-1].rolling(_length).mean().iloc[::-1].drop(axis=0, index=0).reset_index(drop=True)
    _vmax = _vave * _vcoef
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
    _length = 130
    _sum = []
    for _ii in range(len(_vcp)):
        _sum.append(np.sum(_vcp[_ii:_ii+_length]) / _vave[_ii])
    return _sum


def compute_vfi(_klines_dec):
    _df_dec = create_from_offline_df(_klines_dec)
    _vinter = compute_vinter(_df_dec)
    _vcp, _vave = compute_vcp(_df_dec, _vinter)

    return _compute_vfi(_vcp, _vave)



_mt = []


def process_markets(_pe: ProcessingEntry):
    _tickers = ['15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w']
    for _market_info in _pe.market_info_list:
        _market = _market_info['name']
        if _market_info['active'] and _market != "paxg":
            _start = timer()
            logger.info(_market)
            _type = _pe.market_info_collection.name
            _setups = []
            _cses = []
            _processors = []
            for _ticker in _tickers:
                if _ticker == '1d':
                    sleep(randrange(10))
                    _cse = ComputingSetupEntry(_market, _type, _ticker)
                    _cses.append(_cse)
                    _processors.append(manage_entry_computing(_cse))
            [x.join() for x in _processors]
            _setups = list(map(lambda y: y.se, filter(lambda x: x.se, _cses)))
            filter_sell_setups(_setups)
            _setups_exist = define_signal_strength(_setups)  # filter out volume flow index < 0
            if _setups_exist:
                _setup_collection = db_setup.get_collection(_pe.market_info_collection.name.lower(),
                                                            codec_options=codec_options)
                _setup_collection.insert_one(to_mongo(_se))
                for _se in _setups:
                    logger.info(
                        "Setup entry found -- market: {} ticker: {} buy_price: {} signal_strength: {}".format(
                            _se.market.lower(),
                            _se.ticker,
                            _se.buy_price,
                            _se.signal_strength))
            _end = timer()
            _et = _end - _start
            _mt.append(_et)
            logger.info("Market {} time: {} s".format(_market, _et))


def extract_buy_entry_setup(_klines, _market, _ticker):
    _klines_cp = _klines.copy()
    _klines_cp.reverse()
    _df_dec = create_from_offline_df(_klines)
    _df_inc = create_from_offline_df(_klines_cp)
    _conjectures = list(map(lambda x: smooth(_df_inc['open'], x), np.arange(0.1, 1.0, 0.05)))
    _amlag = np.mean(_conjectures, axis=0)
    _tr = compute_tr(_df_inc)
    _inapproximability = np.mean(list(map(lambda x: smooth(_tr, x), np.arange(0.1, 1.0, 0.05))), axis=0)
    _upper_threshold_of_approximability1 = _amlag + _inapproximability * 1.618
    _upper_threshold_of_approximability2 = _amlag + 2 * _inapproximability * 1.618
    _lower_threshold_of_approximability1 = _amlag - _inapproximability * 1.618
    _lower_threshold_of_approximability2 = _amlag - 2 * _inapproximability * 1.618
    _strong_buy = get_crossup(_df_inc, _lower_threshold_of_approximability2)
    _strong_sell = get_crossdn(_df_inc, _upper_threshold_of_approximability2)
    _major = lele(_df_inc['open'], _df_inc['close'], _df_inc['high'], _df_inc['low'], 2, 20)  # bull/bear
    _strong_sell_ind = get_strong_major_indices(_strong_sell, True)
    _strong_buy_ind = get_strong_major_indices(_strong_buy, True)
    _buy_ind = get_major_indices(_major, 1)
    _sell_ind = get_major_indices(_major, -1)
    _buys = [*_strong_buy_ind, *_buy_ind]
    _sell_signal = None
    if len(_buys) == 0:
        return False  # there is no entry setup, we skip
    if len(_strong_sell_ind) > 0:
        _last_strong_sell_ind = _strong_sell_ind[-1] + 1 + 21
        _buys = list(filter(lambda x: x > _last_strong_sell_ind, _buys))
        if _last_strong_sell_ind in _df_inc['time']:
            _sell_signal = int(_df_inc['time'][_last_strong_sell_ind]/1000)
    if len(_buys) == 0:
        return False  # there is no entry setup, we skip
    if len(_sell_ind) > 0:
        _last_sell_ind = _sell_ind[-1] + 21
        _buys = list(filter(lambda x: x > _last_sell_ind, _buys))
        if _last_sell_ind in _df_inc['time']:
            if _sell_signal and _last_sell_ind > _last_strong_sell_ind:
                _sell_signal = int(_df_inc['time'][_last_sell_ind]/1000)
            elif not _sell_signal:
                _sell_signal = int(_df_inc['time'][_last_sell_ind]/1000)
    if len(_buys) == 0:
        return False  # there is no entry setup, we skip
    _buys.sort()
    _adjustment = compute_adjustment(_df_dec['open'], _df_dec['close'], _df_dec['high'], _df_dec['low'],
                                     _df_dec['volume'])
    _money_strength = compute_money_strength(_df_dec['close'], _df_dec['volume'])
    _whale_money_flow = compute_whale_money_flow(_adjustment, _df_dec['volume'], _money_strength)
    _trend_exhaustion = compute_trend_exhaustion(_df_dec['open'], _df_dec['close'], _df_dec['high'], _df_dec['low'],
                                                 _df_dec['volume'])
    _buys = filter_buys_trend_exhaustion(_trend_exhaustion, _buys)
    _buys = filter_buys_whale_money_flow(_whale_money_flow, _buys)
    if len(_buys) == 0:
        return False  # there is no entry setup, we skip
    _t = get_time_buys(_buys, _df_inc)
    _buy_price = extract_order_price(_buys, _df_inc)
    _se = SetupEntry(_market, _buy_price, len(_buys), _ticker, _t[-1])
    if str(_sell_signal) != "None" and _ticker in ['1w', '3d', '1d', '12h']:
        _se.sell_signal[_ticker] = _sell_signal
    return _se if abs(_df_dec['time'][0] - _se.time) < 2 * ticker2num(_se.ticker) * 60 * 60 else False
    # return _se


def _stuff():
    lib_initialize()
    db_markets_info = mongo_client.markets_info
    db_journal = mongo_client.journal

    decimal_codec = DecimalCodec()
    type_registry = TypeRegistry([decimal_codec])
    codec_options = CodecOptions(type_registry=type_registry)

    btc_markets_collection = db_markets_info.get_collection("btc", codec_options=codec_options)
    usdt_markets_collection = db_markets_info.get_collection("usdt", codec_options=codec_options)
    busd_markets_collection = db_markets_info.get_collection("busd", codec_options=codec_options)

    start = timer()
    min_max_scanner(usdt_markets_collection, 1)  # 5, 6, 4
    end = timer()
    et = (end - start) / 60
    logger.info("Total time: {} minutes".format(et))
    logger.info("Avg time: {} minutes".format(np.mean(_mt)))


if __name__ == "__main__":
    _stuff()


