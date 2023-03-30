import traceback
from random import randrange
from time import sleep

import numpy as np
import pandas as pd
from bson.codec_options import TypeRegistry, CodecOptions

from library import setup_logger, DecimalCodec, get_pickled, get_time_from_binance_tmstmp, try_get_klines, \
    get_binance_interval_unit, get_binance_klines, Kline
from mongodb import mongo_client
from tb_lib import compute_tr, smooth, get_crossup, get_crossdn, lele, get_strong_major_indices, get_major_indices, \
    compute_adjustment, compute_money_strength, compute_whale_money_flow, compute_trend_exhaustion

db_klines = mongo_client.klines
db_setup = mongo_client.setup
decimal_codec = DecimalCodec()
type_registry = TypeRegistry([decimal_codec])
codec_options = CodecOptions(type_registry=type_registry)


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


def create_from_offline_df(_klines):
    _open = list(map(lambda x: x['kline']['opening'], _klines))
    _close = list(map(lambda x: x['kline']['closing'], _klines))
    _high = list(map(lambda x: x['kline']['highest'], _klines))
    _low = list(map(lambda x: x['kline']['lowest'], _klines))
    _volume = list(map(lambda x: x['kline']['volume'], _klines))
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
            'start_time': _kline.start_time,
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
        if _whale_money_flow[_ind] < 38.0 or any(filter(lambda x: x < 20.0, _whale_money_flow[_ind:_ind + 20])):
            _r.append(_buy)
    return _r


def get_time_buys(_buys, _df_inc):
    return list(map(lambda x: _df_inc['time'][x], _buys))


def extract_order_price(_buys, _df_inc):
    return (_df_inc['open'][_buys[-1]]+_df_inc['close'][_buys[-1]]+_df_inc['high'][_buys[-1]]+_df_inc['low'][_buys[-1]])/4 if len(_buys) > 0 else False


class SetupEntry(object):
    def __init__(self, _market, _buy_price, _buys_count, _ticker, _time):
        self.market = _market
        self.buy_price = _buy_price
        self.buys_count = _buys_count
        self.ticker = _ticker
        self.time = _time
        self.time_str = get_time_from_binance_tmstmp(_time)
        self.signal_strength = None

    def set_signal_strength(self, _signal_strength):
        self.signal_strength = _signal_strength


def to_mongo(_se: SetupEntry):
    return {
        'market': _se.market.upper(),
        'buy_price': _se.buy_price,
        'buys_count': _se.buys_count,
        'ticker': _se.ticker,
        'signal_strength': _se.signal_strength,
        'time': int(_se.time/1000),
        'time_str': get_time_from_binance_tmstmp(_se.time),
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
    if _ticker == '4h':
        return 4*60*60*1000
    if _ticker == '6h':
        return 6*60*60*1000
    if _ticker == '8h':
        return 8*60*60*1000
    if _ticker == '12h':
        return 12*60*60*1000
    if _ticker == '1d':
        return 24*60*60*1000


def define_signal_strength(_setups):
    _setups.reverse()
    _setups_dict = {}
    for _setup in _setups:
        _setups_dict[_setup.ticker] = _setup

    for _ii in range(len(_setups)):
        _dt = get_delta_t(_setups[_ii].ticker)
        if _setups[_ii].ticker == '1d':
            _signal_strength = 24
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
            _setups[_ii].signal_strength = _signal_strength
        if _setups[_ii].ticker == '12h':
            _signal_strength = 12
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
            _setups[_ii].signal_strength = _signal_strength
        if _setups[_ii].ticker == '8h':
            _signal_strength = 8
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
            _setups[_ii].signal_strength = _signal_strength
        if _setups[_ii].ticker == '6h':
            _signal_strength = 6
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
            _setups[_ii].signal_strength = _signal_strength
        if _setups[_ii].ticker == '4h':
            _signal_strength = 4
            if '4h' in _setups_dict:
                _s = _setups_dict['4h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 4
            if '12h' in _setups_dict:
                _s = _setups_dict['12h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 12
            if '6h' in _setups_dict:
                _s = _setups_dict['6h']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 6
            if '1d' in _setups_dict:
                _s = _setups_dict['1d']
                if _setups[_ii].time - _dt < _s.time < _setups[_ii].time + _dt:
                    _signal_strength += 24
            _setups[_ii].signal_strength = _signal_strength


def min_max_scanner(_market_info_collection):
    _market_info_cursor = _market_info_collection.find()
    _market_info_list = [e for e in _market_info_cursor]
    _tickers = ['4h', '6h', '8h', '12h', '1d']
    for _market_info in _market_info_list:
        if _market_info['active']:
            _market = _market_info['name']
            _type = _market_info_collection.name
            _setups = []
            for _ticker in _tickers:
                _klines = extract_klines(_market, _type, _ticker)
                _se : SetupEntry = extract_buy_entry_setup(_klines, "{}{}".format(_market, _type).upper(), _ticker)
                _setups.append(_se)


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
    _buys = None
    if len(_strong_sell_ind) > 0:
        _last_strong_sell_ind = _strong_sell_ind[-1] + 1 + 21
        _buys = list(filter(lambda x: x > _last_strong_sell_ind, [*_strong_buy_ind, *_buy_ind]))
    if len(_sell_ind) > 0:
        _last_sell_ind = _sell_ind[-1] + 21
        _buys = list(filter(lambda x: x > _last_sell_ind, _buys))
    if _buys:
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
    return SetupEntry(_market, _buy_price, len(_buys), _ticker, _t[-1])


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