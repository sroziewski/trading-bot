import time
import traceback
import warnings
from os import path

import numpy as np

import matplotlib.pyplot as plt
import talib
from binance.client import Client

from library import binance_obj, get_binance_interval_unit, AssetTicker, highest_bid, get_pickled, \
    exclude_markets, take_profit, BuyAsset, find_first_maximum, save_to_file, get_klines, lowest_ask, get_time, key_dir, \
    is_bullish_setup, Asset, find_minimum, find_local_maximum, find_minimum_2, find_first_minimum, \
    is_second_golden_cross, is_first_golden_cross

from binance.client import Client as BinanceClient

warnings.filterwarnings('error')


def relative_strength_index(_closes, n=14):
    _prices = np.array(_closes, dtype=np.float32)

    _deltas = np.diff(_prices)
    _seed = _deltas[:n + 1]
    _up = _seed[_seed >= 0].sum() / n
    _down = -_seed[_seed < 0].sum() / n
    _rs = _up / _down
    _rsi = np.zeros_like(_prices)
    _rsi[:n] = 100. - 100. / (1. + _rs)

    for _i in range(n, len(_prices)):
        _delta = _deltas[_i - 1]  # cause the diff is 1 shorter

        if _delta > 0:
            _upval = _delta
            _downval = 0.
        else:
            _upval = 0.
            _downval = -_delta

        _up = (_up * (n - 1) + _upval) / n
        _down = (_down * (n - 1) + _downval) / n

        _rs = _up / _down
        _rsi[_i] = 100. - 100. / (1. + _rs)

    return _rsi


def get_rsi(_market, _ticker, _time_interval):
    for _i in range(0, 10):
        try:
            _klines = get_klines(_market, _ticker, _time_interval)
            _closes = get_closes(_klines)
            return talib.RSI(_closes, timeperiod=14)
        except Warning:
            time.sleep(1)


plt.figure(1)


### prices

# plt.subplot2grid((8, 1), (0, 0), rowspan = 4)
# plt.plot(_closes[-wins:], 'k', lw = 1)

# plt.subplot2grid((2, 1), (0, 0))
# plt.plot(r[-wins:], color='black', lw=1)
# plt.axhline(y=30,     color='red',   linestyle='-')
# plt.axhline(y=70,     color='blue',  linestyle='-')


def get_magnitude(_reversed_max_ind, _max_val):
    try:
        return int(np.log10(_reversed_max_ind / np.abs(_max_val)))
    except Warning:
        return -1


def get_angle(p1, p2):
    return np.arctan((p2[1] - p1[1]) / (p2[0] - p1[0])) * 180 / np.pi


def is_signal_divergence_ratio(_macd, _macdsignal, _ratio, _start, _stop):
    _diff = _macd - _macdsignal
    _diff_max_val, _diff_reversed_max_ind = find_first_maximum(_diff[_start:_stop:1], 10)
    _diff_max_val2, _diff_reversed_max_ind2 = find_first_maximum(-_diff[_start:_stop:1], 10)
    if _diff_max_val2 > _diff_max_val:
        _diff_max_val = _diff_max_val2
    return np.abs((_diff[_stop] + _diff[_stop - 1]) / 2) / _diff_max_val <= _ratio


def is_rsi_slope_condition(_rsi, _rsi_limit, _angle_limit, _start, _stop, _window=10):
    if (_rsi[_stop] + _rsi[_stop - 1]) / 2 > _rsi_limit:
        return False
    _rsi_max_val, _rsi_reversed_max_ind = find_first_maximum(_rsi[_start:_stop:1], _window)
    _rsi_magnitude = get_magnitude(_rsi_reversed_max_ind, _rsi_max_val)
    if _rsi_magnitude == -1:
        return False
    _rsi_angle = get_angle((0, _rsi[_start:_stop:1][-1]),
                           (_rsi_reversed_max_ind / np.power(10, _rsi_magnitude), _rsi_max_val))
    return _rsi_angle >= _angle_limit


def is_macd_condition(_macd, _angle_limit, _start, _stop, _window=10):
    _macd_max_val, _macd_reversed_max_ind = find_first_maximum(_macd[_start:_stop:1], _window)
    _macd_max_val2, _macd_reversed_max_ind2 = find_first_maximum(-_macd[_start:_stop:1], _window)
    if _macd_reversed_max_ind2 < _macd_reversed_max_ind:
        _current_macd = (_macd[_stop] + _macd[_stop - 1]) / 2
        _local_min_ratio = (_macd_max_val2 - np.abs(_current_macd)) / _macd_max_val2
        if _local_min_ratio > 0.2:
            # we have a minimum at first
            return False
    if _macd_reversed_max_ind == -1:
        return False
    _macd_magnitude = get_magnitude(_macd_reversed_max_ind, _macd_max_val)
    _macd_angle = get_angle((0, _macd[_start:_stop:1][-1]),
                            (_macd_reversed_max_ind / np.power(10, _macd_magnitude), _macd_max_val))
    return _macd_angle >= _angle_limit


def is_higher_low(_values, _limit, _start, _stop, _window=10):
    _flipped_values = np.max(_values[_start:_stop:1]) - _values
    _max_val, _reversed_max_ind = find_first_maximum(_flipped_values[_start:_stop:1], _window)
    _first_local_min = _values[_stop - _reversed_max_ind]
    if _reversed_max_ind == -1 or _first_local_min > _limit:  # 1. there is no local minimum 2. RSI value for minimum is too high
        return False
    _current_value = _values[_stop]
    if _stop - _reversed_max_ind + 1 - (_stop - 1) <= 2:  # len for the interval should be longer than 2
        return False
    _interval_for_local_max = _values[_stop - _reversed_max_ind + 1:_stop - 1]
    _local_max = np.max(_interval_for_local_max)
    return (_local_max - _current_value) > 2  # the difference in RSI local extrema has to be minimum 2 rsi


def is_tradeable(_closes, _rsi, _macd, _macdsignal):
    if len(list(filter(lambda x: x == 0, get_last(_rsi, -1, 10)))) > 0:
        return False
    _start = 33
    _stop = -1
    _slope = 30
    _tight_slope = 40
    _rsi_limit = 45
    _rsi_tight_limit = 30
    _rsi_normal_cond = is_rsi_slope_condition(_rsi, _rsi_limit, _slope, _start, _stop)
    _rsi_tight_cond = is_rsi_slope_condition(_rsi, _rsi_tight_limit, _tight_slope, _start, _stop)
    _macd_normal_cond = is_macd_condition(_macd, _slope, _start, _stop)
    _macd_tight_cond = is_macd_condition(_macd, 50, _start, _stop)
    _divergence_ratio_cond = is_signal_divergence_ratio(_macd, _macdsignal, 0.1, _start, _stop)
    _hl_cond = is_higher_low(_rsi, _rsi_limit, _start, _stop)

    _tradeable = False
    if _rsi_normal_cond and _divergence_ratio_cond and _macd_normal_cond:
        _tradeable = True
    if _rsi_tight_cond or _macd_tight_cond:
        _tradeable = True
    if _hl_cond and _divergence_ratio_cond:
        _tradeable = True
    return _tradeable


def get_closes(_klines):
    return np.array(list(map(lambda _x: float(_x[4]), _klines)))


def get_opens(_klines):
    return np.array(list(map(lambda _x: float(_x[1]), _klines)))


def get_tradeable_assets(_markets, _ticker):
    _time_interval = get_binance_interval_unit(_ticker)
    _tradeable_assets = []
    for _market in _markets:
        _asset = _market.split("BTC")[0]
        try:
            if _asset:
                # print(_asset)
                _closes = get_closes(get_klines(_market, _ticker, _time_interval))
                # _rsi = relative_strength_index(_closes)
                _rsi = get_rsi(_market, _ticker, _time_interval)
                _macd, _macdsignal, _macdhist = talib.MACD(_closes, fastperiod=12, slowperiod=26, signalperiod=9)
                if is_tradeable(_closes, _rsi, _macd, _macdsignal):
                    _tradeable_assets.append(AssetTicker(_asset, _ticker, lowest_ask(_market), time.time()))
        except Exception:
            print('Value Error for {} in {}'.format(_ticker, _market))
    sort_assets(_tradeable_assets)
    return _tradeable_assets


def get_tradeable_and_bullish_assets(_markets, _ticker):
    _time_interval = get_binance_interval_unit(_ticker)
    _assets = []
    for _market in _markets:
        _asset = _market.split("BTC")[0]
        try:
            if _asset:
                # print(_asset)
                _klines = get_klines(_market, _ticker, _time_interval)
                _closes = get_closes(_klines)
                _opens = get_opens(_klines)
                _ma100 = talib.MA(_closes, timeperiod=100)
                _ma50 = talib.MA(_closes, timeperiod=50)
                _ma20 = talib.MA(_closes, timeperiod=20)
                _ma7 = talib.MA(_closes, timeperiod=7)

                _cond1 = bullishness_00(_opens, _closes, _ma100, _ma50, _ma20, _ma7) \
                         or bullishness_01(_opens, _closes, _ma100, _ma50, _ma20, _ma7) or bullishness_1(_opens,
                                                                                                         _closes,
                                                                                                         _ma100, _ma50,
                                                                                                         _ma20,
                                                                                                         _ma7) \
                         or bullishness_2(_opens, _closes, _ma100, _ma50, _ma20, _ma7) or bullishness_3(_opens, _closes,
                                                                                                        _ma100, _ma50,
                                                                                                        _ma20, _ma7)
                _rsi = get_rsi(_market, _ticker, _time_interval)
                _macd, _macdsignal, _macdhist = talib.MACD(_closes, fastperiod=12, slowperiod=26, signalperiod=9)
                _cond2 = is_tradeable(_closes, _rsi, _macd, _macdsignal)

                if _cond1 and _cond2:
                    _assets.append(AssetTicker(_asset, _ticker, lowest_ask(_market), time.time()))
        except Exception as err:
            print('Exception for {} in {}'.format(_ticker, _market))
            # traceback.print_tb(err.__traceback__)
    sort_assets(_assets)
    return _assets


def get_bullish_assets(_markets, _ticker):
    _time_interval = get_binance_interval_unit(_ticker)
    _bullish_assets = []
    for _market in _markets:
        try:
            _asset = _market.split("BTC")[0]
            if _asset:
                # print(_asset)
                _klines = get_klines(_market, _ticker, _time_interval)
                _closes = get_closes(_klines)
                _opens = get_opens(_klines)
                _ma100 = talib.MA(_closes, timeperiod=100)
                _ma50 = talib.MA(_closes, timeperiod=50)
                _ma20 = talib.MA(_closes, timeperiod=20)
                _ma7 = talib.MA(_closes, timeperiod=7)

                _cond1 = bullishness_00(_opens, _closes, _ma100, _ma50, _ma20, _ma7) \
                         or bullishness_01(_opens, _closes, _ma100, _ma50, _ma20, _ma7) or bullishness_1(_opens,
                                                                                                         _closes,
                                                                                                         _ma100, _ma50,
                                                                                                         _ma20,
                                                                                                         _ma7) \
                         or bullishness_2(_opens, _closes, _ma100, _ma50, _ma20, _ma7) or bullishness_3(_opens, _closes,
                                                                                                        _ma100, _ma50,
                                                                                                        _ma20, _ma7)

                if _cond1:
                    _bullish_assets.append(AssetTicker(_asset, _ticker, lowest_ask(_market), time.time()))
        except Exception:
            print('Exception for {} in {}'.format(_ticker, _market))
    sort_assets(_bullish_assets)
    return _bullish_assets


def sort_assets(_assets):
    _assets.sort(key=lambda a: a.name)


def get_avg_last(_values, _stop, _window=1):
    return np.mean(_values[_stop - _window:])


def get_std_last(_values, _stop, _window=1):
    return np.std(_values[_stop - _window:])


def get_last(_values, _stop, _window=1):
    return _values[_stop - _window + 1:]


def get_avg_last_2(_values, _stop, _window=2):
    return np.mean(_values[_stop - _window + 1:_stop])


def get_last_2(_values, _stop, _window=2):
    return _values[_stop - _window + 1:_stop]


def bullishness_00(_opens, _closes, _ma100, _ma50, _ma20, _ma7, _stop=-1):
    _curr_ma100 = get_avg_last(_ma100, _stop)
    _curr_ma50 = get_avg_last(_ma50, _stop)
    _curr_ma_20 = get_avg_last(_ma20, _stop)
    _curr_ma_7 = get_avg_last(_ma7, _stop)
    _curr_ma = _curr_ma_20
    _mean_open = get_avg_last(_opens, _stop, 10)
    _curr_close = get_avg_last(_closes, _stop)
    _closing_diff = _curr_close - _curr_ma
    _opening_diff = _curr_ma - _mean_open
    _cond_1 = _closing_diff < 0 and _closing_diff / _curr_ma < 0.05  # up to 5 percent in difference we trust! <0 is good for bouncing detector
    _cond_2 = _opening_diff > 0 and _opening_diff / _mean_open < 0.05  # up to 5 percent in difference we trust!
    return _curr_ma_20 > _curr_ma50 > _curr_ma100 and _cond_1 and _cond_2


def bullishness_01(_opens, _closes, _ma100, _ma50, _ma20, _ma7, _stop=-1):
    _curr_ma100 = get_avg_last(_ma100, _stop)
    _curr_ma50 = get_avg_last(_ma50, _stop)
    _curr_ma_20 = get_avg_last(_ma20, _stop)
    _curr_ma_7 = get_avg_last(_ma7, _stop)
    _curr_ma = _curr_ma_20
    _mean_open = get_avg_last(_opens, _stop, 10)
    _curr_close = get_avg_last(_closes, _stop)
    _closing_diff = _curr_close - _curr_ma
    _opening_diff = _curr_ma - _mean_open
    _cond_1 = _closing_diff > 0 and _closing_diff / _curr_ma < 0.05  # up to 5 percent in difference we trust! <0 is good for bouncing detector
    _cond_2 = _opening_diff > 0 and _opening_diff / _mean_open < 0.05  # up to 5 percent in difference we trust!
    return _curr_ma_20 > _curr_ma50 > _curr_ma100 and _cond_1 and _cond_2


def bullishness_1(_opens, _closes, _ma100, _ma50, _ma20, _ma7, _stop=-1):
    _curr_ma100 = get_avg_last(_ma100, _stop)
    _curr_ma50 = get_avg_last(_ma50, _stop)
    _curr_ma_20 = get_avg_last(_ma20, _stop)
    _curr_ma_7 = get_avg_last(_ma7, _stop)
    _curr_ma = _curr_ma_7
    _mean_open = get_avg_last(_opens, _stop, 10)
    _curr_close = get_avg_last(_closes, _stop)
    _closing_diff = _curr_close - _curr_ma
    _opening_diff = _curr_ma - _mean_open
    _cond_1 = _closing_diff > 0 and _closing_diff / _curr_ma < 0.05  # up to 5 percent in difference we trust! <0 is good for bouncing detector
    _cond_2 = _opening_diff > 0 and _opening_diff / _mean_open < 0.05  # up to 5 percent in difference we trust!
    return _curr_ma_7 < _curr_ma_20 > _curr_ma50 > _curr_ma100 and _cond_1 and _cond_2


def bullishness_2(_opens, _closes, _ma100, _ma50, _ma20, _ma7, _stop=-1):
    _curr_ma100 = get_avg_last(_ma100, _stop)
    _curr_ma50 = get_avg_last(_ma50, _stop)
    _curr_ma20 = get_avg_last(_ma20, _stop)
    _mean_open = get_avg_last(_opens, _stop, 10)
    _curr_open = get_avg_last(_opens, _stop)
    _curr_close = get_avg_last(_closes, _stop)
    _closing_diff = _curr_close - _curr_ma20
    _opening_diff = _curr_ma20 - _mean_open
    _cond_1 = _closing_diff > 0 and _closing_diff / _curr_ma20 < 0.05  # up to 5 percent in difference we trust!
    _cond_2 = _opening_diff > 0 and _opening_diff / _mean_open < 0.05  # up to 5 percent in difference we trust!
    return _curr_ma20 < _curr_ma50 > _curr_ma100 and _cond_1 and _cond_2


def bullishness_3(_opens, _closes, _ma100, _ma50, _ma20, _ma7, _stop=-1):
    _curr_ma100 = get_avg_last(_ma100, _stop)
    _curr_ma50 = get_avg_last(_ma50, _stop)
    _curr_ma20 = get_avg_last(_ma20, _stop)
    _curr_ma7 = get_avg_last(_ma7, _stop)
    _mean_open = get_avg_last(_opens, _stop, 10)
    _curr_open = get_avg_last(_opens, _stop)
    _curr_close = get_avg_last(_closes, _stop)
    _closing_diff = _curr_close - _curr_ma7
    _opening_diff = _curr_ma7 - _mean_open
    _cond_1 = _closing_diff > 0 and _closing_diff / _curr_ma7 < 0.05  # up to 5 percent in difference we trust!
    _cond_2 = _opening_diff > 0 and _opening_diff / _mean_open < 0.05  # up to 5 percent in difference we trust!
    _cond_3 = _curr_ma7 < _curr_ma20 < _curr_ma50

    return _cond_1 and _cond_2 and _cond_3


def aggregate_assets(_map, _assets, _ticker):
    for _asset in _assets:
        if _asset.name in _map:
            _map[_asset.name].add_ticker(_ticker)
        else:
            _map[_asset.name] = _asset


def post_proc(_map):
    _list = list(map(lambda x: x[1], _map.items()))
    _list.sort(key=lambda a: len(a.tickers))
    _list.reverse()
    return _list


def print_assets(_assets):
    for _a in _assets:
        print(_a.name + " : " + ' '.join(_a.tickers) + " ask price : " + "{:.8f} time : {}".format(_a.ask_price,
                                                                                                   get_time(
                                                                                                       _a.timestamp)))


def analyze_markets():
    markets = binance_obj.get_all_btc_currencies(exclude_markets)

    # tickers = [Client.KLINE_INTERVAL_3MINUTE, Client.KLINE_INTERVAL_5MINUTE,
    #            Client.KLINE_INTERVAL_15MINUTE, Client.KLINE_INTERVAL_30MINUTE, Client.KLINE_INTERVAL_1HOUR,
    #            Client.KLINE_INTERVAL_2HOUR,
    #            Client.KLINE_INTERVAL_4HOUR, Client.KLINE_INTERVAL_6HOUR, Client.KLINE_INTERVAL_8HOUR,
    #            Client.KLINE_INTERVAL_12HOUR,
    #            Client.KLINE_INTERVAL_1DAY, Client.KLINE_INTERVAL_3DAY]

    tickers = [Client.KLINE_INTERVAL_12HOUR]

    print("bullish & tradeable assets")
    bullish_tradeable_map = {}
    for ticker in tickers:
        aggregate_assets(bullish_tradeable_map, get_tradeable_and_bullish_assets(markets, ticker), ticker)

    bullish_tradeable_list = post_proc(bullish_tradeable_map)
    print_assets(bullish_tradeable_list)

    print("bullish assets")
    bullish_map = {}
    for ticker in tickers:
        aggregate_assets(bullish_map, get_bullish_assets(markets, ticker), ticker)

    bullish_list = post_proc(bullish_map)
    print_assets(bullish_list)

    print("tradeable assets")
    tradeable_map = {}
    for ticker in tickers:
        aggregate_assets(tradeable_map, get_tradeable_assets(markets, ticker), ticker)

    tradeable_list = post_proc(tradeable_map)
    print_assets(tradeable_list)


#
# markets = binance.get_all_btc_currencies(exclude_markets)
# ticker = Client.KLINE_INTERVAL_30MINUTE
# tradeable_assets_30min = get_tradeable_assets(markets, ticker)
#
# markets = binance.get_all_btc_currencies(exclude_markets)
# ticker = Client.KLINE_INTERVAL_4HOUR
# tradeable_assets_4h = get_tradeable_assets(markets, ticker)
#
# markets = binance.get_all_btc_currencies(exclude_markets)
# ticker = Client.KLINE_INTERVAL_6HOUR
# tradeable_assets_6h = get_tradeable_assets(markets, ticker)
#
# markets = binance.get_all_btc_currencies(exclude_markets)
# ticker = Client.KLINE_INTERVAL_12HOUR
# tradeable_assets_12h = get_tradeable_assets(markets, ticker)


def is_magnitude_gt(_val, _m):
    return np.log10(_val) > _m


def get_most_volatile_market():
    _filename = "exclude-markets"
    _ticker = Client.KLINE_INTERVAL_3MINUTE
    _volatile_markets = {}
    _exclude_markets = {}
    _window = "1 days ago"
    if path.isfile(key_dir + _filename + ".pkl"):
        _exclude_markets = get_pickled(key_dir, _filename)
    else:
        _exclude_markets[_ticker] = exclude_markets
    _markets = binance_obj.get_all_btc_currencies(_exclude_markets[_ticker])
    _window = "1 day ago"
    for _market in _markets:
        try:
            _klines = get_klines(_market, _ticker, _window)
            _closes = get_closes(_klines)
            if _market == 'COCOSBTC':
                i = 1
            if is_magnitude_gt(_closes[-1], -6.5):
                _std = get_std_last(_closes, 1)
                _volatile_markets[_market] = _std / _closes[-1]
        except Exception:
            print(f"No data for market : {_market}")
            if _ticker in _exclude_markets:
                _exclude_markets[_ticker].append(_market)
            else:
                _exclude_markets[_ticker] = [_market]
    _s = sorted(_volatile_markets, key=_volatile_markets.get, reverse=True)
    save_to_file(key_dir, "exclude-markets", _exclude_markets)
    i = 1


def find_first_golden_cross(__ma50, __ma200, _offset=0):
    for i in range(_offset, len(__ma200)):
        _index = len(__ma200) - i - 1
        if __ma200[_index] > __ma50[_index]:
            return __ma200[_index], i
    return -1, -1


def find_first_drop_below_ma(_ma, _candles):
    for i in range(len(_ma)):
        if _ma[i] > _candles[i]:
            _index = len(_ma) - i - 1
            return _candles[i], _index
    return -1, -1


def main():
    # asset = Asset(exchange="binance", name="LINK", ticker=BinanceClient.KLINE_INTERVAL_1HOUR)
    # is_bullish_setup(asset)
    # analyze_markets()
    # get_most_volatile_market()

    asset = "RDN"
    market = "{}BTC".format(asset)
    ticker = BinanceClient.KLINE_INTERVAL_1HOUR
    time_interval = "1600 hours ago"

    # _klines = get_klines(market, ticker, time_interval)

    # save_to_file("C:/apps/bot/", "klines", _klines)
    _klines = get_pickled('/juno/', "klines-rdn")

    # res = is_first_golden_cross(_klines)

    r = relative_strength_index(get_closes(_klines))

    _closes = np.array(list(map(lambda _x: float(_x[4]), _klines)))
    _opens = np.array(list(map(lambda _x: float(_x[1]), _klines)))
    _high = list(map(lambda _x: float(_x[2]), _klines))
    _low = list(map(lambda _x: float(_x[3]), _klines))

    ## MACD

    macd, macdsignal, macdhist = talib.MACD(_closes, fastperiod=12, slowperiod=26, signalperiod=9)

    #
    start = 33
    # stop = -5*60-30-32
    stop = -1
    # stop = -2650
    # save_to_file("/juno/", "klines-theta", _klines[start:stop:1])

    out = is_second_golden_cross(_closes[:stop])

    # t = is_tradeable(_closes, r, macd, macdsignal)

    # rsi_normal_cond = is_rsi_slope_condition(r, 45, 30, start, stop)
    # rsi_normal_tight = is_rsi_slope_condition(r, 30, 20, start, stop)
    # macd_normal_cond = is_macd_condition(macd, 45, start, stop)
    # divergence_ratio = is_signal_divergence_ratio(macd, macdsignal, 0.1, start, stop)
    # is_hl = is_higher_low(r, 45, start, stop)

    plt.subplot2grid((3, 1), (0, 0))
    plt.plot(macd[start:stop:1], 'blue', lw=1)
    plt.plot(macdsignal[start:stop:1], 'red', lw=1)
    # plt.plot(ema9[-wins:], 'red', lw=1)
    # plt.plot(macd[-wins:], 'blue', lw=1)

    # plt.subplot2grid((2, 1), (7, 0))
    #
    plt.plot(macd[start:stop:1] - macdsignal[start:stop:1], 'k', lw=2)
    plt.plot(np.zeros(len(macd[start:stop:1])), 'y', lw=2)
    # plt.axhline(y=0, color='b', linestyle='-')
    plt.subplot2grid((3, 1), (1, 0))
    plt.plot(r[start:stop:1], 'red', lw=1)

    ma200 = talib.MA(_closes, timeperiod=200)
    # ma100 = talib.MA(_closes, timeperiod=100)
    ma50 = talib.MA(_closes, timeperiod=50)
    # ma20 = talib.MA(_closes, timeperiod=20)
    # ma7 = talib.MA(_closes, timeperiod=7)

    _ma200 = ma200[start:stop:1]
    _ma50 = ma50[start:stop:1]

    _first_gc = find_first_golden_cross(_ma50, _ma200, 50)

    drop_below_ma = find_first_drop_below_ma(_ma200[-_first_gc[1]:], _closes[-_first_gc[1]:])

    _max_high = find_local_maximum(_high[-_first_gc[1]:], 100)
    rally = (_max_high[0] - _first_gc[0]) / _first_gc[0] # 48, 82 %

    k = 1
    # _max_200 = find_local_maximum(_ma200, 200)  # first a long-period maximum
    # _min_200 = find_minimum_2(_ma200, 200)  # first a long-period minimum
    # _max_200_1 = find_first_maximum(_ma200, 5)  # second lower max
    # _min_200_1 = find_first_minimum(_ma200, 25)  # first higher minimum
    #
    #
    # fall = (np.max(_high[-500:])-np.min(_low[-500:]))/np.max(_high[-500:])  # > 22%
    #
    # # _max_200_1 = find_first_maximum(_ma200, 5)
    #
    # _max = find_first_maximum(_ma50, 10)
    # _min = find_minimum(_ma50[-_max[1]:])
    #
    # _max_g = find_local_maximum(_ma50, 50)
    # _max_l = find_local_maximum(_ma50[-_max_g[1]:], 50)
    # _min_l = find_minimum(_ma50[-_max_g[1]:-_max_l[1]])
    # _min_low_l = find_minimum(_low[-_max_g[1]:-_max_l[1]])
    #
    # _min_l_ind = -_max_l[1] + _min_l[1]
    # _min_low_l_ind = -_max_l[1] + _min_low_l[1]
    # _max_l_ind = - _max_l[1]
    #
    # _max_high_l = find_local_maximum(_high[_min_l_ind:-_max_l[1]], 10)
    # _min_before_local_max = find_minimum(_low[_max_l_ind:])
    # rise = (_max_high_l[0]-_min_low_l[0])/_min_low_l[0] # > 15%
    # drop = (_max_high_l[0] - _min_before_local_max[0]) / _max_high_l[0] # > 10%
    # _ma50[-_max_l[1] - 44] - _min_l[0]
    # _ma200[:-_max[1] + 1] # first n elements until max element

    # _max_b = find_local_maximum(_ma200[-_max[1]:_min[1]], 10)

    # 43, 36, 20 %

    # if fall > 0.22 and rise > 0.15 and drop > 0.1 and np.abs(_max_l_ind) > 50:
    #     i = 7

    _ma50 = ma50[start:stop:1]

    _max_50 = find_local_maximum(_ma50, 200)  # first a long-period maximum
    _min_50 = find_minimum_2(_ma50, 200)  # first a long-period minimum
    _max_50_1 = find_first_maximum(_ma50, 10)  # second lower max
    _min_50_1 = find_first_minimum(_ma50, 25)  # first higher minimum

    if _min_50[0] < _min_50_1[0] < _max_50_1[0] < _max_50[0] and _max_50[1] > _min_50[1] > _max_50_1[1] > _min_50_1[1]:
        aja = 1

    # HL_ma50_reversal_cond = _min_50[0] < _min_50_1[0] < _max_50_1[0] < _max_50[0] and _max_50[1] > _min_50[1] > _max_50_1[1] > _min_50_1[1]
    # min_after_max_low_variance = _min_200[0] < _max_200[0] and _max_200[1] > _min_200[1] and np.std(ma200[-200:]) / np.mean(ma200[-200:]) < 0.02
    # before_second_golden_cross_cond = _min_50[0] < _ma200[-_min_50[1]] and _max_50_1[0] > _ma200[-_max_50_1[1]] and _max_50_1[0] > _ma200[
    #     -_max_50_1[1]] and _min_50_1[0] < _ma200[-_min_50_1[1]]


    # if _min_200[0] < _max_200[0] and _max_200[1] > _min_200[1] and np.std(ma200[-200:])/np.mean(ma200[-200:]) < 0.02:
    #     aja = 1
    #
    # if _min_50[0] < _ma200[-_min_50[1]] and _max_50_1[0] > _ma200[-_max_50_1[1]] and _max_50_1[0] > _ma200[-_max_50_1[1]] and _min_50_1[0] < _ma200[-_min_50_1[1]]:
    #     asd = 1

    # if HL_ma50_reversal_cond and min_after_max_low_variance and before_second_golden_cross_cond:
    #     asd = 1

    # _ma200[:-_max[1] + 1] # first n elements until max element

    # _max_b_50 = find_local_maximum(_ma50[-_max_50[1]:_min_50[1]], 10)

    _curr_rsi = get_avg_last_2(r, stop)

    _curr_ma_50 = get_avg_last(ma50, stop)
    # _curr_ma_20 = get_avg_last(ma20, stop)
    # _curr_ma_7 = get_avg_last(ma7, stop)
    # _curr_ma_7_2 = get_avg_last_2(ma7, stop)

    # l1 = get_last(ma7, stop)
    # l2 = get_last_2(ma7, stop)

    # b = bullishness_2(_opens, _closes, ma100, ma50, ma20, stop)

    plt.subplot2grid((3, 1), (2, 0))
    # plt.plot(ma200[start:stop:1], 'green', lw=1)
    # plt.plot(ma50[start:stop:1], 'red', lw=1)


    plt.plot(_ma200, 'black', lw=1)
    plt.plot(_ma200[-_first_gc[1]:], 'green', lw=1)
    plt.plot(_ma50, 'red', lw=1)
    # plt.plot(_ma200[:-_min_200[1] + 1], 'green', lw=1)
    # plt.hlines(_min_200[0], 0, len(_ma200[:-_min_200[1] + 1]), 'black', lw=1)
    # plt.hlines(_max_200[0], 0, len(_ma200[:-_max_200[1] + 1]), 'black', lw=1)
    # plt.hlines(_max_200_1[0], 0, len(_ma200[:-_max_200_1[1] + 1]), 'black', lw=1)
    # plt.vlines(len(_ma200) - _min_200[1], np.min(_ma200[~np.isnan(_ma200)]), np.max(_ma200[~np.isnan(_ma200)]), 'black', lw=1)
    # plt.vlines(len(_ma200) - _max_200_1[1], np.min(_ma200[~np.isnan(_ma200)]), np.max(_ma200[~np.isnan(_ma200)]), 'black',
    #            lw=1)
    # plt.vlines(len(_ma200) - _max_200[1], np.min(_ma200[~np.isnan(_ma200)]), np.max(_ma200[~np.isnan(_ma200)]), 'black', lw=1)
    # plt.plot(_ma200[:-_max_200[1] + 1], 'yellow', lw=1)

    # plt.plot(_ma50, 'black', lw=1)
    # plt.plot(_ma50[:-_min_50[1] + 1], 'green', lw=1)
    # plt.hlines(_min_50[0], 0, len(_ma50[:-_min_50[1] + 1]), 'black', lw=1)
    # plt.hlines(_max_50[0], 0, len(_ma50[:-_max_50[1] + 1]), 'black', lw=1)
    # plt.hlines(_max_50_1[0], 0, len(_ma50[:-_max_50_1[1] + 1]), 'black', lw=1)
    # plt.vlines(len(_ma50) - _min_50[1], np.min(_ma50[~np.isnan(_ma50)]), np.max(_ma50[~np.isnan(_ma50)]), 'black', lw=1)
    # plt.vlines(len(_ma50) - _max_50_1[1], np.min(_ma50[~np.isnan(_ma50)]), np.max(_ma50[~np.isnan(_ma50)]), 'black',
    #            lw=1)
    # plt.vlines(len(_ma50) - _max_50[1], np.min(_ma50[~np.isnan(_ma50)]), np.max(_ma50[~np.isnan(_ma50)]), 'black', lw=1)
    # plt.plot(_ma50[:-_max_50[1] + 1], 'yellow', lw=1)

    # plt.plot(_ma50[:-_max_50_1[1] + 1], 'red', lw=1)
    # plt.plot(ma20[start:stop:1], 'blue ', lw=1)
    plt.show()

    i = 1

    # ba = BuyAsset('ZRX', 0.00002520, 0.00002420, 0.00005520, 1)
    # take_profit(ba)
    i = 1


if __name__ == "__main__":
    main()
