import numpy as np
import pandas as pd
from bson.codec_options import TypeRegistry, CodecOptions
from mongodb import mongo_client

from library import setup_logger, DecimalCodec, save_to_file, get_pickled
from tb_lib import compute_tr, smooth, get_crossup, get_crossdn, lele, get_strong_major_indices, get_major_indices, \
    compute_adjustment, compute_money_strength, compute_whale_money_flow, compute_trend_exhaustion

db_klines = mongo_client.klines


def create_df(_klines):
    _open = list(map(lambda x: x['kline']['opening'], _klines))
    _close = list(map(lambda x: x['kline']['closing'], _klines))
    _high = list(map(lambda x: x['kline']['highest'], _klines))
    _low = list(map(lambda x: x['kline']['lowest'], _klines))
    _volume = list(map(lambda x: x['kline']['volume'], _klines))
    _time = list(map(lambda x: x['kline']['start_time'], _klines))
    _time_str = list(map(lambda x: x['kline']['time_str'], _klines))

    return pd.DataFrame(list(zip(_open, _close, _high, _low, _volume, _time, _time_str)),
                      columns=['open', 'close', 'high', 'low', 'volume', 'time', 'time_str'])

def min_max_scanner(_market_info_collection):
    _market_info_cursor = _market_info_collection.find()
    _market_info_list = [e for e in _market_info_cursor]
    _tickers = ['4h', '6h', '8h', '12h', '1d', '3d', '1w']

    _klines = get_pickled('D:\\bin\\data\\', "sol_usdt_8h")
    _klines_inc = _klines.copy()
    _klines.reverse()
    _df_inc = create_df(_klines_inc)
    _df_dec = create_df(_klines)

    _conjectures = list(map(lambda x: smooth(_df_dec['open'], x), np.arange(0.1, 1.0, 0.05)))
    _amlag = np.mean(_conjectures, axis=0)
    _tr = compute_tr(_df_dec)
    _inapproximability = np.mean(list(map(lambda x: smooth(_tr, x), np.arange(0.1, 1.0, 0.05))), axis=0)

    _upper_threshold_of_approximability1 = _amlag + _inapproximability * 1.618
    _upper_threshold_of_approximability2 = _amlag + 2 * _inapproximability * 1.618
    _lower_threshold_of_approximability1 = _amlag - _inapproximability * 1.618
    _lower_threshold_of_approximability2 = _amlag - 2 * _inapproximability * 1.618

    _strong_buy = get_crossup(_df_dec, _lower_threshold_of_approximability2)
    _strong_sell = get_crossdn(_df_dec, _upper_threshold_of_approximability2)

    _major = lele(_df_dec['open'], _df_dec['close'], _df_dec['high'], _df_dec['low'], 2, 20)  # bull/bear

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

    _adjustment = compute_adjustment(_df_inc['open'], _df_inc['close'], _df_inc['high'], _df_inc['low'], _df_inc['volume'])
    _money_strength = compute_money_strength(_df_inc['close'], _df_inc['volume'])
    _whale_money_flow = compute_whale_money_flow(_adjustment, _df_inc['volume'], _money_strength)
    _trend_exhaustion = compute_trend_exhaustion(_df_inc['open'], _df_inc['close'], _df_inc['high'], _df_inc['low'], _df_inc['volume'])


    k = 1

    # for _market_s in _market_info_list:  # inf loop needed here
    #     for _ticker in _tickers:
    #         _klines = []
    #         _collection = db_klines.get_collection("{}_{}_{}".format(_market_s['name'], _market_info_collection.name, _ticker), codec_options=codec_options)
    #         _cursor = _collection.find().sort("_id", -1)
    #         for _e in _cursor:
    #             _klines.append(_e)
    #             if len(_klines) > 399:
    #                 break
    #


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