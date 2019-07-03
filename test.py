import talib
import time
import traceback
import matplotlib.pyplot as plt

import requests

from library import BullishStrategy, TradeAsset, get_remaining_btc, adjust_buy_asset_btc_volume, get_lot_size_params, \
    get_interval_unit, lowest_ask, is_buy_possible, setup_logger, get_buying_asset_quantity, \
    adjust_quantity, adjust_stop_loss_price, adjust_price_profit, TimeTuple, relative_strength_index, get_closes, \
    get_max_volume, is_red_candle, is_fresh, volume_condition, binance_obj, save_to_file, trades_logs_dir, get_pickled, \
    get_last, is_fresh_test


def main():
    _asset = TradeAsset('CELR')
    start_buy_local_bottom_test(_asset)
    # show_klines(_asset)
    # generate_klines(_asset)


def generate_klines(asset):
    _time_interval = get_interval_unit(asset.ticker)
    _klines = binance_obj.get_klines_currency(asset.market, asset.ticker, _time_interval)
    save_to_file(trades_logs_dir, "test_klines_{}".format(asset.market), _klines)


def show_klines(_asset):
    _klines = get_pickled(trades_logs_dir, "test_klines_{}".format(_asset.market))
    _closes = get_closes(_klines)
    _ma7 = talib.MA(_closes, timeperiod=7)
    _ma50 = talib.MA(_closes, timeperiod=50)
    r = relative_strength_index(_closes)
    plt.subplot2grid((3, 1), (1, 0))
    plt.plot(_ma7, 'pink', lw=1)
    plt.plot(_ma50, 'red', lw=1)
    plt.subplot2grid((3, 1), (2, 0))
    plt.plot(r, 'red', lw=1)
    plt.show()


def start_buy_local_bottom_test(_asset):
    _klines = get_pickled(trades_logs_dir, "test_klines_{}".format(_asset.market))
    _klines_tail = get_last(_klines, -1, 300)
    for _i in range(15, len(_klines_tail)):
        _klines_test = _klines_tail[0:_i]
        buy_local_bottom_test(_klines_test, _i)



def buy_local_bottom_test(_klines, _i):
    logger = setup_logger("test")
    _btc_value = 0.1
    _trade_asset = TradeAsset('CELR')
    # adjust_buy_asset_btc_volume(_trade_asset, _btc_value)
    _params = []

    strategy = BullishStrategy(_trade_asset, _btc_value, _params)

    _time_interval = get_interval_unit(strategy.asset.ticker)
    _time_frame_rsi = 50
    _time_horizon = 60
    _time_horizon_long = 360
    _prev_rsi_high = False
    _trigger = False
    _rsi_low = False
    # while 1:
    try:
        strategy.asset.price = lowest_ask(strategy.asset.market)
        # if not is_buy_possible(strategy.asset, strategy.btc_value, strategy.params):
        #     strategy.asset.running = False
        #     logger.info(
        #         "{} buy_local_bottom : buy not possible, skipping, exiting".format(strategy.asset.market))

        # _klines = binance_obj.get_klines_currency(strategy.asset.market, strategy.asset.ticker, _time_interval)
        _curr_kline = _klines[-1]
        _closes = get_closes(_klines)
        _rsi = relative_strength_index(_closes, 14, strategy.asset)

        if _rsi[-1] < 30:
            ii = 1

        if _rsi[-1] > 70:
            _prev_rsi_high = TimeTuple(_rsi[-1], _curr_kline[0])

        _max_volume = get_max_volume(_klines, _time_horizon)

        if _rsi[-1] < 33.5 and not is_fresh_test(_prev_rsi_high, _time_frame_rsi, _curr_kline[0]):
            _max_volume = get_max_volume(_klines, _time_horizon_long)
            if volume_condition(_klines, _max_volume, 0.9):
                _rsi_low = TimeTuple(_rsi[-1], _curr_kline[0])

        if not _rsi_low and _rsi[-1] < 31 and not is_fresh_test(_prev_rsi_high, _time_frame_rsi, _curr_kline[0]) \
                and volume_condition(_klines, _max_volume, 0.5):
            _rsi_low = TimeTuple(_rsi[-1], _curr_kline[0])

        if not _rsi_low and _rsi[-1] < 20:
            _rsi_low = TimeTuple(_rsi[-1], _curr_kline[0])

        if _rsi_low and _rsi[-1] < 33.5 and is_fresh_test(_rsi_low, _time_frame_rsi, _curr_kline[0]) and not is_fresh_test(_rsi_low, 15, _curr_kline[0]) and \
                _rsi[-1] > _rsi_low.value:
            _max_volume = get_max_volume(_klines, 10)
            # if _rsi[-1] > _rsi_low[0] and volume_condition(_klines, _max_volume, 0.3):  # RSI HL
            if volume_condition(_klines, _max_volume, 0.3):  # RSI HL
                _trigger = TimeTuple(True, _curr_kline[0])

        _ma7 = talib.MA(_closes, timeperiod=7)
        _open = float(_curr_kline[1])
        _close = _closes[-1]
        _max_volume = get_max_volume(_klines, 15)

        if _rsi_low and _close - _ma7[-1] > 0 and _rsi[-1] > _rsi_low.value and volume_condition(_klines, _max_volume,
                                                                                              1.0):  # reversal
            _trigger = TimeTuple(True, _curr_kline[0])

        if _trigger:
            logger.info("{} Buy Local Bottom triggered...".format(strategy.asset.market))
            _la = lowest_ask(strategy.asset.market)
            strategy.asset.buy_price = _la
            _possible_buying_quantity = get_buying_asset_quantity(strategy.asset, strategy.btc_value)
            _quantity_to_buy = adjust_quantity(_possible_buying_quantity, strategy.params)
            if _quantity_to_buy and is_buy_possible(strategy.asset, strategy.btc_value, strategy.params):
                strategy.asset.trading = True
                # _order_id = buy_order(strategy.asset, _quantity_to_buy)
                adjust_stop_loss_price(strategy.asset)
                adjust_price_profit(strategy.asset)
                strategy.set_stop_loss()
                # wait_until_order_filled(strategy.asset.market, _order_id)
                # sell_limit(strategy.asset.market, strategy.asset.name, strategy.asset.price_profit)
                strategy.set_take_profit()
                logger.info(
                    "{} Bought Local Bottom : price : {} value : {} BTC, exiting".format(strategy.asset.market,
                                                                                         strategy.asset.buy_price,
                                                                                         strategy.btc_value))
                strategy.asset.running = False
                # save_to_file(trades_logs_dir, "buy_klines_{}".format(time.time()), _klines)
        _prev_rsi = TimeTuple(_rsi[-1], _curr_kline[0])
        # time.sleep(1)
    except Exception as err:
        if isinstance(err, requests.exceptions.ConnectionError):
            logger.error("Connection problem...")
        else:
            traceback.print_tb(err.__traceback__)
            logger.exception(err.__traceback__)
            # time.sleep(45)


if __name__ == "__main__":
    main()