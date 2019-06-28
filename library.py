import sys

import talib
import threading
import time
import traceback

import requests
from binance.client import Client
import numpy as np
import pickle
import configparser
import logging
import logging.config
from Binance import Binance


class Config(object):
    def __init__(self, section='local', name='resource/config.properties'):
        config = configparser.RawConfigParser()
        config.read(name)
        self.config = dict(config.items(section))

    def get_parameter(self, parameter):
        if parameter in self.config:
            return self.config[parameter]
        raise Exception("There is no such a key in config!")


config = Config()

key_dir = config.get_parameter('key_dir')
logger_global = []
exclude_markets = ['BCCBTC', 'PHXBTC', 'BTCUSDT', 'HSRBTC',
                   'SALTBTC',
                   'SUBBTC',
                   'ICNBTC', 'MODBTC', 'VENBTC', 'WINGSBTC', 'TRIGBTC', 'CHATBTC', 'RPXBTC', 'CLOAKBTC', 'BCNBTC',
                   'TUSDBTC', 'PAXBTC', 'USDCBTC', 'BCHSVBTC']


class Asset(object):
    def __init__(self, name, stop_loss_price, price_profit, profit, ticker, barrier=False):
        self.name = name
        self.market = "{}BTC".format(name)
        self.stop_loss_price = stop_loss_price
        self.price_profit = price_profit
        self.profit = profit  # taking profit only when it's higher than profit %
        self.ticker = ticker
        self.barrier = barrier
        self.buy_price = None


class BuyAsset(Asset):
    def __init__(self, name, price, stop_loss_price, price_profit, ratio=50, profit=5,
                 ticker=Client.KLINE_INTERVAL_1MINUTE, barrier=False):
        super().__init__(name, stop_loss_price, price_profit, profit, ticker, barrier)
        self.price = price
        self.ratio = ratio  # buying ratio [%] of all possessed BTC

    def set_btc_asset_buy_value(self, _total_btc):
        self.btc_asset_buy_value = self.ratio / 100 * _total_btc


class ObserveAsset(Asset):
    def __init__(self, name, buy_price, stop_loss_price, price_profit, profit=5, ticker=Client.KLINE_INTERVAL_1MINUTE,
                 barrier=False):
        super().__init__(name, stop_loss_price, price_profit, profit, ticker, barrier)
        self.buy_price = buy_price


class AssetTicker(object):
    def __init__(self, name, ticker, bid_price):
        self.name = name
        self.tickers = [ticker]
        self.bid_price = bid_price

    def add_ticker(self, ticker):
        self.tickers.append(ticker)


class Strategy(object):
    def __init__(self, asset):
        self.asset = asset

    def set_stop_loss(self):
        _stop_loss_maker = threading.Thread(target=stop_loss, args=(self.asset,),
                                            name='_stop_loss_maker_{}'.format(self.asset.name))
        _stop_loss_maker.start()

    def set_take_profit(self):
        _take_profit_maker = threading.Thread(target=take_profit, args=(self.asset,),
                                              name='_take_profit_maker_{}'.format(self.asset.name))
        _take_profit_maker.start()


class BuyStrategy(Strategy):
    def __init__(self, asset, btc_value, params):
        super().__init__(asset)
        self.btc_value = btc_value
        self.params = params
        logger_global[0].info("{} BuyStrategy object has been created".format(self.asset.market))

    def run(self):
        _la = lowest_ask(self.asset.market)
        self.asset.buy_price = _la
        _possible_buying_quantity = get_buying_asset_quantity(self.asset, self.btc_value)
        _quantity_to_buy = adjust_quantity(_possible_buying_quantity, self.params)
        if _quantity_to_buy:
            _order_id = buy_order(self.asset, _quantity_to_buy)
            self.set_stop_loss()
            wait_until_order_filled(self.asset.market, _order_id)
            sell_limit(self.asset.market, self.asset.name, self.asset.price_profit)
            self.set_take_profit()


class ObserverStrategy(Strategy):
    def __init__(self, asset):
        super().__init__(asset)
        logger_global[0].info("{} ObserverStrategy object has been created".format(self.asset.market))

    def run(self):
        self.set_stop_loss()
        sell_limit(self.asset.market, self.asset.name, self.asset.price_profit)
        self.set_take_profit()


def wait_until_order_filled(_market, _order_id):
    _status = {'status': None}
    while _status['status'] != 'FILLED':
        _status = client.get_order(symbol=_market, orderId=_order_id)
        time.sleep(1)
    logger_global[0].info("{} OrderId : {} has been filled".format(_market, _order_id))


def observe_lower_price(_assets: Asset):
    while 1:
        for _asset in _assets:
            if stop_signal(get_market(_asset), _asset.ticker, get_interval_unit(_asset.ticker), _asset.price, 1):
                _assets = list(
                    filter(lambda _a: _a.name != _asset.name, _assets))  # remove the observed asset from the list
                _btc_value = get_remaining_btc()
                _params = get_lot_size_params(_asset.market)
                if is_buy_possible(_asset, _btc_value, _params):
                    BuyStrategy(_asset, _btc_value, _params).run()
                else:
                    logger_global[0].warning(
                        "{} buying not POSSIBLE, only {} BTC left".format(_asset.market, price_to_string(_btc_value)))
                    return
                if len(_assets) == 0:
                    logger_global[0].info("All assets OBSERVED, exiting")
                    return
        time.sleep(40)


def is_buy_possible(_asset, _btc_value, _params):
    _min_amount = float(_params['minQty']) * _asset.price
    return 0.01 < _btc_value > _min_amount
    # return True


def get_remaining_btc():
    return get_asset_quantity("BTC")


def get_market(_asset):
    return "{}BTC".format(_asset.name)


class SellAsset(Asset):
    pass


def save_to_file(_dir, filename, obj):
    with open(_dir + filename + '.pkl', 'wb') as handle:
        pickle.dump(obj, handle, protocol=pickle.HIGHEST_PROTOCOL)
        handle.close()


def get_pickled(_dir, filename):
    with open(_dir + filename + '.pkl', 'rb') as handle:
        data = pickle.load(handle)
        handle.close()
        return data


keys = get_pickled(key_dir, ".keys")

client = Client(keys[0], keys[1])

binance_obj = Binance(keys[0], keys[1])

sat = 1e-8

general_fee = 0.001


def get_interval_unit(_ticker):
    return {
        Client.KLINE_INTERVAL_1MINUTE: "15 hours ago",
        Client.KLINE_INTERVAL_15MINUTE: "40 hours ago",
        Client.KLINE_INTERVAL_30MINUTE: "75 hours ago",
        Client.KLINE_INTERVAL_1HOUR: "150 hours ago",
        Client.KLINE_INTERVAL_2HOUR: "300 hours ago",
        Client.KLINE_INTERVAL_4HOUR: "600 hours ago",
        Client.KLINE_INTERVAL_6HOUR: "900 hours ago",
        Client.KLINE_INTERVAL_8HOUR: "1200 hours ago",
        Client.KLINE_INTERVAL_12HOUR: "75 days ago",
        Client.KLINE_INTERVAL_1DAY: "150 days ago",
        Client.KLINE_INTERVAL_3DAY: "350 days ago",
    }[_ticker]


def stop_signal(market, time_interval, time0, stop_price, _times=4):
    _klines = binance_obj.get_klines_currency(market, time_interval, time0)
    if len(_klines) > 0:
        _mean_close_price = np.mean(list(map(lambda x: float(x[4]), _klines[-_times:])))
        return True if _mean_close_price <= stop_price else False


def get_sell_price(market):
    _depth = client.get_order_book(symbol=market)
    _highest_bid = float(_depth['bids'][0][0])
    _sell_price = _highest_bid + sat
    return _sell_price


def highest_bid(market):
    _depth = client.get_order_book(symbol=market)
    return float(_depth['bids'][0][0])


def lowest_ask(market):
    _depth = client.get_order_book(symbol=market)
    return float(_depth['asks'][0][0])


def cancel_orders(open_orders, symbol):
    _resp = []
    for _order in open_orders:
        _resp.append(client.cancel_order(symbol=symbol, orderId=_order['orderId']))
    return all(_c["status"] == "CANCELED" for _c in _resp)


def cancel_current_orders(market):
    _open_orders = client.get_open_orders(symbol=market)
    logger_global[0].info("{} orders to cancel : {}".format(market, len(_open_orders)))
    _cancelled_ok = True
    if len(_open_orders) > 0:
        _cancelled_ok = False
        _cancelled_ok = cancel_orders(_open_orders, market)
    else:
        logger_global[0].warning("{} No orders to CANCEL".format(market))
        return
    if _cancelled_ok:
        logger_global[0].info("{} Orders cancelled correctly".format(market))
    else:
        logger_global[0].error("{} Orders not cancelled properly".format(market))


def get_asset_quantity(asset):
    return float(client.get_asset_balance(asset)['free'])


def get_lot_size_params(market):
    client.get_symbol_info(market)
    _info = list(filter(lambda f: f['filterType'] == "LOT_SIZE", client.get_symbol_info(market)['filters']))
    return _info[0] if len(_info) > 0 else False


def get_buying_asset_quantity(asset, total_btc):
    # _useable_btc = (1 - general_fee) * asset.ratio / 100 * total_btc
    _useable_btc = (1 - general_fee) * asset.btc_asset_buy_value
    return _useable_btc / asset.buy_price


def adjust_quantity(quantity, lot_size_params):
    _min_sell_amount = float(lot_size_params['minQty'])
    _diff = quantity - _min_sell_amount
    if _diff < 0:
        return False
    else:
        _power = int(np.log10(float(lot_size_params['stepSize'])))
        _adjusted_quantity = round(quantity, _power)
        if _adjusted_quantity > quantity:
            _adjusted_quantity -= _min_sell_amount
        return _adjusted_quantity


def buy_order(_asset, _quantity):
    _price_str = price_to_string(_asset.price)
    _resp = client.order_limit_buy(symbol=_asset.market, quantity=_quantity, price=_price_str)
    logger_global[0].info(
        "{} Buy limit order (ID : {}) placed: price={} BTC, quantity={} ".format(_asset.market, _resp['orderId'],
                                                                                 _price_str, _quantity))
    return _resp['orderId']


def price_to_string(_price):
    return "{:.8f}".format(_price)


def _sell_order(market, _sell_price, _quantity):
    _sell_price_str = price_to_string(_sell_price)
    _resp = client.order_limit_sell(symbol=market, quantity=_quantity, price=_sell_price_str)
    logger_global[0].info(
        "{} Sell limit order placed: price={} BTC, quantity={} ".format(market, _sell_price_str, _quantity))


def sell_limit(market, asset_name, price):
    cancel_current_orders(market)
    _quantity = get_asset_quantity(asset_name)
    _lot_size_params = get_lot_size_params(market)
    _quantity = adjust_quantity(_quantity, _lot_size_params)
    if _quantity:
        _sell_order(market, price, _quantity)
        return True
    else:
        logger_global[0].error("{} No quantity to SELL".format(market))
        return False


def sell_limit_stop_loss(market, asset):
    cancel_current_orders(market)
    _quantity = get_asset_quantity(asset)
    _sell_price = get_sell_price(market)
    _lot_size_params = get_lot_size_params(market)
    _quantity = adjust_quantity(_quantity, _lot_size_params)
    if _quantity:
        _sell_order(market, _sell_price, _quantity)


def setup_logger(symbol):
    LOGGER_FILE = config.get_parameter('logger_file').format(symbol)
    formater_str = '%(asctime)s,%(msecs)d %(levelname)s %(name)s: %(message)s'
    formatter = logging.Formatter(formater_str)
    logging.config.fileConfig(fname='logging.conf')
    logger = logging.getLogger(symbol)
    file_handler = logging.FileHandler(LOGGER_FILE)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger_global.append(logger)

    return logger


def stop_loss(_asset):
    _ticker = Client.KLINE_INTERVAL_1MINUTE
    _time_interval = "6 hours ago"
    _stop_price = _asset.stop_loss_price

    logger_global[0].info("Starting {} stop-loss maker".format(_asset.market))
    logger_global[0].info("Stop price {} is set up to : {:.8f} BTC".format(_asset.market, _stop_price))

    while 1:
        try:
            _stop_sl = stop_signal(_asset.market, _ticker, _time_interval, _stop_price, 1)
            # stop = True
            if _stop_sl:
                sell_limit_stop_loss(_asset.market, _asset.name)
                logger_global[0].info("Stop-loss LIMIT {} order has been made, exiting".format(_asset.market))
                sys.exit(0)
            time.sleep(50)
        except Exception as err:
            if isinstance(err, requests.exceptions.ConnectionError):
                logger_global[0].error("Connection problem...")
            else:
                traceback.print_tb(err.__traceback__)
                logger_global[0].exception(err.__traceback__)
                time.sleep(50)


def is_profitable(asset, curr_price):
    return (curr_price - asset.buy_price) / asset.buy_price >= asset.profit / 100.0


def adjust_ask_price(asset, _prev_kline, _old_price, _high_price_max, _curr_high):
    _hp = float(_prev_kline[2])  # high price
    if _hp - _high_price_max > 0 or np.abs(_hp - _high_price_max) / _hp < 0.005:
        _hp = _curr_high
        logger_global[0].info(
            "{} ask price adjusted with current high price : {} -> {}".format(asset.market, _old_price, _hp))
    else:
        logger_global[0].info(
            "{} ask price adjusted with previous candle high price : {} -> {}".format(asset.market, _old_price, _hp))
    return _hp


def take_profit(asset):
    _ticker = Client.KLINE_INTERVAL_1MINUTE
    _time_interval = get_interval_unit(_ticker)
    _prev_kline = None
    _time_frame = 60  # last 60 candles
    while 1:
        try:
            _klines = binance_obj.get_klines_currency(asset.market, _ticker, _time_interval)
            # _klines = get_pickled('/juno/', "klines")
            # _klines = _klines[33:-871]
            _closes = get_closes(_klines)
            _highs = get_highs(_klines)

            _ma50 = talib.MA(_closes, timeperiod=50)
            _ma20 = talib.MA(_closes, timeperiod=20)
            _ma7 = talib.MA(_closes, timeperiod=7)

            _local_rsi_max_value = get_rsi_local_max_value(_closes)

            _stop = -1
            _last_candle = _klines[-1]

            # plt.plot(_ma50[0:_stop:1], 'red', lw=1)
            # plt.plot(_ma20[0:_stop:1], 'blue', lw=1)
            # plt.plot(_ma7[0:_stop:1], 'green', lw=1)
            # plt.show()

            _high_price_max = np.max(get_last(_highs, _stop, _time_frame))

            _rsi = relative_strength_index(_closes)
            _rsi_max = np.max(get_last(_rsi, _stop, _time_frame))
            _index_rsi_peak = np.where(_rsi == _rsi_max)[0][0]
            _curr_rsi = get_last(_rsi, _stop)

            _curr_ma_50 = get_last(_ma50, _stop)
            _curr_ma_20 = get_last(_ma20, _stop)
            _curr_ma_7 = get_last(_ma7, _stop)

            _max_volume = get_max_volume(_klines, _time_frame)

            _c1 = rsi_falling_condition(_rsi_max, _curr_rsi, _local_rsi_max_value)
            _c2 = volume_condition(_klines, _max_volume)
            _c3 = candle_condition(_last_candle, _curr_ma_7, _curr_ma_50)
            _c4 = mas_condition(_curr_ma_7, _curr_ma_20, _curr_ma_50)
            _c5 = is_profitable(asset, _closes[-1])
            _c6 = is_red_candle(_last_candle)

            if _c6 and _c5 and _c1 and _c2 and _c3 and _c4:
                logger_global[0].info("Taking profits {} conditions satisfied...".format(asset.market))
                _curr_open = float(_last_candle[1])
                _ask_price = _curr_open
                if _curr_open < _curr_ma_7:
                    _curr_high = float(_last_candle[2])
                    _ask_price = adjust_ask_price(asset, _prev_kline, _ask_price, _high_price_max, _curr_high)
                _sold = sell_limit(asset.market, asset.name, _ask_price)
                if _sold:
                    logger_global[0].info("Took profits {}: LIMIT order has been made, exiting".format(asset.market))
                else:
                    logger_global[0].error("Took profits {}: selling not POSSIBLE, exiting".format(asset.market))
                sys.exit(0)
            _prev_kline = _last_candle
            time.sleep(40)
        except Exception as err:
            if isinstance(err, requests.exceptions.ConnectionError):
                logger_global[0].error("{} Connection problem...".format(asset.market))
            else:
                traceback.print_tb(err.__traceback__)
                logger_global[0].exception(err.__traceback__)
                time.sleep(40)


def is_red_candle(_kline):
    __close = float(_kline[4])-float(_kline[4])
    __open = float(_kline[4])-float(_kline[1])
    return __close - __open > 0


def get_rsi_local_max_value(_closes, _window=10):
    _start = 33
    _stop = -1
    _rsi = relative_strength_index(_closes)
    _rsi_max_val, _rsi_reversed_max_ind = find_maximum(_rsi[_start:_stop:1], _window)
    return _rsi_max_val


def get_max_volume(_klines, _time_frame):
    return np.max(
        list(map(lambda x: float(x[7]), _klines[-int(_time_frame / 2):])))  # get max volume within last 30 klines


def mas_condition(_curr_ma_7, _curr_ma_20, _curr_ma_50):
    return _curr_ma_7 > _curr_ma_20 > _curr_ma_50


def candle_condition(_kline, _curr_ma_7, _curr_ma_50):
    _close = float(_kline[4])
    _low = float(_kline[3])
    return _close < _curr_ma_7 or _low < _curr_ma_50


def volume_condition(_klines, _max_volume):
    _vol_curr = float(_klines[-1][7])
    return 10.0 > _vol_curr / _max_volume > 0.6


def rsi_falling_condition(_rsi_max, _curr_rsi, _local_rsi_max_value):
    return _local_rsi_max_value > 70 and _rsi_max - _curr_rsi > 0 and _rsi_max > 76.0 and _curr_rsi < 65.0 and (_rsi_max - _curr_rsi) / _rsi_max > 0.2


def get_closes(_klines):
    return np.array(list(map(lambda _x: float(_x[4]), _klines)))


def get_highs(_klines):
    return np.array(list(map(lambda _x: float(_x[2]), _klines)))


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


def get_avg_last(_values, _stop, _window=2):
    return np.mean(_values[_stop - _window + 1:])


def get_last(_values, _stop, _window=1):
    return _values[_stop - _window + 1:]


def get_avg_last_2(_values, _stop, _window=2):
    return np.mean(_values[_stop - _window + 1:_stop])


def get_last_2(_values, _stop, _window=1):
    return _values[_stop + 1]


def check_buy_assets(assets):
    logger_global[0].info("Checking BuyAsset prices...")
    if not all(x.price_profit > x.price > x.stop_loss_price for x in assets):
        logger_global[0].error("BuyAsset prices not coherent, stopped")
        raise Exception("BuyAsset prices not coherent, stopped")
    logger_global[0].info("BuyAsset prices : OK")


def check_observe_assets(assets):
    logger_global[0].info("Checking ObserveAsset prices...")
    if not all(x.price_profit > x.buy_price > x.stop_loss_price for x in assets):
        logger_global[0].error("ObserveAsset prices not coherent, stopped")
        raise Exception("ObserveAsset prices not coherent, stopped")
    logger_global[0].info("ObserveAsset prices : OK")


def adjust_buy_asset_btc_volume(_buy_assets, _btc_value):
    list(map(lambda x: x.set_btc_asset_buy_value(_btc_value), _buy_assets))


def find_maximum(values, window):
    _range = int(len(values) / window)
    _max_val = -1
    _min_stop_level = 0.9
    _activate_stop = False
    _max_ind = -1
    for _i in range(0, _range - 1):
        _i_max = np.max(values[len(values) - (_i + 1) * window - 1:len(values) - _i * window - 1])
        _tmp = list(values[len(values) - (_i + 1) * window - 1:len(values) - _i * window - 1])
        _index = window - _tmp.index(max(_tmp)) + _i * window + 1
        if _i_max > _max_val:
            _max_val = _i_max
            _max_ind = _index
            if _max_val > 0:
                _activate_stop = True
        if _activate_stop and _i_max < _min_stop_level * _max_val:
            return _max_val, _max_ind
    return _max_val, _max_ind
