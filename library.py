import time

import binance
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
exclude_markets = ['TFUELBTC', 'FTMBTC', 'PHBBTC', 'ONEBTC', 'BCCBTC', 'PHXBTC', 'BTCUSDT', 'HSRBTC', 'SALTBTC',
                   'SUBBTC',
                   'ICNBTC', 'MODBTC', 'VENBTC', 'WINGSBTC', 'TRIGBTC', 'CHATBTC', 'RPXBTC', 'CLOAKBTC', 'BCNBTC',
                   'TUSDBTC', 'PAXBTC', 'USDCBTC', 'BCHSVBTC']


class Asset(object):
    def __init__(self, name, price, ratio, ticker, barrier=False):
        self.name = name
        self.market = "{}BTC".format(name)
        self.price = price
        self.ratio = ratio  # buy for ratio [%] of all possessing BTC
        self.ticker = ticker
        self.barrier = barrier
        self.buy_price = None


class AssetTicker(object):
    def __init__(self, name, ticker, bid_price):
        self.name = name
        self.tickers = [ticker]
        self.bid_price = bid_price

    def add_ticker(self, ticker):
        self.tickers.append(ticker)


class Strategy(object):
    def __init__(self, asset, btc_value, params):
        self.asset = asset
        self.btc_value = btc_value
        self.params = params


class BuyStrategy(Strategy):
    def __init__(self, asset, btc_value, params):
        super().__init__(asset, btc_value, params)

    def run(self):
        _la = lowest_ask(self.asset.market)
        self.asset.buy_price = _la
        _possible_buying_quantity = get_buying_asset_quantity(self.asset, self.btc_value)
        _quantity_to_buy = adjust_quantity(_possible_buying_quantity, self.params)
        if _quantity_to_buy:
            buy_order(self.asset, _quantity_to_buy)


class BuyAsset(Asset):
    def __init__(self, name, price, ratio=50, ticker=Client.KLINE_INTERVAL_1MINUTE, barrier=False):
        super().__init__(name, price, ratio, ticker, barrier)


def observe_lower_price(_assets: Asset):
    while 1:
        for _asset in _assets:
            if stop_signal(get_market(_asset), _asset.ticker, get_interval_unit(_asset.ticker), _asset.price, 1):
                _assets = list(
                    filter(lambda _a: _a.name != _asset.name, _assets))  # remove an observed asset from the list
                _btc_value = get_remaining_btc()
                _params = get_lot_size_params(_asset.market)
                if is_buy_possible(_asset, _btc_value, _params):
                    BuyStrategy(_asset, _btc_value, _params).run()
                else:
                    return
                if len(_assets) == 0:
                    return
        time.sleep(40)


def is_buy_possible(_asset, _btc_value, _params):
    _min_amount = float(_params['minQty']) * _asset.price
    return 0.01 < _btc_value > _min_amount


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

binance = Binance(keys[0], keys[1])

sat = 1e-8

general_fee = 0.001


def get_interval_unit(_ticker):
    return {
        Client.KLINE_INTERVAL_1MINUTE: "6 hours ago",
        Client.KLINE_INTERVAL_15MINUTE: "40 hours ago",
        Client.KLINE_INTERVAL_30MINUTE: "75 hours ago",
        Client.KLINE_INTERVAL_1HOUR: "150 hours ago",
        Client.KLINE_INTERVAL_2HOUR: "300 hours ago",
        Client.KLINE_INTERVAL_4HOUR: "600 hours ago",
        Client.KLINE_INTERVAL_6HOUR: "900 hours ago",
        Client.KLINE_INTERVAL_8HOUR: "1200 hours ago",
        Client.KLINE_INTERVAL_12HOUR: "75 days ago",
        Client.KLINE_INTERVAL_1DAY: "150 days ago",
        Client.KLINE_INTERVAL_3DAY: "360 days ago",
    }[_ticker]


def stop_signal(market, time_interval, time0, stop_price, _times=4):
    _klines = binance.get_klines_currency(market, time_interval, time0)
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
    _cancelled_ok = True
    if len(_open_orders):
        _cancelled_ok = False
        _cancelled_ok = cancel_orders(_open_orders, market)
    if _cancelled_ok:
        print("{} Orders cancelled correctly".format(market))


def get_asset_quantity(asset):
    return float(client.get_asset_balance(asset)['free'])


def get_lot_size_params(market):
    client.get_symbol_info(market)
    _info = list(filter(lambda f: f['filterType'] == "LOT_SIZE", client.get_symbol_info(market)['filters']))
    return _info[0] if len(_info) > 0 else False


def get_buying_asset_quantity(asset, total_btc):
    _useable_btc = (1 - general_fee) * asset.ratio / 100 * total_btc
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
    _sell_price_str = price_to_string(_asset.price)
    _resp = client.order_limit_buy(symbol=_asset.market, quantity=_quantity, price=_sell_price_str)
    logger_global[0].info(
        "{} Buy limit order placed: price={} BTC, quantity={} ".format(_asset.market, _sell_price_str, _quantity))


def price_to_string(_price):
    return "{:.8f}".format(_price)


def sell_order(market, _sell_price, _quantity):
    _sell_price_str = "{:.8f}".format(_sell_price)
    _resp = client.order_limit_sell(symbol=market, quantity=_quantity, price=_sell_price_str)
    logger_global[0].info(
        "{} Sell limit order placed: price={} BTC, quantity={} ".format(market, _sell_price_str, _quantity))


def sell_limit(market, asset):
    cancel_current_orders(market)
    _quantity = get_asset_quantity(asset)
    _sell_price = get_sell_price(market)
    _lot_size_params = get_lot_size_params(market)
    _quantity = adjust_quantity(_quantity, _lot_size_params)
    if _quantity:
        sell_order(market, _sell_price, _quantity)


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
