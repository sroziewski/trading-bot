import binance
from binance.client import Client
import numpy as np
import pickle
import logging
import logging.config
from Binance import Binance

ssh_dir = '/home/szymon/.config/'
logger_global = []
exclude_markets = ['TFUELBTC', 'PHBBTC', 'ONEBTC', 'BCCBTC', 'PHXBTC', 'BTCUSDT', 'HSRBTC', 'SALTBTC', 'SUBBTC', 'ICNBTC', 'MODBTC', 'VENBTC', 'WINGSBTC', 'TRIGBTC', 'CHATBTC', 'RPXBTC', 'CLOAKBTC', 'BCNBTC', 'TUSDBTC', 'PAXBTC', 'USDCBTC', 'BCHSVBTC']


def save_to_file(_dir, filename, obj):
    with open(_dir + filename + '.pkl', 'wb') as handle:
        pickle.dump(obj, handle, protocol=pickle.HIGHEST_PROTOCOL)
        handle.close()


def get_pickled(_dir, filename):
    with open(_dir + filename + '.pkl', 'rb') as handle:
        data = pickle.load(handle)
        handle.close()
        return data


keys = get_pickled(ssh_dir, ".keys")

client = Client(keys[0], keys[1])

binance = Binance(keys[0], keys[1])

sat = 1e-8


def stop_signal(market, time_interval, time0, stop_price):
    _klines = binance.get_klines_currency(market, time_interval, time0)
    if len(_klines) > 0:
        _mean_close_price = np.mean(list(map(lambda x: float(x[4]), _klines[-4:])))
        return True if _mean_close_price <= stop_price else False


def get_sell_price(market):
    _depth = client.get_order_book(symbol=market)
    _highest_bid = float(_depth['bids'][0][0])
    _sell_price = _highest_bid + sat
    return _sell_price


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


def sell_order(market, _sell_price, _quantity):
    _sell_price_str = "{:.8f}".format(_sell_price)
    _resp = client.order_limit_sell(symbol=market, quantity=_quantity, price=_sell_price_str)
    logger_global[0].info("{} Sell limit order placed: price={} BTC, quantity={} ".format(market, _sell_price_str, _quantity))


def sell_limit(market, asset):
    cancel_current_orders(market)
    _quantity = get_asset_quantity(asset)
    _sell_price = get_sell_price(market)
    _lot_size_params = get_lot_size_params(market)
    _quantity = adjust_quantity(_quantity, _lot_size_params)
    if _quantity:
        sell_order(market, _sell_price, _quantity)


def setup_logger(symbol):
    LOGGER_FILE = "/var/log/szymon/trader-{}.log".format(symbol)
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

