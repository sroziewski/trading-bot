import csv
import datetime
import hashlib
import logging
import logging.config
import os
import pickle
import smtplib
import ssl
import sys
import threading
import time
import traceback
import warnings
from datetime import timedelta
from decimal import Decimal
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from getpass import getpass
from operator import attrgetter
from os import path
from random import randrange

import numpy as np
import requests
import talib
from binance.client import Client as BinanceClient
from bson import Decimal128
from bson.codec_options import TypeCodec
from kucoin.client import Client as KucoinClient
from pymongo import DESCENDING
from pymongo.errors import PyMongoError, InvalidDocument
from urllib3.exceptions import ReadTimeoutError

from Binance import Binance
from config import config

warnings.filterwarnings('error')

variable = ''
key_dir = config.get_parameter('key_dir')
keys_filename = config.get_parameter('keys_filename')
trades_logs_dir = config.get_parameter('trades_logs_dir')
logger_global = []
exclude_markets = ['BCCBTC', 'PHXBTC', 'BTCUSDT', 'HSRBTC',
                   'SALTBTC',
                   'SUBBTC',
                   'ICNBTC', 'MODBTC', 'VENBTC', 'WINGSBTC', 'TRIGBTC', 'CHATBTC', 'RPXBTC', 'CLOAKBTC', 'BCNBTC',
                   'TUSDBTC', 'PAXBTC', 'USDCBTC', 'BCHSVBTC']


class DecimalCodec(TypeCodec):
    python_type = Decimal  # the Python type acted upon by this type codec
    bson_type = Decimal128  # the BSON type acted upon by this type codec

    def transform_python(self, value):
        return Decimal128(value)

    def transform_bson(self, value):
        """Function that transforms a vanilla BSON type value into our custom type."""
        return value.to_decimal()


class Kline(object):
    def __init__(self, start_time, opening, closing, highest, lowest, volume, btc_volume, time_str, ticker=None):
        self.start_time = start_time
        self.opening = opening
        self.closing = closing
        self.highest = highest
        self.lowest = lowest
        self.volume = volume
        self.btc_volume = btc_volume
        self.time_str = time_str
        self.buy_btc_volume = 0
        self.buy_quantity = 0
        self.sell_btc_volume = 0
        self.sell_quantity = 0
        self.bid_depth = None
        self.ask_depth = None
        if ticker:
            self.ticker = ticker

    def add_buy_depth(self, _bd):
        self.bid_depth = _bd

    def add_sell_depth(self, _sd):
        self.ask_depth = _sd

    def add_market(self, _market):
        self.market = _market

    def add_exchange(self, exchange):
        self.exchange = exchange

    def add_trade_volumes(self, buy_btc_volume, buy_quantity, sell_btc_volume, sell_quantity):
        self.buy_btc_volume = buy_btc_volume
        self.buy_quantity = buy_quantity
        self.sell_btc_volume = sell_btc_volume
        self.sell_quantity = sell_quantity


def from_kucoin_klines(klines, ticker=None):
    if klines and ticker:
        return list(
            map(lambda x: Kline(x[0], float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5]), float(x[6]),
                                get_time(int(x[0])), ticker), klines))
    elif klines:
        return list(
            map(lambda x: Kline(x[0], float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5]), float(x[6]),
                                get_time(int(x[0]))), klines))
    else:
        return []


def from_binance_klines(klines, ticker=None):
    if klines and ticker:
        return list(
            map(lambda x: Kline(x[0], float(x[1]), float(x[4]), float(x[2]), float(x[3]), float(x[5]), float(x[7]),
                                get_time_from_binance_tmstmp(x[0]), ticker), klines))
    elif klines:
        return list(
            map(lambda x: Kline(x[0], float(x[1]), float(x[4]), float(x[2]), float(x[3]), float(x[5]), float(x[7]),
                                get_time_from_binance_tmstmp(x[0])), klines))
    else:
        return []


def from_binance_socket_kline(_kline_msg):
    return Kline(_kline_msg['t'], float(_kline_msg['o']), float(_kline_msg['c']), float(_kline_msg['h']), float(_kline_msg['l']), float(_kline_msg['v']), float(_kline_msg['q']), get_time_from_binance_tmstmp(_kline_msg['t']), _kline_msg['i'])


def get_kucoin_klines(market, ticker, start=None):
    _data = from_kucoin_klines(kucoin_client.get_kline_data(market, ticker, start), ticker)
    _data.reverse()
    return _data


def get_binance_klines(market, ticker, start=None):
    _data = get_klines(market, ticker, start)
    return from_binance_klines(_data, ticker)


def ticker_to_kucoin(_ticker):
    return _ticker.replace("m", "min").replace("h", "hour").replace("d", "day").replace("w", "week")


def check_kucoin_offer_validity(_asset, return_value=False):
    _exit = False
    _market_price = None
    if _asset.kucoin_side == KucoinClient.SIDE_BUY:
        _market_bid = float(kucoin_client.get_order_book(_asset.market)['bids'][0][0])
        if _asset.price - _market_bid >= 0.01 * sat:
            _market_price = _market_bid
            _exit = True
    elif _asset.kucoin_side == KucoinClient.SIDE_SELL:
        _market_ask = float(kucoin_client.get_order_book(_asset.market)['asks'][0][0])
        if _asset.price - _market_ask <= 0.01 * sat:
            _market_price = _market_ask
            _exit = True
    if _exit:
        logger_global[0].info(
            f"{_asset.market} {_asset.kucoin_side} check_kucoin_offer_validity failed: your price {get_format_price(_asset.price)} : market price : {get_format_price(_market_price)}")
        if return_value:
            return _market_price + _asset.kucoin_price_increment
        else:
            sys.exit(-1)


def price_increment(_price, _increment):
    _dp = 1 / np.power(10, len(get_format_price(_price).split(".")[1]))
    return round_float_price(float(_price) + _dp, float(_increment))


def price_decrement(_price, _increment):
    _dp = 1 / np.power(10, len(get_format_price(_price).split(".")[1]))
    return round_float_price(float(_price) - _dp, float(_increment))


class Asset(object):
    def __init__(self, exchange, name, stop_loss_price=False, price_profit=False, profit=False, ticker=False,
                 tight=False, barrier=False, _stop_loss=True):
        stop_when_not_exchange(exchange)
        self.exchange = exchange
        self.name = name
        self.stop_loss = _stop_loss
        if exchange == "kucoin":
            self.market = "{}-BTC".format(name)
            self.ticker = ticker_to_kucoin(ticker)
            self.kucoin_increment = float(get_kucoin_symbol(self.market, 'baseIncrement'))
            self.kucoin_price_increment = float(get_kucoin_symbol(self.market, 'priceIncrement'))
        if exchange == "binance":
            self.market = "{}BTC".format(name)
            self.price_ticker_size = get_binance_price_tick_size(self.market)
            self.ticker = ticker
        if stop_loss_price is not None:
            self.stop_loss_price = round(stop_loss_price + delta, 10)
        self.tight = tight
        if price_profit is not None:
            self.price_profit = round(price_profit + delta, 10)
        if profit is not None:
            self.profit = round(profit + delta, 10)  # taking profit only when it's higher than profit %
        self.take_profit_ratio = profit * 0.632  # taking profit only when it's higher than profit % for a high-candle-sell
        self.barrier = barrier
        self.buy_price = None
        self.cancel = True
        self.original_price = None
        self.order_id = None

    def set_order_id(self, _order_id):
        self.order_id = _order_id

    def set_tight(self):
        self.tight = True

    def keep_lowest_ask(self, _tolerance):
        while True:
            self.keep_lowest_ask_process(_tolerance)
            time.sleep(30)

    def keep_highest_bid_process(self, _tolerance):
        self.price = self.original_price
        _asks = kucoin_client.get_order_book(self.market)['bids']
        save_to_file(key_dir, "market-bids", _asks)
        _asks = get_pickled(key_dir, "market-bids")
        _market_ask = _asks[0]

        _market_ask_price = float(_market_ask[0])
        _market_ask_vol = float(_market_ask[1])
        _asks_value = 0
        _cut_value = _prev_ask = 0
        _to_increment = False
        # _asks = kucoin_client.get_order_book(self.market)['asks']
        if _market_ask_price >= self.price and _market_ask_vol >= _tolerance:
            self.price = price_increment(_market_ask[0], self.kucoin_increment)
            cancel_kucoin_current_orders(self.market)
            self.limit_hidden_order()
        else:
            for _ask in _asks:
                if float(_ask[0]) > self.price and _asks_value < _tolerance:
                    _asks_value += float(_ask[1])
                else:
                    if 0 < _asks_value < _tolerance:
                        _to_increment = True
                    if _prev_ask != 0:
                        _cut_value = float(_prev_ask[0])
                        break
                _prev_ask = _ask
            if self.price < _market_ask_price or _asks_value > _tolerance:
                if _to_increment:
                    self.price = price_decrement(_cut_value, self.kucoin_increment)
                else:
                    self.price = price_increment(_cut_value, self.kucoin_increment)
                cancel_kucoin_current_orders(self.market)
                self.limit_hidden_order()

    def keep_lowest_ask_process(self, _tolerance):
        self.price = self.original_price
        _asks = kucoin_client.get_order_book(self.market)['asks']
        _market_ask = _asks[0]
        _market_ask_price = float(_market_ask[0])
        _market_ask_vol = float(_market_ask[1])
        _asks_value = 0
        _cut_value = _prev_ask = 0
        _to_increment = False
        if _market_ask_price <= self.price and _market_ask_vol >= _tolerance:
            self.price = price_decrement(_market_ask[0], self.kucoin_increment)
            cancel_kucoin_current_orders(self.market)
            self.limit_hidden_order()
        else:
            for _ask in _asks:
                if float(_ask[0]) < self.price and _asks_value < _tolerance:
                    _asks_value += float(_ask[1])
                else:
                    if 0 < _asks_value < _tolerance:
                        _to_increment = True
                    if _prev_ask != 0:
                        _cut_value = float(_prev_ask[0])
                        break
                _prev_ask = _ask
            if self.price > _market_ask_price or _asks_value > _tolerance:
                if _to_increment:
                    self.price = price_increment(_cut_value, self.kucoin_increment)
                else:
                    self.price = price_decrement(_cut_value, self.kucoin_increment)
                cancel_kucoin_current_orders(self.market)
                self.limit_hidden_order()

    def hidden_buy_order(self, compute_purchase_fund=False):
        if compute_purchase_fund:
            _remaining_btc = get_remaining_btc_kucoin()
            if _remaining_btc:
                self.ratio = 100
                _useable_btc = (1 - kucoin_general_fee) * _remaining_btc
                self.purchase_fund = round(self.ratio / 100 * _useable_btc, 8)
                logger_global[0].info(
                    f"{self.market} : computing purchase fund -- remaining BTC : {_remaining_btc}, purchase fund : {self.purchase_fund} BTC")
        _market_price = check_kucoin_offer_validity(self, return_value=True)
        if self.cancel:
            cancel_kucoin_current_orders(self.market)
        if self.kucoin_side == KucoinClient.SIDE_BUY:
            size = self.purchase_fund / self.price
            get_or_create_kucoin_trade_account(self.name)
        elif self.kucoin_side == KucoinClient.SIDE_SELL:
            size = float(get_or_create_kucoin_trade_account(self.name)['available'])

        self.adjusted_size = adjust_kucoin_order_size(self, size)
        required_size = float(get_kucoin_symbol(self.market, 'baseMinSize'))
        _strategy = Strategy(self)
        if self.adjusted_size >= required_size:
            _id = None
            if _market_price:
                self.price = round(_market_price + delta, 10)

            _id = kucoin_client.create_limit_order(self.market, self.kucoin_side, str(self.price),
                                                   str(self.adjusted_size),
                                                   hidden=True)['orderId']
            self.set_order_id(_id)
            logger_global[0].info(
                "{} {}::limit_hidden_order : ratio : {} order_id : {} has been placed.".format(self.market, self,
                                                                                               self.ratio, _id))
            logger_global[0].info(
                "{} {}::limit_hidden_order : {} {} @ {} BTC : {} BTC".format(self.market, self, self.adjusted_size,
                                                                             self.name,
                                                                             get_format_price(self.price),
                                                                             get_format_price(
                                                                                 self.adjusted_size * self.price)))
            if self.stop_loss:
                _strategy.set_stop_loss()
            return _id
        else:
            logger_global[0].info(
                "{} {}::limit_hidden_order: size too small, size: {} required_size: {}".format(self.market, self,
                                                                                               get_format_price(
                                                                                                   self.adjusted_size),
                                                                                               required_size))
            sys.exit(-1)

    def limit_hidden_order(self, is_profit=False, stop_loss=True):
        if is_profit:
            logger_global[0].info(f"{self.market} : taking profits...")
            self.tight = True
            self.kucoin_side = KucoinClient.SIDE_SELL
            self.ratio = 100
        self.stop_loss = stop_loss
        if not self.tight:
            check_kucoin_offer_validity(self)
        if self.cancel:
            cancel_kucoin_current_orders(self.market)
        _btc_value = get_remaining_btc_kucoin()
        _useable_btc = (1 - kucoin_general_fee) * _btc_value
        purchase_fund = self.ratio / 100 * _useable_btc
        if self.kucoin_side == KucoinClient.SIDE_BUY:
            size = purchase_fund / self.price
            get_or_create_kucoin_trade_account(self.name)
        elif self.kucoin_side == KucoinClient.SIDE_SELL:
            size = float(get_or_create_kucoin_trade_account(self.name)['available'])
            self.size_profit = size

        self.adjusted_size = adjust_kucoin_order_size(self, size)
        required_size = float(get_kucoin_symbol(self.market, 'baseMinSize'))
        _strategy = Strategy(self)
        if self.adjusted_size >= required_size:
            _price = self.price_profit if is_profit else self.price
            _id = \
                kucoin_client.create_limit_order(self.market, self.kucoin_side, str(_price),
                                                 str(self.adjusted_size),
                                                 hidden=True)['orderId']
            self.set_order_id(_id)
            logger_global[0].info(
                "{} {}::limit_hidden_order : ratio : {} order_id : {} has been placed.".format(self.market, self,
                                                                                               self.ratio, _id))

            logger_global[0].info(
                "{} {}::limit_hidden_order : {} {} @ {} BTC : {} BTC".format(self.market, self, self.adjusted_size,
                                                                             self.name,
                                                                             get_format_price(_price),
                                                                             get_format_price(
                                                                                 self.adjusted_size * _price)))
            if self.stop_loss:
                _strategy.set_stop_loss()
            return _id
        else:
            logger_global[0].info(
                "{} {}::limit_hidden_order: size too small, size: {} required_size: {}".format(self.market, self,
                                                                                               get_format_price(
                                                                                                   self.adjusted_size),
                                                                                               required_size))
            sys.exit(-1)


class BuyAsset(Asset):
    def __init__(self, exchange, name, price, stop_loss_price=None, price_profit=None, ratio=100, profit=5, tight=False,
                 kucoin_side=False,
                 ticker=BinanceClient.KLINE_INTERVAL_1MINUTE, barrier=False, sleep=None, stop_loss=False):
        super().__init__(exchange, name, stop_loss_price, price_profit, profit, ticker, tight, barrier, stop_loss)
        self.ratio = ratio  # buying ratio [%] of all possessed BTC
        self.price = round(price + delta, 10)
        self.kucoin_side = kucoin_side
        self.sleep = sleep

    def set_purchase_fund(self, _remaining_btc):
        if _remaining_btc:
            _useable_btc = (1 - kucoin_general_fee) * _remaining_btc
            self.purchase_fund = round(self.ratio / 100 * _useable_btc, 8)

    def set_btc_asset_buy_value(self, _total_btc):
        self.btc_asset_buy_value = self.ratio / 100 * _total_btc

    def keep_existing_orders(self):
        self.cancel = False

    def set_ratio(self, _ratio):
        self.ratio = round(_ratio + delta, 10)

    def __str__(self):
        return "BuyAsset"


class SellAsset(Asset):
    def __init__(self, exchange, name, stop_loss_price, tight=False,
                 ticker=BinanceClient.KLINE_INTERVAL_1MINUTE, price=False, ratio=False, kucoin_side=False,
                 stop_loss_volume=None, delta_price=False, market_sell=False):
        super().__init__(exchange, name, stop_loss_price, None, 0, ticker, tight=tight)
        self.price = round(price + delta, 10)
        self.original_price = self.price
        self.kucoin_side = kucoin_side
        self.stop_loss_volume = stop_loss_volume
        self.delta_price = delta_price
        self.market_sell = market_sell
        self.ratio = ratio  # buying ratio [%] of all possessed BTC

    def __str__(self):
        return "SellAsset"


class ObserveAsset(Asset):
    def __init__(self, exchange, name, buy_price, stop_loss_price=None, price_profit=None, profit=5, tight=False,
                 ticker=BinanceClient.KLINE_INTERVAL_4HOUR,
                 barrier=False, line=None, horizon=None, mas=None):
        super().__init__(exchange, name, stop_loss_price, price_profit, profit, ticker, tight, barrier)
        self.buy_price = buy_price
        self.line = line
        self.horizon = horizon
        self.mas = mas


class TradeAsset(BuyAsset):
    def __init__(self, name, ticker=BinanceClient.KLINE_INTERVAL_1MINUTE, ratio=100, profit=8, tight=False):
        super().__init__(name, None, None, None, ratio, profit, tight, ticker)
        self.trading = False
        self.running = False


class AlertAsset(TradeAsset):
    def __init__(self, name, ticker=BinanceClient.KLINE_INTERVAL_1MINUTE):
        super().__init__(name, ticker)
        self.sent = False


class AssetTicker(object):
    def __init__(self, name, ticker, ask_price, timestamp):
        self.name = name
        self.tickers = [ticker]
        self.ask_price = ask_price
        self.timestamp = timestamp

    def add_ticker(self, ticker):
        self.tickers.append(ticker)


class Strategy(object):
    def __init__(self, asset):
        self.asset = asset

    def set_stop_loss(self):
        _stop_loss_maker = threading.Thread(target=stop_loss_func, args=(self.asset,),
                                            name='_stop_loss_maker_{}'.format(self.asset.name))
        _stop_loss_maker.start()

    def set_take_profit(self):
        _take_profit_maker = threading.Thread(target=take_profit, args=(self.asset,),
                                              name='_take_profit_maker_{}'.format(self.asset.name))
        _take_profit_maker.start()

    def __str__(self):
        return str(self.__class__).split('\'')[1].split('.')[1]


class BuyStrategy(Strategy):
    def __init__(self, asset, btc_value, params, subclass=False):
        super().__init__(asset)
        self.btc_value = btc_value
        self.params = params
        self.asset = asset
        if not subclass:
            logger_global[0].info("{} BuyStrategy object has been created".format(self.asset.market))

    def run(self):
        if self.asset.exchange == 'binance':
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
        if self.asset.exchange == 'kucoin':
            kucoin_client.create_market_order(self.asset.market, KucoinClient.SIDE_BUY, size=self.btc_value)


class ObserverStrategy(Strategy):
    def __init__(self, asset):
        super().__init__(asset)
        logger_global[0].info("{} ObserverStrategy object has been created".format(self.asset.market))

    def run(self):
        self.set_stop_loss()
        sell_limit(self.asset.market, self.asset.name, self.asset.price_profit)
        self.set_take_profit()


class BullishStrategy(BuyStrategy):
    def __init__(self, asset, btc_value, params, subclass=False):
        super().__init__(asset, btc_value, params, True)
        if not subclass:
            logger_global[0].info("{} BullishStrategy object has been created".format(self.asset.market))

    def run(self):
        self.set_buy_local_bottom()
        wait_until_running(self)
        if self.asset.trading:
            self.set_sell_local_top()

    def set_buy_local_bottom(self):
        _buy_local_bottom_maker = threading.Thread(target=self.buy_local_bottom,
                                                   name='_buy_local_bottom_maker_{}'.format(self.asset.name))
        _buy_local_bottom_maker.start()

    def set_sell_local_top(self):
        _sell_local_top_maker = threading.Thread(target=sell_local_top, args=(self.asset,),
                                                 name='_sell_local_top_maker_{}'.format(self.asset.name))
        _sell_local_top_maker.start()

    def buy_local_bottom(self):
        _time_interval = get_binance_interval_unit(self.asset.ticker)
        _time_frame_short = 10
        _time_frame_middle = 30
        _time_frame_rsi = 50
        _time_horizon = 60
        _time_horizon_long = 360
        _prev_rsi_high = False
        _trigger = False
        _rsi_low = False
        _rsi_low_fresh = False
        _prev_rsi = TimeTuple(0, 0)
        _slope_condition = TimeTuple(False, 0)
        while 1:
            try:
                self.asset.price = lowest_ask(self.asset.market)
                if not is_buy_possible(self.asset, self.btc_value, self.params):
                    self.asset.running = False
                    logger_global[0].info(
                        "{} buy_local_bottom {} : buy not possible, skipping, exiting".format(self.asset.market, self))
                    sys.exit(0)

                if not _rsi_low and not _rsi_low_fresh and not is_bullish_setup(self.asset):
                    self.asset.running = False
                    logger_global[0].info(
                        "{} buy_local_bottom {} : NOT bullish setup, skipping, exiting".format(self.asset.market, self))
                    sys.exit(0)

                _klines = get_klines(self.asset, _time_interval)
                _curr_kline = _klines[-1]
                _closes = get_closes(_klines)
                _rsi = relative_strength_index(_closes, _prev_rsi.value, 14, self.asset)
                _max_volume = get_max_volume(_klines, _time_horizon)
                _rsi_curr = _rsi[-1]
                _ma7 = talib.MA(_closes, timeperiod=7)
                _time_curr = _curr_kline[0]
                _open = float(_curr_kline[1])
                _close = _closes[-1]

                if _rsi_curr > 70:
                    _prev_rsi_high = TimeTuple(_rsi_curr, _time_curr)
                    _rsi_low = False
                    _rsi_low_fresh = False

                _max_volume = get_max_volume(_klines, _time_horizon)

                # if _rsi_curr < 33.5 and not is_fresh_test(_prev_rsi_high, _time_frame_rsi, _time_curr):
                if _rsi_curr < 33.5 and not is_fresh(_prev_rsi_high, _time_frame_rsi):
                    _max_volume_long = get_max_volume(_klines, _time_horizon_long)
                    if volume_condition(_klines, _max_volume_long, 0.9) and not_equal_rsi(_rsi_curr, _rsi_low_fresh):
                        _rsi_low = TimeTuple(_rsi_curr, _time_curr)

                if _rsi_curr < 33.5 and is_fresh(_prev_rsi_high, _time_frame_middle) and not_equal_rsi(
                        _rsi_curr, _rsi_low):
                    if volume_condition(_klines, _max_volume, 0.9):
                        _rsi_low_fresh = TimeTuple(_rsi_curr, _time_curr)

                if not _rsi_low and _rsi_curr < 31 and not is_fresh(_prev_rsi_high, _time_frame_rsi) \
                        and volume_condition(_klines, _max_volume, 0.5) and not_equal_rsi(_rsi_curr, _rsi_low_fresh):
                    if not is_rsi_slope_condition(_rsi, 100, 68, len(_rsi) - 45, -1, _window=10):
                        _rsi_low = TimeTuple(_rsi_curr, _time_curr)
                    else:
                        _slope_condition = TimeTuple(True, _time_curr)

                if not _rsi_low and _rsi_curr < 20 and not_equal_rsi(_rsi_curr, _rsi_low_fresh):
                    _rsi_low = TimeTuple(_rsi_curr, _time_curr)

                _c1 = _rsi_low and _rsi_curr < 35 and is_fresh(_rsi_low, _time_frame_rsi) and not is_fresh(_rsi_low,
                                                                                                           15) and \
                      _rsi_curr > _rsi_low.value and not is_fresh(_rsi_low_fresh, _time_frame_middle)

                _c2 = _rsi_low and _rsi_low_fresh and _rsi_curr > _rsi_low_fresh.value and _rsi_curr > _rsi_low.value and \
                      not is_fresh(_rsi_low_fresh, _time_frame_middle)

                _rsi_temp = get_one_of_rsi(_rsi_low_fresh, _rsi_low)
                _c3 = _rsi_temp and _rsi_curr > _rsi_temp.value and not is_fresh(_rsi_temp, _time_horizon) and \
                      volume_condition(_klines, _max_volume, 0.9) and _rsi_curr < 33.5

                if _c1 or _c2 or _c3:
                    _max_volume_short = get_max_volume(_klines, _time_frame_short)
                    # if _rsi_curr > _rsi_low[0] and volume_condition(_klines, _max_volume, 0.3):  # RSI HL
                    if volume_condition(_klines, _max_volume_short, 0.3):  # RSI HL
                        _trigger = TimeTuple(True, _time_curr)

                _max_volume_middle = get_max_volume(_klines, 10)

                if _rsi_low and _close - _ma7[-1] > 0 and _rsi_curr > _rsi_low.value and \
                        volume_condition(_klines, _max_volume_middle, 1.0):  # reversal
                    _trigger = TimeTuple(True, _time_curr)

                if _rsi_low and _rsi_low.value < 20 and is_fresh(_rsi_low, 15):
                    _trigger = False

                if _slope_condition.value:
                    _rsi_low = False
                    _rsi_low_fresh = False
                    _trigger = False
                if not is_fresh_test(_slope_condition, _time_horizon, _time_curr):
                    _slope_condition = TimeTuple(False, _time_curr)

                if _rsi_low and is_red_candle(_curr_kline):
                    _green_klines = get_green_candles(_klines)
                    _max_volume_long = get_max_volume(_green_klines, _time_horizon)
                    if volume_condition(_klines, _max_volume_long, 2.1):
                        _rsi_low = False
                        _rsi_low_fresh = False

                if _rsi_low and _rsi_low.value == 0:
                    _rsi_low = False
                if _rsi_low_fresh and _rsi_low_fresh.value == 0:
                    _rsi_low_fresh = False

                if not self.asset.trading and _close - _ma7[-1] > 0 and is_fresh(_trigger, 15) > 0:
                    # logger_global[0].info("{} Buy Local Bottom triggered : {} ...".format(self.asset.market, self))
                    _la = lowest_ask(self.asset.market)
                    self.asset.buy_price = _la
                    _possible_buying_quantity = get_buying_asset_quantity(self.asset, self.btc_value)
                    _quantity_to_buy = adjust_quantity(_possible_buying_quantity, self.params)

                    _min_notional = float(get_filter(self.asset.market, "MIN_NOTIONAL")['minNotional'])
                    _buy_cond = self.asset.buy_price * _quantity_to_buy >= _min_notional

                    if not _buy_cond:
                        logger_global[0].error(
                            "{} min notional condition NOT MET {} > {}, exiting".format(self.asset.market,
                                                                                        _min_notional, price_to_string(
                                    self.asset.buy_price * _quantity_to_buy)))
                        sys.exit(0)

                    if _quantity_to_buy and is_buy_possible(self.asset, self.btc_value, self.params):
                        dump_variables(self.asset.market, _prev_rsi_high, _trigger, _rsi_low, _rsi_low_fresh,
                                       TimeTuple(False, 0), TimeTuple(False, 0), TimeTuple(False, 0), _slope_condition)
                        self.asset.trading = True
                        _order_id = buy_order(self.asset, _quantity_to_buy)
                        adjust_stop_loss_price(self.asset)
                        adjust_price_profit(self.asset)
                        self.set_stop_loss()
                        wait_until_order_filled(self.asset.market, _order_id)
                        sell_limit(self.asset.market, self.asset.name, self.asset.price_profit)
                        self.set_take_profit()
                        logger_global[0].info(
                            "{} Bought Local Bottom {} : price : {} value : {} BTC, exiting".format(self.asset.market,
                                                                                                    self,
                                                                                                    price_to_string(
                                                                                                        self.asset.buy_price),
                                                                                                    self.btc_value))
                        self.asset.running = False
                        save_to_file(trades_logs_dir, "buy_klines_{}".format(time.time()), _klines)
                        sys.exit(0)
                _prev_rsi = TimeTuple(_rsi, _time_curr)
                time.sleep(45)
            except Exception as err:
                if isinstance(err, requests.exceptions.ConnectionError):
                    logger_global[0].error("{} {}".format(self.asset.market, "Connection problem..."))
                else:
                    traceback.print_tb(err.__traceback__)
                    logger_global[0].exception("{} {}".format(self.asset.market, err.__traceback__))
                    time.sleep(45)


class BearishStrategy(BullishStrategy):
    def __init__(self, asset, btc_value, params):
        super().__init__(asset, btc_value, params, True)
        logger_global[0].info("{} BearishStrategy object has been created".format(self.asset.market))

    def buy_local_bottom(self):
        _time_interval = get_binance_interval_unit(self.asset.ticker)
        _time_frame_short = 10
        _time_frame_middle = 30
        _time_frame_rsi = 50
        _time_horizon = 60
        _time_horizon_long = 360
        _prev_rsi_high = False
        _trigger = False
        _rsi_low = False
        _rsi_low_fresh = False
        _prev_rsi = TimeTuple(0, 0)
        _last_ma7_gt_ma100 = TimeTuple(False, 0)
        _big_volume_sold_out = TimeTuple(False, 0)
        _bearish_trigger = TimeTuple(False, 0)
        _slope_condition = TimeTuple(False, 0)
        while 1:
            try:
                self.asset.price = lowest_ask(self.asset.market)
                if not is_buy_possible(self.asset, self.btc_value, self.params):
                    self.asset.running = False
                    logger_global[0].info(
                        "{} buy_local_bottom {} : buy not possible, skipping, exiting".format(self.asset.market, self))
                    sys.exit(0)

                if not _rsi_low and not _rsi_low_fresh and is_bullish_setup(self.asset):
                    self.asset.running = False
                    logger_global[0].info(
                        "{} buy_local_bottom {} : NOT bearish setup, skipping, exiting".format(self.asset.market, self))
                    sys.exit(0)

                _klines = get_klines(self.asset, _time_interval)
                _curr_kline = _klines[-1]
                _closes = get_closes(_klines)
                _rsi = relative_strength_index(_closes, _prev_rsi.value, 14, self.asset)
                _ma7_curr = talib.MA(_closes, timeperiod=7)[-1]
                _ma100_curr = talib.MA(_closes, timeperiod=100)[-1]
                _time_curr = _curr_kline[0]
                _open = float(_curr_kline[1])
                _volume_curr = float(_curr_kline[7])
                _close = _closes[-1]
                _rsi_curr = _rsi[-1]

                if _ma7_curr > _ma100_curr:
                    _last_ma7_gt_ma100 = TimeTuple(_close, _time_curr)

                if _last_ma7_gt_ma100.value and is_red_candle(
                        _curr_kline) and _ma100_curr > _ma7_curr > _close and _rsi_curr < 30:
                    _max_volume_long = get_max_volume(_klines, _time_horizon_long)
                    if volume_condition(_klines, _max_volume_long, 1.2):
                        _big_volume_sold_out = TimeTuple(_volume_curr, _time_curr)

                if _rsi_curr > 70:
                    _prev_rsi_high = TimeTuple(_rsi_curr, _time_curr)
                    _rsi_low = False
                    _rsi_low_fresh = False

                _max_volume = get_max_volume(_klines, _time_horizon)

                # if _rsi_curr < 33.5 and not is_fresh_test(_prev_rsi_high, _time_frame_rsi, _curr_kline[0]):
                if _rsi_curr < 33.5 and not is_fresh(_prev_rsi_high, _time_frame_rsi):
                    _max_volume_long = get_max_volume(_klines, _time_horizon_long)
                    if volume_condition(_klines, _max_volume_long, 0.9) and not_equal_rsi(_rsi_curr, _rsi_low_fresh):
                        _rsi_low = TimeTuple(_rsi_curr, _time_curr)

                if _rsi_curr < 33.5 and is_fresh(_prev_rsi_high, _time_frame_middle) and not_equal_rsi(
                        _rsi_curr, _rsi_low):
                    if volume_condition(_klines, _max_volume, 0.9):
                        _rsi_low_fresh = TimeTuple(_rsi_curr, _time_curr)

                if not _rsi_low and _rsi_curr < 31 and not is_fresh(_prev_rsi_high, _time_frame_rsi) \
                        and volume_condition(_klines, _max_volume, 0.5) and not_equal_rsi(_rsi_curr, _rsi_low_fresh):
                    if not is_rsi_slope_condition(_rsi, 100, 68, len(_rsi) - 45, -1, _window=10):
                        _rsi_low = TimeTuple(_rsi_curr, _time_curr)
                    else:
                        _slope_condition = TimeTuple(True, _time_curr)

                if not _rsi_low and _rsi_curr < 20 and not_equal_rsi(_rsi_curr, _rsi_low_fresh):
                    _rsi_low = TimeTuple(_rsi_curr, _time_curr)

                _c1 = _rsi_low and _rsi_curr < 33.5 and is_fresh(_rsi_low, _time_frame_rsi) and not is_fresh(_rsi_low,
                                                                                                             15) and \
                      _rsi_curr > _rsi_low.value and not is_fresh(_rsi_low_fresh, _time_frame_middle)

                _c2 = _rsi_low and _rsi_low_fresh and _rsi_curr > _rsi_low_fresh.value and _rsi_curr > _rsi_low.value and \
                      not is_fresh(_rsi_low_fresh, _time_frame_middle)

                _rsi_temp = get_one_of_rsi(_rsi_low_fresh, _rsi_low)
                _c3 = _rsi_temp and _rsi_curr > _rsi_temp.value and not is_fresh(_rsi_temp, _time_horizon) and \
                      volume_condition(_klines, _max_volume, 0.9) and _rsi_curr < 33.5

                if _c1 or _c2 or _c3:
                    _max_volume_short = get_max_volume(_klines, _time_frame_short)
                    # if _rsi_curr > _rsi_low[0] and volume_condition(_klines, _max_volume, 0.3):  # RSI HL
                    if volume_condition(_klines, _max_volume_short, 0.3):  # RSI HL
                        _trigger = TimeTuple(True, _time_curr)

                _max_volume_middle = get_max_volume(_klines, _time_frame_short)

                if _rsi_low and _close - _ma7_curr > 0 and _rsi_curr > _rsi_low.value and \
                        volume_condition(_klines, _max_volume_middle, 1.0):  # reversal
                    _trigger = TimeTuple(True, _time_curr)

                if _big_volume_sold_out.value:
                    if price_drop(_last_ma7_gt_ma100.value, _close, 0.08):
                        _bearish_trigger = TimeTuple(True, _time_curr)

                if _rsi_low and _rsi_low.value < 20 and is_fresh(_rsi_low, 15):
                    _trigger = False

                if _slope_condition.value:
                    _rsi_low = False
                    _rsi_low_fresh = False
                    _trigger = False
                if not is_fresh_test(_slope_condition, _time_horizon, _time_curr):
                    _slope_condition = TimeTuple(False, _time_curr)

                if _rsi_low and is_red_candle(_curr_kline):
                    _green_klines = get_green_candles(_klines)
                    _max_volume_long = get_max_volume(_green_klines, _time_horizon)
                    if volume_condition(_klines, _max_volume_long, 2.1):
                        _rsi_low = False
                        _rsi_low_fresh = False

                if _rsi_low and _rsi_low.value == 0:
                    _rsi_low = False
                if _rsi_low_fresh and _rsi_low_fresh.value == 0:
                    _rsi_low_fresh = False

                if not self.asset.trading and _close - _ma7_curr > 0 and is_fresh(_trigger, 15) and is_fresh(
                        _bearish_trigger, 15):
                    # logger_global[0].info("{} Buy Local Bottom triggered {} ...".format(self.asset.market, self))
                    _la = lowest_ask(self.asset.market)
                    self.asset.buy_price = _la
                    _possible_buying_quantity = get_buying_asset_quantity(self.asset, self.btc_value)
                    _quantity_to_buy = adjust_quantity(_possible_buying_quantity, self.params)

                    _min_notional = float(get_filter(self.asset.market, "MIN_NOTIONAL")['minNotional'])
                    _buy_cond = self.asset.buy_price * _quantity_to_buy >= _min_notional

                    if not _buy_cond:
                        logger_global[0].error(
                            "{} min notional condition NOT MET {} > {}, exiting".format(self.asset.market,
                                                                                        _min_notional, price_to_string(
                                    self.asset.buy_price * _quantity_to_buy)))
                        sys.exit(0)

                    if _quantity_to_buy and is_buy_possible(self.asset, self.btc_value, self.params):
                        dump_variables(self.asset.market, _prev_rsi_high, _trigger, _rsi_low, _rsi_low_fresh, _prev_rsi,
                                       _last_ma7_gt_ma100, _big_volume_sold_out, _bearish_trigger, _slope_condition)
                        self.asset.trading = True
                        _order_id = buy_order(self.asset, _quantity_to_buy)
                        adjust_stop_loss_price(self.asset)
                        adjust_price_profit(self.asset)
                        self.set_stop_loss()
                        wait_until_order_filled(self.asset.market, _order_id)
                        sell_limit(self.asset.market, self.asset.name, self.asset.price_profit)
                        self.set_take_profit()
                        logger_global[0].info(
                            "{} Bought Local Bottom {} : price : {} value : {} BTC, exiting".format(self.asset.market,
                                                                                                    self,
                                                                                                    price_to_string(
                                                                                                        self.asset.buy_price),
                                                                                                    self.btc_value))
                        self.asset.running = False
                        save_to_file(trades_logs_dir, "buy_klines_{}".format(time.time()), _klines)
                        sys.exit(0)
                _prev_rsi = TimeTuple(_rsi, _time_curr)
                time.sleep(45)
            except Exception as err:
                if isinstance(err, requests.exceptions.ConnectionError):
                    logger_global[0].error("{} {}".format(self.asset.market, "Connection problem..."))
                else:
                    traceback.print_tb(err.__traceback__)
                    logger_global[0].exception("{} {}".format(self.asset.market, err.__traceback__))
                    time.sleep(45)


class AlertsBullishStrategy(BuyStrategy):
    def __init__(self, asset, subclass=False):
        super().__init__(asset, None, None, True)
        if not subclass:
            logger_global[0].info("{} AlertsBullishStrategy object has been created".format(self.asset.market))

    def run(self):
        self.set_alert_buy_local_bottom()
        wait_until_running(self)

    def set_alert_buy_local_bottom(self):
        _alert_buy_local_bottom_maker_ = threading.Thread(target=self.alert_buy_local_bottom,
                                                          name='_alert_buy_local_bottom_maker_{}'.format(
                                                              self.asset.name))
        _alert_buy_local_bottom_maker_.start()

    def alert_buy_local_bottom(self):
        _time_interval = get_binance_interval_unit(self.asset.ticker)
        _time_frame_short = 10
        _time_frame_middle = 30
        _time_frame_rsi = 50
        _time_horizon = 60
        _time_horizon_long = 360
        _prev_rsi_high = False
        _trigger = False
        _rsi_low = False
        _rsi_low_fresh = False
        _prev_rsi = TimeTuple(0, 0)
        _slope_condition = TimeTuple(False, 0)

        _mail_title = 'Bullish Buy Bottom Alert'

        while 1:
            try:
                if is_fresh(self.asset.sent, _time_horizon):
                    self.asset.running = False
                    logger_global[0].info(
                        "{} buy_local_bottom {} : sent not possible, skipping, exiting".format(self.asset.market, self))
                    sys.exit(0)

                if not _rsi_low and not _rsi_low_fresh and not is_bullish_setup(self.asset):
                    self.asset.running = False
                    logger_global[0].info(
                        "{} buy_local_bottom {} : NOT bullish setup, skipping, exiting".format(self.asset.market, self))
                    sys.exit(0)

                _klines = get_klines(self.asset, _time_interval)
                _curr_kline = _klines[-1]
                _closes = get_closes(_klines)
                _rsi = relative_strength_index(_closes, _prev_rsi.value, 14, self.asset)
                _max_volume = get_max_volume(_klines, _time_horizon)
                _rsi_curr = _rsi[-1]
                _ma7 = talib.MA(_closes, timeperiod=7)
                _time_curr = _curr_kline[0]
                _open = float(_curr_kline[1])
                _close = _closes[-1]

                if _rsi_curr > 70:
                    _prev_rsi_high = TimeTuple(_rsi_curr, _time_curr)
                    _rsi_low = False
                    _rsi_low_fresh = False

                _max_volume = get_max_volume(_klines, _time_horizon)

                # if _rsi_curr < 33.5 and not is_fresh_test(_prev_rsi_high, _time_frame_rsi, _time_curr):
                if _rsi_curr < 33.5 and not is_fresh(_prev_rsi_high, _time_frame_rsi):
                    _max_volume_long = get_max_volume(_klines, _time_horizon_long)
                    if volume_condition(_klines, _max_volume_long, 0.9) and not_equal_rsi(_rsi_curr, _rsi_low_fresh):
                        _rsi_low = TimeTuple(_rsi_curr, _time_curr)

                if _rsi_curr < 33.5 and is_fresh(_prev_rsi_high, _time_frame_middle) and not_equal_rsi(
                        _rsi_curr, _rsi_low):
                    if volume_condition(_klines, _max_volume, 0.9):
                        _rsi_low_fresh = TimeTuple(_rsi_curr, _time_curr)

                if not _rsi_low and _rsi_curr < 31 and not is_fresh(_prev_rsi_high, _time_frame_rsi) \
                        and volume_condition(_klines, _max_volume, 0.5) and not_equal_rsi(_rsi_curr, _rsi_low_fresh):
                    if not is_rsi_slope_condition(_rsi, 100, 68, len(_rsi) - 45, -1, _window=10):
                        _rsi_low = TimeTuple(_rsi_curr, _time_curr)
                    else:
                        _slope_condition = TimeTuple(True, _time_curr)

                if not _rsi_low and _rsi_curr < 20 and not_equal_rsi(_rsi_curr, _rsi_low_fresh):
                    _rsi_low = TimeTuple(_rsi_curr, _time_curr)

                _c1 = _rsi_low and _rsi_curr < 35 and is_fresh(_rsi_low, _time_frame_rsi) and not is_fresh(_rsi_low,
                                                                                                           15) and \
                      _rsi_curr > _rsi_low.value and not is_fresh(_rsi_low_fresh, _time_frame_middle)

                _c2 = _rsi_low and _rsi_low_fresh and _rsi_curr > _rsi_low_fresh.value and _rsi_curr > _rsi_low.value and \
                      not is_fresh(_rsi_low_fresh, _time_frame_middle)

                _rsi_temp = get_one_of_rsi(_rsi_low_fresh, _rsi_low)
                _c3 = _rsi_temp and _rsi_curr > _rsi_temp.value and not is_fresh(_rsi_temp, _time_horizon) and \
                      volume_condition(_klines, _max_volume, 0.9) and _rsi_curr < 33.5

                if _c1 or _c2 or _c3:
                    _max_volume_short = get_max_volume(_klines, _time_frame_short)
                    # if _rsi_curr > _rsi_low[0] and volume_condition(_klines, _max_volume, 0.3):  # RSI HL
                    if volume_condition(_klines, _max_volume_short, 0.3):  # RSI HL
                        _trigger = TimeTuple(True, _time_curr)

                _max_volume_middle = get_max_volume(_klines, 10)

                if _rsi_low and _close - _ma7[-1] > 0 and _rsi_curr > _rsi_low.value and \
                        volume_condition(_klines, _max_volume_middle, 1.0):  # reversal
                    _trigger = TimeTuple(True, _time_curr)

                if _rsi_low and _rsi_low.value < 20 and is_fresh(_rsi_low, 15):
                    _trigger = False

                if _slope_condition.value:
                    _rsi_low = False
                    _rsi_low_fresh = False
                    _trigger = False
                if not is_fresh_test(_slope_condition, _time_horizon, _time_curr):
                    _slope_condition = TimeTuple(False, _time_curr)

                if _rsi_low and is_red_candle(_curr_kline):
                    _green_klines = get_green_candles(_klines)
                    _max_volume_long = get_max_volume(_green_klines, _time_horizon)
                    if volume_condition(_klines, _max_volume_long, 2.1):
                        _rsi_low = False
                        _rsi_low_fresh = False

                if _rsi_low and _rsi_low.value == 0:
                    _rsi_low = False
                if _rsi_low_fresh and _rsi_low_fresh.value == 0:
                    _rsi_low_fresh = False

                if not self.asset.trading and _close - _ma7[-1] > 0 and is_fresh(_trigger, 15) > 0:
                    # logger_global[0].info("{} Buy Local Bottom triggered : {} ...".format(self.asset.market, self))
                    _la = lowest_ask(self.asset.market)
                    self.asset.buy_price = _la
                    dump_variables(self.asset.market, _prev_rsi_high, _trigger, _rsi_low, _rsi_low_fresh,
                                   TimeTuple(False, 0), TimeTuple(False, 0), TimeTuple(False, 0), _slope_condition)
                    self.asset.trading = True
                    logger_global[0].info(
                        "{} Alert Buy Local Bottom {} : price : {} value : {} BTC, exiting".format(self.asset.market,
                                                                                                   self,
                                                                                                   price_to_string(
                                                                                                       self.asset.buy_price),
                                                                                                   self.btc_value))
                    _message = "{} Alert Buy Local Bottom {} : price : {} value : {} BTC".format(self.asset.market,
                                                                                                 self,
                                                                                                 price_to_string(
                                                                                                     self.asset.buy_price),
                                                                                                 get_time(
                                                                                                     _curr_kline[0]))
                    self.asset.sent = _curr_kline[0]
                    send_mail(_mail_title, _message, self.asset)
                    self.asset.running = False
                    save_to_file(trades_logs_dir, "alert_buy_klines_{}".format(time.time()), _klines)
                    sys.exit(0)
                _prev_rsi = TimeTuple(_rsi, _time_curr)
                time.sleep(45)
            except Exception as err:
                if isinstance(err, requests.exceptions.ConnectionError):
                    logger_global[0].error("{} {}".format(self.asset.market, "Connection problem..."))
                else:
                    traceback.print_tb(err.__traceback__)
                    logger_global[0].exception("{} {}".format(self.asset.market, err.__traceback__))
                    time.sleep(45)


class AlertsBearishStrategy(AlertsBullishStrategy):
    def __init__(self, asset):
        super().__init__(asset)
        logger_global[0].info("{} AlertsBearishStrategy object has been created".format(self.asset.market))

    def alert_buy_local_bottom(self):
        _time_interval = get_binance_interval_unit(self.asset.ticker)
        _time_frame_short = 10
        _time_frame_middle = 30
        _time_frame_rsi = 50
        _time_horizon = 60
        _time_horizon_long = 360
        _prev_rsi_high = False
        _trigger = False
        _rsi_low = False
        _rsi_low_fresh = False
        _prev_rsi = TimeTuple(0, 0)
        _last_ma7_gt_ma100 = TimeTuple(False, 0)
        _big_volume_sold_out = TimeTuple(False, 0)
        _bearish_trigger = TimeTuple(False, 0)
        _slope_condition = TimeTuple(False, 0)

        _mail_title = 'Bearish Buy Bottom Alert'

        while 1:
            try:
                if is_fresh(self.asset.sent, _time_horizon):
                    self.asset.running = False
                    logger_global[0].info(
                        "{} alert_buy_local_bottom {} : sent not possible, skipping, exiting".format(self.asset.market,
                                                                                                     self))
                    sys.exit(0)

                if not _rsi_low and not _rsi_low_fresh and is_bullish_setup(self.asset):
                    self.asset.running = False
                    logger_global[0].info(
                        "{} alert_buy_local_bottom {} : NOT bearish setup, skipping, exiting".format(self.asset.market,
                                                                                                     self))
                    sys.exit(0)

                _klines = get_klines(self.asset, _time_interval)
                _curr_kline = _klines[-1]
                _closes = get_closes(_klines)
                _rsi = relative_strength_index(_closes, _prev_rsi.value, 14, self.asset)
                _ma7_curr = talib.MA(_closes, timeperiod=7)[-1]
                _ma100_curr = talib.MA(_closes, timeperiod=100)[-1]
                _time_curr = _curr_kline[0]
                _open = float(_curr_kline[1])
                _volume_curr = float(_curr_kline[7])
                _close = _closes[-1]
                _rsi_curr = _rsi[-1]

                if _ma7_curr > _ma100_curr:
                    _last_ma7_gt_ma100 = TimeTuple(_close, _time_curr)

                if _last_ma7_gt_ma100.value and is_red_candle(
                        _curr_kline) and _ma100_curr > _ma7_curr > _close and _rsi_curr < 30:
                    _max_volume_long = get_max_volume(_klines, _time_horizon_long)
                    if volume_condition(_klines, _max_volume_long, 1.2):
                        _big_volume_sold_out = TimeTuple(_volume_curr, _time_curr)

                if _rsi_curr > 70:
                    _prev_rsi_high = TimeTuple(_rsi_curr, _time_curr)
                    _rsi_low = False
                    _rsi_low_fresh = False

                _max_volume = get_max_volume(_klines, _time_horizon)

                # if _rsi_curr < 33.5 and not is_fresh_test(_prev_rsi_high, _time_frame_rsi, _curr_kline[0]):
                if _rsi_curr < 33.5 and not is_fresh(_prev_rsi_high, _time_frame_rsi):
                    _max_volume_long = get_max_volume(_klines, _time_horizon_long)
                    if volume_condition(_klines, _max_volume_long, 0.9) and not_equal_rsi(_rsi_curr, _rsi_low_fresh):
                        _rsi_low = TimeTuple(_rsi_curr, _time_curr)

                if _rsi_curr < 33.5 and is_fresh(_prev_rsi_high, _time_frame_middle) and not_equal_rsi(
                        _rsi_curr, _rsi_low):
                    if volume_condition(_klines, _max_volume, 0.9):
                        _rsi_low_fresh = TimeTuple(_rsi_curr, _time_curr)

                if not _rsi_low and _rsi_curr < 31 and not is_fresh(_prev_rsi_high, _time_frame_rsi) \
                        and volume_condition(_klines, _max_volume, 0.5) and not_equal_rsi(_rsi_curr, _rsi_low_fresh):
                    if not is_rsi_slope_condition(_rsi, 100, 68, len(_rsi) - 45, -1, _window=10):
                        _rsi_low = TimeTuple(_rsi_curr, _time_curr)
                    else:
                        _slope_condition = TimeTuple(True, _time_curr)

                if not _rsi_low and _rsi_curr < 20 and not_equal_rsi(_rsi_curr, _rsi_low_fresh):
                    _rsi_low = TimeTuple(_rsi_curr, _time_curr)

                _c1 = _rsi_low and _rsi_curr < 33.5 and is_fresh(_rsi_low, _time_frame_rsi) and not is_fresh(_rsi_low,
                                                                                                             15) and \
                      _rsi_curr > _rsi_low.value and not is_fresh(_rsi_low_fresh, _time_frame_middle)

                _c2 = _rsi_low and _rsi_low_fresh and _rsi_curr > _rsi_low_fresh.value and _rsi_curr > _rsi_low.value and \
                      not is_fresh(_rsi_low_fresh, _time_frame_middle)

                _rsi_temp = get_one_of_rsi(_rsi_low_fresh, _rsi_low)
                _c3 = _rsi_temp and _rsi_curr > _rsi_temp.value and not is_fresh(_rsi_temp, _time_horizon) and \
                      volume_condition(_klines, _max_volume, 0.9) and _rsi_curr < 33.5

                if _c1 or _c2 or _c3:
                    _max_volume_short = get_max_volume(_klines, _time_frame_short)
                    # if _rsi_curr > _rsi_low[0] and volume_condition(_klines, _max_volume, 0.3):  # RSI HL
                    if volume_condition(_klines, _max_volume_short, 0.3):  # RSI HL
                        _trigger = TimeTuple(True, _time_curr)

                _max_volume_middle = get_max_volume(_klines, _time_frame_short)

                if _rsi_low and _close - _ma7_curr > 0 and _rsi_curr > _rsi_low.value and \
                        volume_condition(_klines, _max_volume_middle, 1.0):  # reversal
                    _trigger = TimeTuple(True, _time_curr)

                if _big_volume_sold_out.value:
                    if price_drop(_last_ma7_gt_ma100.value, _close, 0.08):
                        _bearish_trigger = TimeTuple(True, _time_curr)

                if _rsi_low and _rsi_low.value < 20 and is_fresh(_rsi_low, 15):
                    _trigger = False

                if _slope_condition.value:
                    _rsi_low = False
                    _rsi_low_fresh = False
                    _trigger = False
                if not is_fresh_test(_slope_condition, _time_horizon, _time_curr):
                    _slope_condition = TimeTuple(False, _time_curr)

                if _rsi_low and is_red_candle(_curr_kline):
                    _green_klines = get_green_candles(_klines)
                    _max_volume_long = get_max_volume(_green_klines, _time_horizon)
                    if volume_condition(_klines, _max_volume_long, 2.1):
                        _rsi_low = False
                        _rsi_low_fresh = False

                if _rsi_low and _rsi_low.value == 0:
                    _rsi_low = False
                if _rsi_low_fresh and _rsi_low_fresh.value == 0:
                    _rsi_low_fresh = False

                if not self.asset.trading and _close - _ma7_curr > 0 and is_fresh(_trigger, 15) and is_fresh(
                        _bearish_trigger, 15):
                    # logger_global[0].info("{} Buy Local Bottom triggered {} ...".format(self.asset.market, self))
                    _la = lowest_ask(self.asset.market)
                    self.asset.buy_price = _la
                    dump_variables(self.asset.market, _prev_rsi_high, _trigger, _rsi_low, _rsi_low_fresh,
                                   TimeTuple(False, 0), TimeTuple(False, 0), TimeTuple(False, 0), _slope_condition)
                    logger_global[0].info(
                        "{} Alert Buy Local Bottom {} : price : {} value : {} BTC, exiting".format(self.asset.market,
                                                                                                   self,
                                                                                                   price_to_string(
                                                                                                       self.asset.buy_price),
                                                                                                   self.btc_value))
                    _message = "{} Alert Buy Local Bottom {} : price : {} value : {} BTC".format(self.asset.market,
                                                                                                 self,
                                                                                                 price_to_string(
                                                                                                     self.asset.buy_price),
                                                                                                 get_time(
                                                                                                     _curr_kline[0]))
                    self.asset.sent = _curr_kline[0]
                    send_mail(_mail_title, _message, self.asset)
                    self.asset.running = False
                    save_to_file(trades_logs_dir, "alert_buy_klines_{}".format(time.time()), _klines)
                    sys.exit(0)
                _prev_rsi = TimeTuple(_rsi, _time_curr)
                time.sleep(45)
            except Exception as err:
                if isinstance(err, requests.exceptions.ConnectionError):
                    logger_global[0].error("{} {}".format(self.asset.market, "Connection problem..."))
                else:
                    traceback.print_tb(err.__traceback__)
                    logger_global[0].exception("{} {}".format(self.asset.market, err.__traceback__))
                    time.sleep(45)


def check_price_filter(_asset):
    _min_notional = float(get_filter(_asset.market, "MIN_NOTIONAL")['minNotional'])


def run_strategy(_strategy):
    _strategy.run()


def wait_until_running(_strategy):
    while _strategy.asset.running:
        time.sleep(1)


def adjust_stop_loss_price(asset):
    asset.stop_loss_price = np.round(0.968 * asset.buy_price, 8)


def adjust_price_profit(asset):
    asset.price_profit = np.round((1 + asset.profit / 100) * asset.buy_price, 8)


def get_klines(*args):
    if len(args) == 2:
        return get_klines_1(args[0], args[1])
    elif len(args) == 3:
        return get_klines_2(args[0], args[1], args[2])


def get_klines_1(_asset, _time_interval):
    try:
        return binance_obj.get_klines_currency(_asset.market, _asset.ticker, _time_interval)
    except TypeError:
        time.sleep(2)
        get_klines_1(_asset, _time_interval)


def get_klines_2(_market, _ticker, _time_interval):
    try:
        return binance_obj.get_klines_currency(_market, _ticker, _time_interval)
    except TypeError:
        time.sleep(2)
        get_klines_2(_market, _ticker, _time_interval)


def sell_local_top(asset):
    _time_interval = get_binance_interval_unit(asset.ticker)
    _max_volume_max_rsi = -1
    _trigger = False
    _time_frame = 30
    _prev_rsi = TimeTuple(0, 0)
    while 1:
        try:
            if not asset.trading:
                logger_global[0].info("{} sell_local_top : sold, not trading, skipping, exiting".format(asset.market))
                sys.exit(0)

            _klines = get_klines(asset, _time_interval)
            _curr_kline = _klines[-1]
            _closes = get_closes(_klines)
            _rsi = relative_strength_index(_closes, _prev_rsi.value, 14, asset)

            if _rsi[-1] > 70:
                _max_volume_temp = get_volume(_curr_kline)
                if is_green_candle(_curr_kline) and _max_volume_temp > _max_volume_max_rsi:
                    _max_volume_max_rsi = _max_volume_temp
                _max_volume = get_max_volume(_klines, _time_frame)
                if is_fresh(_prev_rsi, _time_frame) and volume_condition(_klines, _max_volume, 0.4):
                    # if volume_condition(_klines, _max_volume, 0.4):
                    _trigger = TimeTuple(True, _curr_kline[0])
            if _trigger and is_red_candle(_curr_kline) and is_profitable(asset, _closes[-1]):
                # if True:
                _ma7 = talib.MA(_closes, timeperiod=7)
                _open = float(_curr_kline[1])
                _close = _closes[-1]
                if _ma7[-1] - _close > 0:
                    # if True:
                    logger_global[0].info(
                        "{} Sell Local Maximum Conditions: trigger and red candle below MA7 : TRUE".format(
                            asset.market))
                    _price = highest_bid(asset.market)
                    _quantity = sell_limit(asset.market, asset.name, _price)
                    logger_global[0].info(
                        "{} Sold Local Top price : {} value : {} BTC, exiting".format(asset.market, _price,
                                                                                      _quantity * _price))
                    save_to_file(trades_logs_dir, "sell_klines_{}".format(time.time()), _klines)
                    asset.running = False
                    asset.trading = False
                    sys.exit(0)
            _prev_rsi = TimeTuple(_rsi, _curr_kline[0])
            time.sleep(45)
        except Exception as err:
            if isinstance(err, requests.exceptions.ConnectionError):
                logger_global[0].error("Connection problem...")
            else:
                traceback.print_tb(err.__traceback__)
                logger_global[0].exception(err.__traceback__)
                time.sleep(45)


_last_asset = 'START'


def start_trading(_trade_asset, _btc_value):
    global _last_asset
    _c = not (_trade_asset.running or _trade_asset.trading)
    if _c:
        _params = get_lot_size_params(_trade_asset.market)
        if _last_asset and _last_asset != _trade_asset.name:
            _last_asset = _trade_asset.name
            _trade_asset.running = True
            if is_bullish_setup(_trade_asset):
                _bs = BullishStrategy(_trade_asset, _btc_value, _params)
            else:
                _bs = BearishStrategy(_trade_asset, _btc_value, _params)

            _run_strategy_maker = threading.Thread(target=run_strategy, args=(_bs,),
                                                   name='_run_strategy_maker_{}'.format(_trade_asset.name))
            _run_strategy_maker.start()

        time.sleep(5)


def start_alerts(_trade_asset):
    global _last_asset
    _c = not (_trade_asset.running or _trade_asset.trading)
    if _c:
        if _last_asset and _last_asset != _trade_asset.name:
            _last_asset = _trade_asset.name
            _trade_asset.running = True
            if is_bullish_setup(_trade_asset):
                _bs = AlertsBullishStrategy(_trade_asset)
            else:
                _bs = AlertsBearishStrategy(_trade_asset)

            _run_strategy_maker = threading.Thread(target=run_strategy, args=(_bs,),
                                                   name='_run_strategy_maker_{}'.format(_trade_asset.name))
            _run_strategy_maker.start()

        time.sleep(5)


def is_fresh(_tuple, _period):
    _ts = time.time()
    return _period - (_ts - _tuple.timestamp) / 60 >= 0 if _tuple else False


def is_fresh_test(_tuple, _period, _curr_timestamp):
    _ts = _curr_timestamp / 1000
    return _period - (_ts - _tuple.timestamp) / 60 >= 0 if _tuple else False


# def is_mature(_tuple, _period):
#     _ts = time.time()
#     return (_ts - _tuple[1])/60 - _period >= 0 if _tuple else False


def adjust_sell_price(_ma7, _open, _close):
    _diff = _open - _close
    return np.round(_close + _diff / 2, 8) if _diff > sat else _close


def wait_until_order_filled(_market, _order_id):
    _status = {'status': None}
    while _status['status'] != 'FILLED':
        _status = binance_client.get_order(symbol=_market, orderId=_order_id)
        time.sleep(1)
    logger_global[0].info("{} OrderId : {} has been filled".format(_market, _order_id))


def observe_lower_price_binance(_assets):
    while 1:
        for _asset in _assets:
            if stop_signal(_asset.exchange, _asset.market, _asset.ticker, _asset.price, 1):
                _assets = list(
                    filter(lambda _a: _a.name != _asset.name, _assets))  # remove the observed asset from the list
                _btc_value = get_remaining_btc_binance()
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


def observe_lower_price_kucoin(_assets):
    while 1:
        for _asset in _assets:
            if stop_signal(_asset.exchange, _asset.market, _asset.ticker, _asset.price, 1):
                _assets = list(
                    filter(lambda _a: _a.name != _asset.name, _assets))  # remove the observed asset from the list
                _btc_value = get_remaining_btc_binance()
                _params = get_kucoin_currency_info(_asset.name)
                if _btc_value > 0:
                    BuyStrategy(_asset, _btc_value, _params).run()
                else:
                    logger_global[0].warning(
                        "{} -- {} buying not POSSIBLE, only {} BTC left".format(_asset.exchange, _asset.market,
                                                                                price_to_string(_btc_value)))
                    return
                if len(_assets) == 0:
                    logger_global[0].info("All assets OBSERVED, exiting")
                    return
        time.sleep(40)


def is_buy_possible(_asset, _btc_value, _params):
    if _asset.price:
        _min_amount = float(_params['minQty']) * _asset.price
    else:
        _min_amount = float(_params['minQty']) * _asset.buy_price
    b = 0.001 <= _btc_value > _min_amount
    return b


def get_remaining_btc_binance():
    return get_asset_quantity_binance("BTC")


def get_remaining_btc_kucoin():
    return float(get_or_create_kucoin_trade_account('BTC')['available'])


def get_market(_asset):
    return "{}BTC".format(_asset.name)


class TimeTuple(object):
    def __init__(self, value, timestamp):
        self.value = value
        self.timestamp = timestamp / 1000


def save_to_file(_dir, filename, obj):
    with open(_dir + filename + '.pkl', 'wb') as handle:
        pickle.dump(obj, handle, protocol=pickle.HIGHEST_PROTOCOL)
        handle.close()


def get_pickled(_dir, filename):
    with open(_dir + filename + '.pkl', 'rb') as handle:
        data = pickle.load(handle)
        handle.close()
        return data


keys = None
keys_b = None
binance_client = None
kucoin_client = []

binance_obj = None
kucoin_client = []


def lib_initialize():
    global keys
    global keys_b
    global binance_client
    global binance_obj
    keys = get_pickled(key_dir, keys_filename)
    keys_b = keys['binance']
    binance_client = BinanceClient(keys_b[0], keys_b[1])
    binance_obj = Binance(keys_b[0], keys_b[1])


def get_binance_obj():
    return binance_obj


sat = 1e-8
delta = 1e-21

general_fee = 0.001
kucoin_general_fee = 0.001


def get_binance_interval_unit(_ticker, _no_such_market):
    if _no_such_market is True:
        return {
            BinanceClient.KLINE_INTERVAL_1MINUTE: "30 days ago",
            BinanceClient.KLINE_INTERVAL_3MINUTE: "30 days ago",
            BinanceClient.KLINE_INTERVAL_5MINUTE: "100 days ago",
            BinanceClient.KLINE_INTERVAL_15MINUTE: "100 days ago",
            BinanceClient.KLINE_INTERVAL_30MINUTE: "100 days ago",
            BinanceClient.KLINE_INTERVAL_1HOUR: "500 days ago",
            BinanceClient.KLINE_INTERVAL_2HOUR: "1000 days ago",
            BinanceClient.KLINE_INTERVAL_4HOUR: "1000 days ago",
            BinanceClient.KLINE_INTERVAL_6HOUR: "1000 days ago",
            BinanceClient.KLINE_INTERVAL_8HOUR: "1000 days ago",
            BinanceClient.KLINE_INTERVAL_12HOUR: "1000 days ago",
            BinanceClient.KLINE_INTERVAL_1DAY: "1000 days ago",
            BinanceClient.KLINE_INTERVAL_3DAY: "1000 days ago",
            BinanceClient.KLINE_INTERVAL_1WEEK: "1000 days ago",
        }[_ticker]
    elif _no_such_market == "strategy":
        return {
            BinanceClient.KLINE_INTERVAL_4HOUR: "5 days ago",
            BinanceClient.KLINE_INTERVAL_6HOUR: "5 days ago",
            BinanceClient.KLINE_INTERVAL_8HOUR: "5 days ago",
            BinanceClient.KLINE_INTERVAL_12HOUR: "5 days ago",
            BinanceClient.KLINE_INTERVAL_1DAY: "10 days ago"
        }[_ticker]
    else:
        return {
            BinanceClient.KLINE_INTERVAL_1MINUTE: "6 hours ago",
            BinanceClient.KLINE_INTERVAL_3MINUTE: "18 hours ago",
            BinanceClient.KLINE_INTERVAL_5MINUTE: "1 days ago",
            BinanceClient.KLINE_INTERVAL_15MINUTE: "2 days ago",
            BinanceClient.KLINE_INTERVAL_30MINUTE: "4 days ago",
            BinanceClient.KLINE_INTERVAL_1HOUR: "4 days ago",
            BinanceClient.KLINE_INTERVAL_2HOUR: "4 days ago",
            BinanceClient.KLINE_INTERVAL_4HOUR: "10 days ago",
            BinanceClient.KLINE_INTERVAL_6HOUR: "10 days ago",
            BinanceClient.KLINE_INTERVAL_8HOUR: "10 days ago",
            BinanceClient.KLINE_INTERVAL_12HOUR: "10 days ago",
            BinanceClient.KLINE_INTERVAL_1DAY: "30 days ago",
            BinanceClient.KLINE_INTERVAL_3DAY: "30 days ago",
            BinanceClient.KLINE_INTERVAL_1WEEK: "30 days ago",
        }[_ticker]


def get_timestamp(_min, _hrs, _days, _weeks):
    return datetime.datetime.now() - timedelta(minutes=_min, hours=_hrs, days=_days, weeks=_weeks)


def get_kucoin_interval_unit(_ticker, _multiplier=360):
    return int({
                   '1min': get_timestamp(1 * _multiplier, 0, 0, 0),
                   '3min': get_timestamp(3 * _multiplier, 0, 0, 0),
                   '5min': get_timestamp(5 * _multiplier, 0, 0, 0),
                   '15min': get_timestamp(15 * _multiplier, 0, 0, 0),
                   '30min': get_timestamp(30 * _multiplier, 0, 0, 0),
                   '1hour': get_timestamp(0, 1 * _multiplier, 0, 0),
                   '2hour': get_timestamp(0, 2 * _multiplier, 0, 0),
                   '4hour': get_timestamp(0, 4 * _multiplier, 0, 0),
                   '6hour': get_timestamp(0, 6 * _multiplier, 0, 0),
                   '8hour': get_timestamp(0, 8 * _multiplier, 0, 0),
                   '12hour': get_timestamp(0, 12 * _multiplier, 0, 0),
                   '1day': get_timestamp(0, 0, 1 * _multiplier, 0),
                   '1week': get_timestamp(0, 0, 0, 1 * _multiplier),
               }[_ticker].timestamp())


def stop_signal(_exchange, _market, _ticker, _stop_price, _times=4):
    stop_when_not_exchange(_exchange)
    if _exchange == 'kucoin':
        _klines = get_kucoin_klines(_market, _ticker, get_kucoin_interval_unit(_ticker))
    elif _exchange == "binance":
        _klines = get_binance_klines(_market, _ticker, get_binance_interval_unit(_ticker))
    if len(_klines) > 0:
        _mean_close_price = np.mean(list(map(lambda x: float(x.closing), _klines[-_times:])))
        return True if _mean_close_price <= _stop_price else False
    return False


def stop_signal_ext(_sell_asset):
    _price, _volume = get_first_bid(_sell_asset.market)
    if _price < _sell_asset.stop_loss_price - _sell_asset.delta_price:
        return True
    return _price <= _sell_asset.stop_loss_price and _volume <= _sell_asset.stop_loss_volume


def get_first_bid(_market):
    _price = float(binance_client.get_order_book(symbol=_market, limit=5)['bids'][0][0])
    _volume = float(binance_client.get_order_book(symbol=_market, limit=5)['bids'][0][1])
    return _price, _volume


def get_klines_asset(_asset):
    stop_when_not_exchange(_asset.exchange)
    if _asset.exchange == 'kucoin':
        _klines = get_kucoin_klines(_asset.market, _asset.ticker, get_kucoin_interval_unit(_asset.ticker))
    elif _asset.exchange == "binance":
        _klines = get_binance_klines(_asset.market, _asset.ticker, get_binance_interval_unit(_asset.ticker))
    return _klines


def check_horizontal_price_level(_asset, _type, _klines):
    if _asset.buy_price == 0:
        return False
    if not _asset.horizon:
        return False
    if len(_klines) > 0:
        _closing_price = get_last_closing_price2(_klines)
        if _type == "up":
            return True if _closing_price >= _asset.buy_price else False
        elif _type == "down":
            return True if _closing_price <= _asset.buy_price else False
    return False


def get_last_closing_price(_asset):
    stop_when_not_exchange(_asset.exchange)
    if not _asset.line and (not _asset.horizon or _asset.buy_price == 0):
        return False
    if _asset.exchange == 'kucoin':
        _klines = get_kucoin_klines(_asset.market, _asset.ticker, get_kucoin_interval_unit(_asset.ticker))
    elif _asset.exchange == "binance":
        _klines = get_binance_klines(_asset.market, _asset.ticker, get_binance_interval_unit(_asset.ticker))
    return _klines[-2].closing if len(_klines) > 0 else False


def get_last_closing_price2(_klines):
    return _klines[-2].closing if len(_klines) > 0 else False


def stop_when_not_exchange(_name):
    if _name not in ['kucoin', 'binance']:
        sys.exit("You must provide exchange parameter value: [binance, kucoin]")


def get_time_from_binance_tmstmp(_tmstmp):
    return get_time(datetime.datetime.fromtimestamp(_tmstmp / 1000).timestamp()) if _tmstmp > 0 else 0


def get_sell_price(asset):
    _depth = binance_client.get_order_book(symbol=asset.market)
    _highest_bid = float(_depth['bids'][0][0])
    if asset.tight:
        _sell_price = _highest_bid
    else:
        _sell_price = _highest_bid + asset.price_ticker_size
    return _sell_price


def highest_bid(market):
    _depth = binance_client.get_order_book(symbol=market)
    return float(_depth['bids'][0][0])


def lowest_ask(market):
    _depth = binance_client.get_order_book(symbol=market)
    return float(_depth['asks'][0][0])


def cancel_binance_orders(open_orders, symbol):
    _resp = []
    for _order in open_orders:
        _resp.append(binance_client.cancel_order(symbol=symbol, orderId=_order['orderId']))
    return all(_c["status"] == "CANCELED" for _c in _resp)


def cancel_binance_margin_orders(_open_margin_orders, symbol):
    _resp = []
    for _order in _open_margin_orders:
        _resp.append((binance_client.cancel_margin_order(symbol=symbol, orderId=_order['orderId'], isIsolated=True),
                      _order['origQty']))
    return _resp


def cancel_kucoin_orders(open_orders):
    _resp = []
    for _order in open_orders['items']:
        _resp.append(kucoin_client.cancel_order(_order['id']))
    return len(list(filter(lambda x: len(x['cancelledOrderIds']) > 0, _resp))) == open_orders['totalNum']


def cancel_binance_current_orders(market):
    _open_orders = binance_client.get_open_orders(symbol=market)
    logger_global[0].info("{} orders to cancel : {}".format(market, len(_open_orders)))
    _cancelled_ok = True
    if len(_open_orders) > 0:
        _cancelled_ok = False
        _cancelled_ok = cancel_binance_orders(_open_orders, market)
    else:
        logger_global[0].warning("{} No orders to CANCEL".format(market))
        return
    if _cancelled_ok:
        logger_global[0].info("{} Orders cancelled correctly".format(market))
    else:
        logger_global[0].error("{} Orders not cancelled properly".format(market))


def cancel_binance_current_margin_orders(market):
    _open_margin_orders = binance_client.get_open_margin_orders(symbol=market, isIsolated=True)
    logger_global[0].info("{} orders to cancel : {}".format(market, len(_open_margin_orders)))
    _cancelled_ok = True
    _available_margin_asset = -1
    if len(_open_margin_orders) > 0:
        _cancelled_ok = False
        _response = cancel_binance_margin_orders(_open_margin_orders, market)
        _cancelled_ok = all(_c[0]["status"] == "CANCELED" for _c in _response)
        _available_margin_asset = sum(map(lambda x: float(x[1]), _response))
    else:
        logger_global[0].warning("{} No isolated margin orders to CANCEL".format(market))
        return _available_margin_asset
    if _cancelled_ok:
        logger_global[0].info("{} Isolated margin orders cancelled correctly".format(market))
    else:
        logger_global[0].error("{} Isolated margin orders not cancelled properly".format(market))
    return _available_margin_asset


def cancel_kucoin_current_orders(market):
    _open_orders = kucoin_client.get_orders(symbol=market, status='active')
    logger_global[0].info("{} orders to cancel : {}".format(market, _open_orders['totalNum']))
    _cancelled_ok = True
    if _open_orders['totalNum'] > 0:
        _cancelled_ok = False
        _cancelled_ok = cancel_kucoin_orders(_open_orders)
        time.sleep(5)
    else:
        logger_global[0].warning("{} No orders to CANCEL".format(market))
        return
    if _cancelled_ok:
        logger_global[0].info("{} Orders cancelled correctly".format(market))
    else:
        logger_global[0].error("{} Orders not cancelled properly".format(market))


def get_asset_quantity_binance(currency):
    return float(binance_client.get_asset_balance(currency)['free'])


def get_margin_asset_quantity_binance(currency):
    return float(binance_client.get_asset_balance(currency)['free'])


def get_kucoin_currency_info(currency):
    return kucoin_client.get_currency(currency)


def get_lot_size_params(market):
    _info = list(filter(lambda f: f['filterType'] == "LOT_SIZE", binance_client.get_symbol_info(market)['filters']))
    return _info[0] if len(_info) > 0 else False


def get_filter(_market, _filter):
    _info = list(filter(lambda f: f['filterType'] == _filter, binance_client.get_symbol_info(_market)['filters']))
    return _info[0] if len(_info) > 0 else False


def get_filters(_market, _filter):
    return binance_client.get_symbol_info(_market)['filters']


def get_binance_price_tick_size(market):
    return float(get_filter(market, "PRICE_FILTER")['tickSize'])


def get_kucoin_symbol(_market, _symbol):
    _r = list(filter(lambda x: x['symbol'] == _market, kucoin_client.get_symbols()))
    return False if len(_r) == 0 else _r[0][_symbol]


def round_float_price(_value, _increment):
    # _val_magnitude = abs(int(np.log10(_value)))
    # _value_cp = _value * np.power(10, _val_magnitude)
    _nd = abs(int(np.log10(_increment)))
    _out = round(_value, _nd)
    if _value - round(_value, _nd) < 0:
        _out -= _increment
    # _out = round(_out / np.power(10, _val_magnitude), 12)
    return round(_out, _nd)


def round_price(_p, _s=0):
    if _p > 1000:
        _r = round(_p)
    elif _p > 100:
        _r = round(_p, 1-_s)
    elif _p > 10:
        _r = round(_p, 2-_s)
    elif _p > 1:
        _r = round(_p, 3)
    elif _p > 0.1:
        _r = round(_p, 4)
    elif _p > 0.01:
        _r = round(_p, 5)
    elif _p > 0.001:
        _r = round(_p, 6)
    else:
        _r = _p
    return _r

def adjust_kucoin_order_size(_asset, _value):
    return round_float_price(_value, _asset.kucoin_increment)


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
        _power = 0 if _power < 0 else _power
        _adjusted_quantity = round(quantity, _power)
        if _adjusted_quantity > quantity:
            _adjusted_quantity -= _min_sell_amount
        return _adjusted_quantity


def buy_order(_asset, _quantity):
    _price_str = price_to_string(_asset.buy_price)
    logger_global[0].info(
        "{} Buy limit order to be placed: price={} BTC, quantity={} ".format(_asset.market, _price_str, _quantity))
    _resp = binance_client.order_limit_buy(symbol=_asset.market, quantity=_quantity, price=_price_str)
    logger_global[0].info(
        "{} Buy limit order (ID : {}) placed: price={} BTC, quantity={} DONE".format(_asset.market, _resp['orderId'],
                                                                                     _price_str, _quantity))
    return _resp['orderId']


def price_to_string(_price):
    return "{:.8f}".format(_price)


def _sell_order(market, _sell_price, _quantity):
    _sell_price_str = price_to_string(_sell_price)
    logger_global[0].info(
        "{} Sell limit order to be placed: price={} BTC, quantity={} ".format(market, _sell_price_str, _quantity))
    _resp = binance_client.order_limit_sell(symbol=market, quantity=_quantity, price=_sell_price_str)
    logger_global[0].info(
        "{} Sell limit order placed: price={} BTC, quantity={} DONE".format(market, _sell_price_str, _quantity))


def _sell_margin_order(market, _sell_price, _quantity):
    _sell_price_str = price_to_string(_sell_price)
    logger_global[0].info(
        "{} Sell margin limit order to be placed: price={} BTC, quantity={} ".format(market, _sell_price_str,
                                                                                     _quantity))
    _resp = binance_client.create_margin_order(symbol=market, quantity=_quantity, price=_sell_price_str, side="SELL",
                                               type="LIMIT", isIsolated=True, timeInForce="GTC")
    logger_global[0].info(
        "{} Sell margin limit order placed: price={} BTC, quantity={} DONE".format(market, _sell_price_str, _quantity))


def _sell_margin_market(market, _sell_price, _quantity):
    _sell_price_str = price_to_string(_sell_price)
    logger_global[0].info(
        "{} Sell margin market order to be placed: quantity={} ".format(market, _quantity))
    _resp = binance_client.create_margin_order(symbol=market, quantity=_quantity, side="SELL",
                                               type="MARKET", isIsolated=True)
    logger_global[0].info(
        "{} Sell margin market order placed: price={} BTC, quantity={} DONE".format(market, _sell_price_str, _quantity))


def sell_limit(market, asset_name, price):
    cancel_binance_current_orders(market)
    _quantity = get_asset_quantity_binance(asset_name)
    _lot_size_params = get_lot_size_params(market)
    _quantity = adjust_quantity(_quantity, _lot_size_params)
    if _quantity:
        logger_global[0].info("{} : {} : {}".format(market, price, _quantity))
        _sell_order(market, price, _quantity)
        return _quantity
    else:
        logger_global[0].error("{} No quantity to SELL".format(market))
        return False


def sell_margin_limit(market, asset_name, price):
    cancel_binance_current_orders(market)
    _quantity = get_asset_quantity_binance(asset_name)
    _lot_size_params = get_lot_size_params(market)
    _quantity = adjust_quantity(_quantity, _lot_size_params)
    if _quantity:
        logger_global[0].info("{} : {} : {}".format(market, price, _quantity))
        _sell_order(market, price, _quantity)
        return _quantity
    else:
        logger_global[0].error("{} No quantity to SELL".format(market))
        return False


def sell_limit_stop_loss(market, asset):
    if asset.exchange == 'binance':
        cancel_binance_current_orders(market)
        _quantity = get_asset_quantity_binance(asset.name)
        _sell_price = get_sell_price(asset)
        _lot_size_params = get_lot_size_params(market)
        _quantity = adjust_quantity(_quantity, _lot_size_params)
        if _quantity:
            _sell_order(market, _sell_price, _quantity)
    if asset.exchange == 'kucoin':
        cancel_kucoin_current_orders(market)
        _account = get_or_create_kucoin_trade_account(asset.name)
        _amount = float(_account['available'])
        if _amount == 0.0:
            raise AccountHoldingZero("You don't have any amount to sell for market: {}".format(asset.market))
        kucoin_client.create_market_order(asset.market, KucoinClient.SIDE_SELL, size=_amount)


def sell_margin_limit_stop_loss(_asset):
    if _asset.exchange == 'binance':
        _available_margin_asset = cancel_binance_current_margin_orders(_asset.market)
        _sell_price = get_sell_price(_asset)
        _lot_size_params = get_lot_size_params(_asset.market)
        _quantity = _available_margin_asset
        if _asset.market_sell and _quantity:
            _sell_margin_market(_asset.market, _sell_price, _quantity)
        elif _quantity:
            _sell_margin_order(_asset.market, _sell_price, _quantity)


def get_or_create_kucoin_trade_account(_currency):
    for _account in kucoin_client.get_accounts():
        if _account['currency'] == _currency:
            return _account
    return kucoin_client.get_account(kucoin_client.create_account('trade', _currency)['id'])


class AccountHoldingZero(Exception):
    def __init__(self, message):
        super(Exception, self).__init__(message)

    def __str__(self):
        return self.args[0]


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


def get_format_price(_price):
    _price = float(_price)
    _f = "{" + ":.{}f".format(get_price_magnitude(_price)) + "}"
    return _f.format(_price)


def get_price_magnitude(_price):
    _m = abs(int(np.log10(_price)))
    _l = len(str(round(_price * 10 ** _m, 10)))
    _l = _l - 2 if _price <= 1 else _l
    return _l + _m


def stop_loss_func(_asset):
    if _asset.exchange == 'binance':
        _ticker = BinanceClient.KLINE_INTERVAL_1MINUTE
        _time_interval = get_binance_interval_unit(_ticker)
    if _asset.exchange == 'kucoin':
        _ticker = ticker_to_kucoin(BinanceClient.KLINE_INTERVAL_5MINUTE)
        _time_interval = get_kucoin_interval_unit(_ticker)

    _stop_price = _asset.stop_loss_price

    logger_global[0].info("{} -- Starting {} stop-loss maker".format(_asset.exchange, _asset.market))
    logger_global[0].info(
        "Stop price {} is set up to : {} BTC".format(_asset.market, get_format_price(_stop_price)))

    while 1:
        if type(_asset) is TradeAsset:
            if not _asset.trading:
                logger_global[0].info(
                    "{} -- {} Stop-Loss : sold, not trading, skipping, exiting".format(_asset.exchange, _asset.market))
                sys.exit(0)
        try:
            _stop_sl = stop_signal(_asset.exchange, _asset.market, _ticker, _stop_price, 1)
            # stop = True
            if _stop_sl:
                sell_limit_stop_loss(_asset.market, _asset)
                logger_global[0].info(
                    "{} -- Stop-loss LIMIT {} order has been made : {}, exiting".format(_asset.exchange, _asset.market,
                                                                                        lowest_ask(
                                                                                            _asset.market)))
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
    return (curr_price - asset.buy_price) / asset.buy_price >= asset.take_profit_ratio / 100.0


def is_trading_possible(_assets):
    return not any(filter(lambda _a: _a.running, _assets))


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
    _ticker = BinanceClient.KLINE_INTERVAL_1MINUTE
    _time_interval = get_binance_interval_unit(_ticker)
    _prev_kline = None
    _prev_rsi = TimeTuple(0, 0)
    _time_frame = 60  # last 60 candles
    while 1:
        if type(asset) is TradeAsset:
            if not asset.trading:
                logger_global[0].info("{} Take profit : sold, not trading, skipping, exiting".format(asset.market))
                sys.exit(0)
        try:
            _klines = get_klines(asset.market, _ticker, _time_interval)
            # _klines = get_pickled('/juno/', "klines")
            # _klines = _klines[33:-871]
            _closes = get_closes(_klines)
            _highs = get_highs(_klines)

            _ma50 = talib.MA(_closes, timeperiod=50)
            _ma20 = talib.MA(_closes, timeperiod=20)
            _ma7 = talib.MA(_closes, timeperiod=7)

            _local_rsi_max_value = get_rsi_local_max_value(_closes, _prev_rsi.value, 10, asset)

            _stop = -1
            _curr_kline = _klines[-1]

            # plt.plot(_ma50[0:_stop:1], 'red', lw=1)
            # plt.plot(_ma20[0:_stop:1], 'blue', lw=1)
            # plt.plot(_ma7[0:_stop:1], 'green', lw=1)
            # plt.show()

            _high_price_max = np.max(get_last(_highs, _stop, _time_frame))

            _rsi = relative_strength_index(_closes, _prev_rsi.value, 14, asset)
            _rsi_max = np.max(get_last(_rsi, _stop, _time_frame))
            _index_rsi_peak = np.where(_rsi == _rsi_max)[0][0]
            _curr_rsi = get_last(_rsi, _stop)

            _curr_ma_50 = get_last(_ma50, _stop)
            _curr_ma_20 = get_last(_ma20, _stop)
            _curr_ma_7 = get_last(_ma7, _stop)

            _max_volume = get_max_volume(_klines, _time_frame)

            _c1 = rsi_falling_condition(_rsi_max, _curr_rsi, _local_rsi_max_value)
            _c2 = volume_condition(_klines, _max_volume)
            _c3 = candle_condition(_curr_kline, _curr_ma_7, _curr_ma_50)
            _c4 = mas_condition(_curr_ma_7, _curr_ma_20, _curr_ma_50)
            _c5 = is_profitable(asset, _closes[-1])
            _c6 = is_red_candle(_curr_kline)

            if _c6 and _c5 and _c1 and _c2 and _c3 and _c4:
                logger_global[0].info("Taking profits {} conditions satisfied...".format(asset.market))
                _curr_open = float(_curr_kline[1])
                _ask_price = _curr_open
                if _curr_open < _curr_ma_7:
                    _curr_high = float(_curr_kline[2])
                    _ask_price = adjust_ask_price(asset, _prev_kline, _ask_price, _high_price_max, _curr_high)
                _sold = sell_limit(asset.market, asset.name, _ask_price)
                if _sold:
                    logger_global[0].info("Took profits {}: LIMIT order has been made, exiting".format(asset.market))
                else:
                    logger_global[0].error("Took profits {}: selling not POSSIBLE, exiting".format(asset.market))
                sys.exit(0)
            _prev_kline = _curr_kline
            _prev_rsi = TimeTuple(_rsi, _curr_kline[0])
            time.sleep(40)
        except Exception as err:
            if isinstance(err, requests.exceptions.ConnectionError):
                logger_global[0].error("{} Connection problem...".format(asset.market))
            else:
                traceback.print_tb(err.__traceback__)
                logger_global[0].exception(err.__traceback__)
                time.sleep(40)


def is_red_candle(_kline):
    __close = float(_kline[4])
    __open = float(_kline[1])
    return __close - __open < 0


def is_green_candle(_kline):
    __close = float(_kline[4])
    __open = float(_kline[1])
    return __close - __open >= 0


def get_rsi_local_max_value(_closes, _prev_rsi, _window=10, _asset=None):
    _start = 33
    _stop = -1
    _rsi = relative_strength_index(_closes, _prev_rsi, 14, _asset)
    _rsi_max_val, _rsi_reversed_max_ind = find_first_maximum(_rsi[_start:_stop:1], _window)
    return _rsi_max_val


def get_volume(_kline):
    return float(_kline[7])


def get_max_volume(_klines, _time_frame):
    return np.max(
        list(map(lambda x: float(x[7]),
                 _klines[-_time_frame:-1])))  # get max volume within last _time_frame klines, without current volume


def mas_condition(_curr_ma_7, _curr_ma_20, _curr_ma_50):
    return _curr_ma_7 > _curr_ma_20 > _curr_ma_50


def candle_condition(_kline, _curr_ma_7, _curr_ma_50):
    _close = float(_kline[4])
    _low = float(_kline[3])
    return _close < _curr_ma_7 or _low < _curr_ma_50


def volume_condition(_klines, _max_volume, _ratio=0.6):
    _vol_curr = float(_klines[-1][7])
    return 10.0 > _vol_curr / _max_volume > _ratio


def rsi_falling_condition(_rsi_max, _curr_rsi, _local_rsi_max_value):
    return _local_rsi_max_value > 70 and _rsi_max - _curr_rsi > 0 and _rsi_max > 76.0 and _curr_rsi < 65.0 and (
            _rsi_max - _curr_rsi) / _rsi_max > 0.2


def get_closes(_klines):
    return np.array(list(map(lambda _x: float(_x[4]), _klines)))


def get_closes2(_klines):
    return np.array(list(map(lambda _x: float(_x.closing), _klines)))


def get_highs(_klines):
    return np.array(list(map(lambda _x: float(_x[2]), _klines)))


def is_bullish_setup(asset):  # or price lower than MA100
    _time_interval = get_binance_interval_unit(asset.ticker)
    _klines = get_klines(asset, _time_interval)
    # _klines = get_pickled('/juno/', "klines")
    _stop = -1  # -5*60-30-16-10
    _start = 33
    _curr_kline = _klines[-1]
    _closes = get_closes(_klines)
    _time_horizon = 6 * 60

    _ma100 = talib.MA(_closes, timeperiod=100)

    _curr_ma100 = _ma100[-1]

    # _below, _above = price_counter(_ma100[-_time_horizon:], _closes[-_time_horizon:], _time_horizon)

    _min_ma100, _ma100_reversed_min_ind = find_minimum(_ma100[-_time_horizon:])

    # _ma50 = talib.MA(_closes, timeperiod=50)
    # _ma20 = talib.MA(_closes, timeperiod=20)
    #
    # import matplotlib.pyplot as plt
    # plt.plot(_ma100[_start:_stop:1], 'green', lw=1)
    # plt.plot(_ma50[_start:_stop:1], 'red', lw=1)
    # plt.show()

    # _flipped_values = np.max(_values[_start:_stop:1]) - _values
    # _max_val, _reversed_max_ind = find_maximum(_flipped_values[_start:_stop:1], 2)

    return _curr_ma100 - _min_ma100 > 0
    # return True


def price_counter(_ma200, _closes, _time_horizon):
    _above = 0
    _below = 0
    for _i in range(0, _time_horizon):
        if (_ma200[-_time_horizon:][_i] - _closes[-_time_horizon:][_i]) / _closes[-_time_horizon:][_i] > 0.001:
            _below += 1
        else:
            _above += 1
    return _below, _above


def relative_strength_index(_closes, _prev_rsi=None, n=14, _asset=None):
    try:
        _rsi = talib.RSI(_closes, timeperiod=14)
    except Warning:
        # logger_global[0].error("{} RSI computing error".format(_asset.market))
        # save_to_file(trades_logs_dir, "broken_rsi_closes_{}".format(time.time()), _closes)
        _rsi = _prev_rsi

    if len(list(filter(lambda x: x == 0, get_last(_rsi, 10)))) == 10:
        _rsi = _prev_rsi

    return _rsi


# def relative_strength_index(_closes, _prev_rsi=None, n=14, _asset=None):
#     try:
#         _prices = np.array(_closes, dtype=np.float32)
#
#         _deltas = np.diff(_prices)
#         _seed = _deltas[:n + 1]
#         _up = _seed[_seed >= 0].sum() / n
#         _down = -_seed[_seed < 0].sum() / n
#         _rs = _up / _down
#         _rsi = np.zeros_like(_prices)
#         _rsi[:n] = 100. - 100. / (1. + _rs)
#
#         for _i in range(n, len(_prices)):
#             _delta = _deltas[_i - 1]  # cause the diff is 1 shorter
#
#             if _delta > 0:
#                 _upval = _delta
#                 _downval = 0.
#             else:
#                 _upval = 0.
#                 _downval = -_delta
#
#             _up = (_up * (n - 1) + _upval) / n
#             _down = (_down * (n - 1) + _downval) / n
#
#             _rs = _up / _down
#             _rsi[_i] = 100. - 100. / (1. + _rs)
#     except Warning:
#         # logger_global[0].error("{} RSI computing error".format(_asset.market))
#         # save_to_file(trades_logs_dir, "broken_rsi_closes_{}".format(time.time()), _closes)
#         _rsi = _prev_rsi
#
#     return _rsi


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
    _not_passed = check_prices(assets)
    if len(_not_passed) > 0:
        logger_global[0].error(
            "BuyAsset prices not coherent : {}, stopped".format(' '.join(map(lambda x: x.name, _not_passed))))
        raise Exception("BuyAsset prices not coherent, stopped")
    logger_global[0].info("BuyAsset prices : OK")


def check_prices(assets):
    _n = []
    for x in assets:
        if not x.price_profit > x.price > x.stop_loss_price:
            _n.append(x)
    return _n


def check_observe_assets(assets):
    logger_global[0].info("Checking ObserveAsset prices...")
    _not_passed = check_prices(assets)
    if len(_not_passed) > 0:
        logger_global[0].error(
            "ObserveAsset prices not coherent : {}, stopped".format(' '.join(map(lambda x: x.name, _not_passed))))
        raise Exception("ObserveAsset prices not coherent, stopped")
    logger_global[0].info("ObserveAsset prices : OK")


def adjust_buy_asset_btc_volume(_buy_assets, _btc_value):
    list(map(lambda x: x.set_btc_asset_buy_value(_btc_value), _buy_assets))


def find_first_maximum(_values, _window):  # find the first maximum
    _range = int(len(_values) / _window)
    _max_val = -1
    _min_stop_level = 0.9
    # _activate_stop = False
    _max_ind = -1
    _maxes = []
    for _i in range(0, _range - 1):
        _i_max = np.max(_values[len(_values) - (_i + 1) * _window - 1:len(_values) - _i * _window - 1])
        _tmp = list(_values[len(_values) - (_i + 1) * _window - 1:len(_values) - _i * _window - 1])
        _index = _window - _tmp.index(max(_tmp)) + _i * _window + 1
        _maxes.append((_i_max, _index))
        if _i_max > _max_val:
            _max_val = _i_max
            _max_ind = _index
            # if _max_val > 0:
            #     _activate_stop = True
        if _i_max < _min_stop_level * _max_val:
            _res = check_maxes(_values, _maxes)
            if _res[1] > -1:
                return _res
    return check_maxes(_values, _maxes)


def check_maxes(_data, _tuples):
    _tuples = sorted(_tuples, key=lambda x: x[0], reverse=True)
    for _tuple in _tuples:
        if _data[-_tuple[1] - 1] < _tuple[0] > _data[-_tuple[1] + 1]:
            return _tuple
    return -1, -1


def check_mins(_data, _tuples):
    _tuples = sorted(_tuples, key=lambda x: x[0])
    for _tuple in _tuples:
        if _data[-_tuple[1] - 1] >= _tuple[0] <= _data[-_tuple[1] + 1]:
            return _tuple
    return -1, -1


def find_first_minimum(_values, _window, _reversed=False):  # find the first maximum
    _range = int(len(_values) / _window)
    _min_val = 1000
    _min_stop_level = 0.9
    # _activate_stop = False
    _min_ind = -1
    _mins = []
    for _i in range(0, _range - 1):
        _i_min = np.min(_values[len(_values) - (_i + 1) * _window - 1:len(_values) - _i * _window - 1])
        _tmp = list(_values[len(_values) - (_i + 1) * _window - 1:len(_values) - _i * _window - 1])
        _index = _window - _tmp.index(min(_tmp)) + _i * _window + 1
        if _mins and _i_min < _mins[-1][0]:
            _mins.append((_i_min, _index))
        elif _mins and _i_min > _mins[-1][0]:
            return _reverse_result(check_mins(_values, _mins), _values, _reversed)
        if not _mins:
            _mins.append((_i_min, _index))
        if _i_min < _min_val:
            _min_val = _i_min
            _min_ind = _index
            # if _min_val < 1000:
            #     _activate_stop = True
        if _i_min > _min_stop_level * _min_val:
            _res = check_mins(_values, _mins)
            if _res[1] > -1:
                return _reverse_result(_res, _values, _reversed)
    return _reverse_result(check_mins(_values, _mins), _values, _reversed)


def _reverse_result(_tuple, _values, _reversed=False):
    if _reversed:
        return _tuple[0], len(_values) - _tuple[0] - 1
    else:
        return _tuple


def find_maximum_2(_values, _window):  # find the first maximum
    _range = int(len(_values) / _window)
    _max_val = -1
    _min_stop_level = 0.9
    _activate_stop = False
    _max_ind = -1
    for _i in range(0, _range - 1):
        _i_max = np.max(_values[len(_values) - (_i + 1) * _window - 1:len(_values) - _i * _window - 1])
        _tmp = list(_values[len(_values) - (_i + 1) * _window - 1:len(_values) - _i * _window - 1])
        _index = _window - _tmp.index(max(_tmp)) + _i * _window + 1
        if _i_max > _max_val:
            _max_val = _i_max
            _max_ind = _index
    return _max_val, _max_ind


def find_minimum_2(_values, _window):
    _range = int(len(_values) / _window)
    _min_val = 1000
    _min_stop_level = 0.9
    _activate_stop = False
    _min_ind = -1
    for _i in range(0, _range - 1):
        _i_min = np.min(_values[len(_values) - (_i + 1) * _window - 1:len(_values) - _i * _window - 1])
        _tmp = list(_values[len(_values) - (_i + 1) * _window - 1:len(_values) - _i * _window - 1])
        _index = _window - _tmp.index(min(_tmp)) + _i * _window + 1
        if _i_min < _min_val:
            _min_val = _i_min
            _min_ind = _index
    return _min_val, _min_ind


def find_local_maximum(_values, _window, _i=0):
    _max = find_first_maximum(_values, _window)
    _range = int(len(_values) / _window)
    if _range * _window - _max[1] < _window <= len(_values):
        return find_local_maximum(_values[_window:], _window, _i + 1)
    elif _window <= len(_values):
        return _max[0], _max[1]
    else:
        return -1, -1


def find_minimum(values):
    _range = len(values)
    _min = values[-1]
    _ind = -1
    _threshold = 0.001
    for _i in range(0, _range - 1):
        if (_min - values[-_i]) / values[-_i] > _threshold:
            _min = values[-_i]
            _ind = -_i
    return _min, _ind


def get_angle(p1, p2):
    return np.arctan((p2[1] - p1[1]) / (p2[0] - p1[0])) * 180 / np.pi


def get_magnitude(_reversed_max_ind, _max_val):
    try:
        return int(np.log10(_reversed_max_ind / np.abs(_max_val)))
    except Warning:
        return -1


def not_equal_rsi(_rsi_1, _rsi_2):
    return not _rsi_1 or not _rsi_2 or _rsi_1 != _rsi_2.value


def get_one_of_rsi(_rsi_fresh, _rsi_):
    if _rsi_fresh or _rsi_:
        if _rsi_fresh:
            return _rsi_fresh
        else:
            return _rsi_
    else:
        return False


def get_time(_timestamp):
    return datetime.datetime.fromtimestamp(_timestamp).strftime('%d %B %Y %H:%M:%S')


def price_drop(price0, price1, _ratio):
    return (price0 - price1) / price0 > _ratio


def get_green_candles(_klines):
    return list(filter(lambda x: float(x[4]) - float(x[1]) >= 0, _klines))


def slope(x1, y1, x2, y2):
    return (y2 - y1) / (x2 - x1)


def bias(x1, y1, x2, y2):
    return y1 - x1 * (y1 - y2) / (x1 - x2)


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


def send_mail(subject, message, asset=False):
    global variable

    sender = config.get_parameter('sender')
    receiver = config.get_parameter('receiver')

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = receiver

    msg.attach(MIMEText(message, 'html'))

    smtp_server = "smtp.wp.pl"
    port = 587  # For starttls
    password = variable

    # Create a secure SSL context
    context = ssl.create_default_context()
    # Try to log in to server and send email

    try:
        server = smtplib.SMTP(smtp_server, port)
        server.ehlo()  # Can be omitted
        server.starttls(context=context)  # Secure the connection
        server.ehlo()  # Can be omitted
        server.login(sender, password)
        server.send_message(msg)
        if asset:
            logger_global[0].info("{} Email sent".format(asset.name))
        else:
            logger_global[0].info(f"Email sent : {subject}")
    except Exception as err:
        traceback.print_tb(err.__traceback__)
        logger_global[0].exception(err.__traceback__)
    finally:
        server.quit()


def authorize():
    global variable
    _phrase = config.get_parameter('phrase')
    _input = getpass("Pass: ")
    _hash = hashlib.sha512(_input.encode('utf-8')).hexdigest()
    if _phrase != _hash:
        print('Exiting...BYE!')
        exit(0)
    variable = _input[1:len(_input) - 1]
    logger_global[0].info('Authorized : OK')


def dump_variables(_market, _prev_rsi_high, _trigger, _rsi_low, _rsi_low_fresh, _last_ma7_gt_ma100,
                   _big_volume_sold_out,
                   _bearish_trigger, _slope_condition):
    logger_global[0].info(
        "{} _prev_rsi_high: {} _trigger: {} _rsi_low: {} _rsi_low_fresh: {} _last_ma7_gt_ma100: {} _big_volume_sold_out: {} _bearish_trigger: {} _slope_condition: {}".format(
            _market,
            _prev_rsi_high.value if _prev_rsi_high else False, _trigger.value if _trigger else False,
            _rsi_low.value if _rsi_low else False, _rsi_low_fresh.value if _rsi_low_fresh else False,
            _last_ma7_gt_ma100.value if _last_ma7_gt_ma100.value else False,
            _big_volume_sold_out.value if _big_volume_sold_out.value else False,
            _bearish_trigger.value if _bearish_trigger.value else False,
            _slope_condition.value if _slope_condition.value else False
        ))
    print(
        "{} _prev_rsi_high: {} _trigger: {} _rsi_low: {} _rsi_low_fresh: {} _last_ma7_gt_ma100: {} _big_volume_sold_out: {} _bearish_trigger: {} _slope_condition: {}".format(
            _market,
            _prev_rsi_high.value if _prev_rsi_high else False, _trigger.value if _trigger else False,
            _rsi_low.value if _rsi_low else False, _rsi_low_fresh.value if _rsi_low_fresh else False,
            _last_ma7_gt_ma100.value if _last_ma7_gt_ma100.value else False,
            _big_volume_sold_out.value if _big_volume_sold_out.value else False,
            _bearish_trigger.value if _bearish_trigger.value else False,
            _slope_condition.value if _slope_condition.value else False
        ))


def is_first_golden_cross_challenge(_klines):
    _closes = np.array(list(map(lambda _x: float(_x.closing), _klines)))
    _high = list(map(lambda _x: float(_x.highest), _klines))
    _low = list(map(lambda _x: float(_x.lowest), _klines))

    _ma200 = talib.MA(_closes, timeperiod=200)
    _ma50 = talib.MA(_closes, timeperiod=50)

    fall = (np.max(_high[-500:]) - np.min(_low[-500:])) / np.max(_high[-500:])  # > 22%

    _max_g = find_local_maximum(_ma50, 50)
    if check_extremum(_max_g):
        return False
    _max_l = find_local_maximum(_ma50[-_max_g[1]:], 50)
    if check_extremum(_max_l):
        return False
    _min_l = find_minimum(_ma50[-_max_g[1]:-_max_l[1]])
    if check_extremum(_min_l):
        return False
    _min_low_l = find_minimum(_low[-_max_g[1]:-_max_l[1]])
    if check_extremum(_min_low_l):
        return False
    _min_l_ind = -_max_l[1] + _min_l[1]
    _min_low_l_ind = -_max_l[1] + _min_low_l[1]
    _max_l_ind = - _max_l[1]

    _max_high_l = find_local_maximum(_high[_min_l_ind:-_max_l[1]], 10)
    _min_before_local_max = find_minimum(_low[_max_l_ind:])
    rise = (_max_high_l[0] - _min_low_l[0]) / _min_low_l[0]  # > 15%
    drop = (_max_high_l[0] - _min_before_local_max[0]) / _max_high_l[0]  # > 10%

    # 43, 36, 20 % -- these are another numbers to try...
    hours_after_local_max_ma50 = 50

    return fall > 0.22 and rise > 0.15 and drop > 0.1 and np.abs(_max_l_ind) > hours_after_local_max_ma50 and _closes[
        -1] < _ma50[-1]


def is_first_golden_cross(_klines):
    _closes = np.array(list(map(lambda _x: float(_x.closing), _klines)))
    _high = list(map(lambda _x: float(_x.highest), _klines))
    _low = list(map(lambda _x: float(_x.lowest), _klines))

    _ma200 = talib.MA(_closes, timeperiod=200)
    _ma50 = talib.MA(_closes, timeperiod=50)

    _macd, _macdsignal, _macdhist = talib.MACD(_closes, fastperiod=12, slowperiod=26, signalperiod=9)
    _rsi = relative_strength_index(_closes)

    if not is_tradeable(_closes, _rsi, _macd, _macdsignal):
        return False, _closes[-1]

    fall = (np.max(_high[-500:]) - np.min(_low[-500:])) / np.max(_high[-500:])  # > 22%

    _min_ind, _max_ind = find_global_max_min_ind(_low, _high)

    _is_fall = _min_ind < _max_ind

    _max_g = find_local_maximum(_ma50, 50)
    if check_extremum(_max_g):
        return False, _closes[-1]
    _max_l = find_local_maximum(_ma50[-_max_g[1]:], 50)
    if check_extremum(_max_l):
        return False, _closes[-1]
    _min_l = find_minimum(_ma50[-_max_g[1]:-_max_l[1]])
    if check_extremum(_min_l):
        return False, _closes[-1]
    _min_low_l = find_minimum(_low[-_max_g[1]:-_max_l[1]])
    if check_extremum(_min_low_l):
        return False, _closes[-1]
    _min_l_ind = -_max_l[1] + _min_l[1]
    _min_low_l_ind = -_max_l[1] + _min_low_l[1]
    _max_l_ind = - _max_l[1]

    _max_high_l = find_local_maximum(_high[_min_l_ind:-_max_l[1]], 10)
    _max_high_l_ind = -_max_high_l[1] + _max_l_ind
    _min_before_local_max = find_minimum(_low[_max_l_ind:])
    rise = (_max_high_l[0] - _min_low_l[0]) / _min_low_l[0]  # > 15%
    drop = (_max_high_l[0] - _min_before_local_max[0]) / _max_high_l[0]  # > 10%

    # 43, 36, 20 % -- these are another numbers to try...
    hours_after_local_max_ma50 = 50

    _not_elder_than_global_max = _max_g[1] < 500 and _max_l_ind < 500
    # _max_high_l_after_min_before_local_max = np.abs(_min_before_local_max[1]) > np.abs(_max_high_l_ind)

    _about_one_week_old = np.abs(_min_low_l_ind) - np.abs(_min_before_local_max[1]) < 150 and np.abs(
        _max_high_l_ind) - np.abs(_min_before_local_max[1]) < 150

    _bc_val, _bc_ind = bear_cross(_closes)
    _max_bc_val, _max_bc_ind0 = find_first_maximum(_closes[-_bc_ind:][::-1], 5)
    _max_bc_ind = _bc_ind - _max_bc_ind0 + 1
    _min_bc_val, _min_bc_ind0 = find_first_minimum(_closes[-_max_bc_ind - 2:][::-1], 5)
    _c_avg = get_avg_last(_closes, -5)

    _true = True
    if _bc_ind == -1 or _max_bc_ind0 == -1 or _min_bc_ind0 == -1 or _c_avg < _min_bc_val:
        _true = False

    return _true and _is_fall and _not_elder_than_global_max and fall > 0.22 and rise > 0.15 and drop > 0.1 and np.abs(
        _max_l_ind) > hours_after_local_max_ma50 and _closes[
               -1] < _ma50[-1] and _about_one_week_old, _closes[-1]


def find_global_max_min_ind(_low, _high):
    _max = np.max(_high[-500:])
    _min = np.min(_low[-500:])
    _i_max = -1
    _i_min = -1
    for _i in range(len(_high)):
        if _high[len(_high) - _i - 1] == _max:
            _i_max = _i
    for _i in range(len(_low)):
        if _low[len(_low) - _i - 1] == _min:
            _i_min = _i
    return _i_min, _i_max


def check_extremum(_data):
    return _data[1] == -1


def is_drop_below_ma50_after_rally(_klines):
    _closes = np.array(list(map(lambda _x: float(_x.closing), _klines)))
    _high = list(map(lambda _x: float(_x.highest), _klines))
    _low = list(map(lambda _x: float(_x.lowest), _klines))
    _ma200 = talib.MA(_closes, timeperiod=200)
    _ma50 = talib.MA(_closes, timeperiod=50)

    _f_ma50 = np.flip(_ma50)
    _f_low = np.flip(_low)

    _below_ma = drop_below_ma_rev(_f_ma50, _f_low)

    if _below_ma[1] == -1:
        return False, _closes[-1]

    try:
        _ma50_above_ma200 = all(_ma50[-_below_ma[1]:] > _ma200[-_below_ma[1]:])
    except RuntimeWarning:
        return False, _closes[-1]

    _low_above_ma50 = _low[-_below_ma[1]:] > _ma50[-_below_ma[1]:]
    _trues = np.sum(_low_above_ma50)
    _falses = len(_low_above_ma50) - _trues
    _ratio = _trues / (_trues + _falses)

    _max_high = find_local_maximum(_high[-_below_ma[1] - 1:], 3)

    _min_l = find_minimum(_low[-_max_high[1]:])
    _min_l_cl = find_minimum(_closes[-_max_high[1]:])
    _drop = (_max_high[0] - _min_l[0]) / _max_high[0]
    rally = (_max_high[0] - _below_ma[0]) / _below_ma[0]  # 48, 82 %

    _is_currently_below_ma50 = _low[-1] < _ma50[-1]

    _bounce = (_max_high[0] - _min_l_cl[0]) / _min_l_cl[0]

    return _bounce < 0.1 and _is_currently_below_ma50 and _ratio > 0.7 and _drop > 0.25 and rally > 0.3 and _below_ma[
        1] > 0 and _ma50_above_ma200, _closes[-1]


def is_drop_below_ma200_after_rally(_klines):
    _closes = np.array(list(map(lambda _x: float(_x.closing), _klines)))
    _high = list(map(lambda _x: float(_x.highest), _klines))
    _low = list(map(lambda _x: float(_x.lowest), _klines))
    _ma200 = talib.MA(_closes, timeperiod=200)
    _ma50 = talib.MA(_closes, timeperiod=50)
    _first_gc = find_first_golden_cross(_ma50, _ma200, 50)
    if _first_gc[1] == -1:
        return False, _closes[-1]
    _below_ma = drop_below_ma(_ma200[-_first_gc[1]:], _low[-_first_gc[1]:])
    _max_high = find_local_maximum(_high[-_first_gc[1]:], 24)

    _min_l = find_minimum(_low[-_max_high[1]:])
    _min_l_cl = find_minimum(_closes[-_max_high[1]:])
    _drop = (_max_high[0] - _min_l[0]) / _max_high[0]
    rally = (_max_high[0] - _first_gc[0]) / _first_gc[0]  # 48, 82 %

    _ma50_above_ma200 = all(_ma50[-_first_gc[1]:] > _ma200[-_first_gc[1]:])
    _low_below_ma50 = _low[-_first_gc[1]:] > _ma50[-_first_gc[1]:]
    _trues = np.sum(_low_below_ma50)
    _falses = len(_low_below_ma50) - _trues
    _ratio = _trues / (_trues + _falses)

    _bounce = (_max_high[0] - _min_l_cl[0]) / _min_l_cl[0]

    return _bounce < 0.1 and _ratio > 0.7 and _ma50_above_ma200 and _drop > 0.25 and rally > 0.5 and _below_ma[1] > 0, \
           _closes[-1]


def find_first_golden_cross(__ma50, __ma200, _offset=0):
    for i in range(_offset, len(__ma200)):
        _index = len(__ma200) - i - 1
        if __ma200[_index] > __ma50[_index]:
            return __ma200[_index], i
    return -1, -1


def drop_below_ma(_ma, _candles, _window=5, _max_window=100):
    for i in range(len(_ma)):
        if _ma[i] > _candles[i]:
            _index = len(_ma) - i - 1
            if _index > _max_window:
                return -1, -1
            if _index > _window:
                return drop_below_ma(_ma[-_index:], _candles[-_index:], _window)
            else:
                return _candles[i], _index
    return -1, -1


def drop_below_ma_rev(_ma, _candles, _window=5):
    _len = len(_ma)
    for i in range(_len):
        _j = i
        if _ma[_j] > _candles[_j]:
            _index = _j
            if _index < _window:
                if _index == 0:
                    _index += _window
                return drop_below_ma_rev(_ma[_index:], _candles[_index:], _window)
            else:
                return _candles[_j], _index
    return -1, -1


def is_second_golden_cross(_klines):
    _closes = np.array(list(map(lambda _x: float(_x.closing), _klines)))
    _ma200 = talib.MA(_closes, timeperiod=200)
    _ma50 = talib.MA(_closes, timeperiod=50)
    _max_200 = find_local_maximum(_ma200, 200)  # first a long-period maximum
    _min_200 = find_minimum_2(_ma200, 200)  # first a long-period minimum
    _max_200_1 = find_first_maximum(_ma200, 5)  # second lower max
    _min_200_1 = find_first_minimum(_ma200, 25)  # first higher minimum
    _macd, _macdsignal, _macdhist = talib.MACD(_closes, fastperiod=12, slowperiod=26, signalperiod=9)
    _rsi = relative_strength_index(_closes)

    if not is_tradeable(_closes, _rsi, _macd, _macdsignal):
        return False, _closes[-1]

    _max_50 = find_local_maximum(_ma50, 200)  # first a long-period maximum
    _min_50 = find_minimum_2(_ma50, 200)  # first a long-period minimum
    _max_50_1 = find_first_maximum(_ma50, 10)  # second lower max
    _min_50_1 = find_first_minimum(_ma50, 25)  # first higher minimum

    HL_ma50_reversal_cond = _min_50[0] < _min_50_1[0] < _max_50_1[0] < _max_50[0] and _max_50[1] > _min_50[1] > \
                            _max_50_1[1] > _min_50_1[1]
    min_after_max_low_variance = _min_200[0] < _max_200[0] and _max_200[1] > _min_200[1] and np.std(
        _ma200[-200:]) / np.mean(_ma200[-200:]) < 0.02
    before_second_golden_cross_cond = _min_50[0] < _ma200[-_min_50[1]] and _max_50_1[0] > _ma200[-_max_50_1[1]] and \
                                      _max_50_1[0] > _ma200[
                                          -_max_50_1[1]] and _min_50_1[0] < _ma200[-_min_50_1[1]]
    _is_below_ma50 = (_ma50[-1] - _closes[-1]) / _closes[-1]

    return _is_below_ma50 < 0.05 and HL_ma50_reversal_cond and min_after_max_low_variance and before_second_golden_cross_cond, \
           _closes[-1]


def is_falling_wedge(_klines):
    _closes = np.array(list(map(lambda _x: float(_x.closing), _klines)))
    _w = is_wedge(_closes)
    _r = relative_strength_index(_closes)
    try:
        _hl = is_higher_low(_r, 45.0, 33, -1)

    except ValueError:
        return False, _closes[-1]

    return _w and _hl, _closes[-1]


def get_markets(_exchange, _ticker=False, _exclude_markets=False):
    if _exclude_markets:
        if _exchange == "binance":
            _markets = binance_obj.get_all_btc_currencies(_exclude_markets[_ticker])
        elif _exchange == "kucoin":
            _markets = list(filter(lambda y: y not in _exclude_markets[_ticker],
                                   map(lambda x: x['currency'], kucoin_client.get_currencies())))
    else:
        if _exchange == "binance":
            _markets = binance_obj.get_all_btc_currencies()
        elif _exchange == "kucoin":
            _markets = list(map(lambda x: x['currency'], kucoin_client.get_currencies()))
    return _markets


def get_excluded_markets(_markets, _to_exclude, _ticker):
    _res = []
    if _ticker in _to_exclude:
        for _m in _markets:
            if not _m in _to_exclude[_ticker]:
                _res.append(_m)
        return _res
    return _markets


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
    if _macd_magnitude < 0:
        return False
    _macd_angle = get_angle((0, _macd[_start:_stop:1][-1]),
                            (_macd_reversed_max_ind / np.power(10, _macd_magnitude), _macd_max_val))
    return _macd_angle >= _angle_limit


def is_signal_divergence_ratio(_macd, _macdsignal, _ratio, _start, _stop):
    _diff = _macd - _macdsignal
    _diff_max_val, _diff_reversed_max_ind = find_first_maximum(_diff[_start:_stop:1], 10)
    _diff_max_val2, _diff_reversed_max_ind2 = find_first_maximum(-_diff[_start:_stop:1], 10)
    if _diff_max_val2 > _diff_max_val:
        _diff_max_val = _diff_max_val2
    return np.abs((_diff[_stop] + _diff[_stop - 1]) / 2) / _diff_max_val <= _ratio


def is_higher_low(_values, _limit, _start, _stop, _window=10):
    _flipped_values = np.max(_values[_start:_stop:1]) - _values
    _max_val, _reversed_max_ind = find_first_maximum(_flipped_values[_start:_stop:1], _window)
    _first_local_min = _values[_stop - _reversed_max_ind]
    if _reversed_max_ind == -1 or _first_local_min > _limit:  # 1. there is no local minimum 2. RSI value for minimum is too high
        return False
    _current_value = _values[_stop]
    if abs(_stop - _reversed_max_ind + 1 - (_stop - 1)) <= 2:  # len for the interval should be longer than 2
        return False
    _interval_for_local_max = _values[_stop - _reversed_max_ind + 1:_stop - 1]
    _local_max = np.max(_interval_for_local_max)
    _is_retraced = _local_max - (_local_max - _first_local_min) * 0.5 > _current_value
    return _is_retraced and (
            _local_max - _current_value) > 2  # the difference in RSI local extrema has to be minimum 2 rsi


def is_tradeable_strategy(_klines):
    _closes = np.array(list(map(lambda _x: float(_x.closing), _klines)))
    _macd, _macdsignal, _macdhist = talib.MACD(_closes, fastperiod=12, slowperiod=26, signalperiod=9)
    _rsi = relative_strength_index(_closes)

    return is_tradeable(_closes, _rsi, _macd, _macdsignal), _closes[-1]


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
    _divergence_ratio_cond = is_signal_divergence_ratio(_macd, _macdsignal, 0.5, _start, _stop)
    try:
        _hl_cond = is_higher_low(_rsi, _rsi_limit, _start, _stop)
    except ValueError:
        _hl_cond = False

    _tradeable = False
    if _rsi_normal_cond and _divergence_ratio_cond and _macd_normal_cond:
        _tradeable = True
    if _rsi_tight_cond or _macd_tight_cond:
        _tradeable = True
    if _hl_cond and _divergence_ratio_cond:
        _tradeable = True
    return _tradeable


def analyze_markets(_filename, _ticker, _time_interval, _exchange, _markets_obj):
    logger_global[0].info(_exchange)
    _golden_cross_markets = []
    _exclude_markets = {}
    if path.isfile(key_dir + _filename + ".pkl"):
        _exclude_markets = get_pickled(key_dir, _filename)
    else:
        _exclude_markets[_ticker] = []
    if _ticker in _exclude_markets:
        _markets_raw = get_markets(_exchange, _ticker, _exclude_markets)
    else:
        _markets_raw = get_markets(_exchange)

    _markets = get_filtered_markets(_exchange, _markets_obj, _markets_raw, _exclude_markets, _ticker)

    logger_global[0].info(
        f"We are analyzing {len(_markets)} markets on {_exchange} ( with at least btc volume: {_markets_obj.binance_btc_vol} <- binance, {_markets_obj.kucoin_btc_vol} <- kucoin )")

    for _market in _markets:
        try:
            # if _exchange == 'kucoin':
            #     _klines = get_kucoin_klines(f"{_market}-BTC", _ticker, _time_interval)
            # elif _exchange == "binance":
            #     _klines = get_binance_klines(_market, _ticker, _time_interval)

            if _exchange == 'kucoin':
                _klines = try_get_klines("kucoin", f"{_market}-BTC", _ticker, _time_interval)
            elif _exchange == "binance":
                _klines = try_get_klines("binance", _market, _ticker, _time_interval)

            _is_2nd_golden = compute_wider_interval(is_second_golden_cross, _klines)
            if _is_2nd_golden[0] > 0:
                _golden_cross_markets.append((_market, "is_second_golden_cross", _is_2nd_golden[1], _is_2nd_golden[0]))
            _is_1st_golden = compute_wider_interval(is_first_golden_cross, _klines)
            if _is_1st_golden[0] > 0:
                _golden_cross_markets.append((_market, "is_first_golden_cross", _is_1st_golden[1], _is_1st_golden[0]))
            _is_drop_below_ma200 = compute_wider_interval(is_drop_below_ma200_after_rally, _klines)
            if _is_drop_below_ma200[0] > 0:
                _golden_cross_markets.append(
                    (_market, "drop_below_ma200_after_rally", _is_drop_below_ma200[1], _is_drop_below_ma200[0]))
            _is_drop_below_ma50 = compute_wider_interval(is_drop_below_ma50_after_rally, _klines)
            if _is_drop_below_ma50[0] > 0:
                _golden_cross_markets.append(
                    (_market, "is_drop_below_ma50_after_rally", _is_drop_below_ma50[1], _is_drop_below_ma50[0]))
            _is_fw = compute_wider_interval(is_falling_wedge, _klines)
            if _is_fw[0] > 0:
                _golden_cross_markets.append((_market, "is_falling_wedge", _is_fw[1], _is_fw[0]))
            _is_bf = compute_wider_interval(is_bull_flag, _klines)
            if _is_bf[0] > 0:
                _golden_cross_markets.append((_market, "is_bull_flag", _is_bf[1], _is_bf[0]))
            _is_tradeable_strategy = compute_wider_interval(is_tradeable_strategy, _klines)
            if _is_tradeable_strategy[0] > 0:
                _golden_cross_markets.append(
                    (_market, "is_tradeable_strategy", _is_tradeable_strategy[1], _is_tradeable_strategy[0]))
            _is_tilting = compute_wider_interval(is_tilting, _klines)
            if _is_tilting[0] > 0:
                _golden_cross_markets.append((_market, "is_tilting", _is_tilting[1], _is_tilting[0]))

        except Exception as e:

            if e.args[0] == 'Integers to negative integer powers are not allowed.' or e.args[
                0] == 'invalid value encountered in greater' or e.args[
                0] == 'zero-size array to reduction operation maximum which has no identity' \
                    or e.args[
                0] == 'invalid value encountered in greater' or e.args[
                0] == 'not enough values to unpack (expected 3, got 2)':

                _is_2nd_golden = compute_wider_interval(is_second_golden_cross, _klines)
                if _is_2nd_golden[0] > 0:
                    _golden_cross_markets.append(
                        (_market, "is_second_golden_cross", _is_2nd_golden[1], _is_2nd_golden[0]))
                _is_1st_golden = compute_wider_interval(is_first_golden_cross, _klines)
                if _is_1st_golden[0] > 0:
                    _golden_cross_markets.append(
                        (_market, "is_first_golden_cross", _is_1st_golden[1], _is_1st_golden[0]))
                _is_drop_below_ma200 = compute_wider_interval(is_drop_below_ma200_after_rally, _klines)
                if _is_drop_below_ma200[0] > 0:
                    _golden_cross_markets.append(
                        (_market, "drop_below_ma200_after_rally", _is_drop_below_ma200[1], _is_drop_below_ma200[0]))
                _is_drop_below_ma50 = compute_wider_interval(is_drop_below_ma50_after_rally, _klines)
                if _is_drop_below_ma50[0] > 0:
                    _golden_cross_markets.append(
                        (_market, "is_drop_below_ma50_after_rally", _is_drop_below_ma50[1], _is_drop_below_ma50[0]))
                _is_fw = compute_wider_interval(is_falling_wedge, _klines)
                if _is_fw[0] > 0:
                    _golden_cross_markets.append((_market, "is_falling_wedge", _is_fw[1], _is_fw[0]))
                _is_bf = compute_wider_interval(is_bull_flag, _klines)
                if _is_bf[0] > 0:
                    _golden_cross_markets.append((_market, "is_bull_flag", _is_bf[1], _is_bf[0]))
                _is_tradeable_strategy = compute_wider_interval(is_tradeable_strategy, _klines)
                if _is_tradeable_strategy[0] > 0:
                    _golden_cross_markets.append(
                        (_market, "is_tradeable_strategy", _is_tradeable_strategy[1], _is_tradeable_strategy[0]))
                _is_tilting = compute_wider_interval(is_tilting, _klines)
                if _is_tilting[0] > 0:
                    _golden_cross_markets.append((_market, "is_tilting", _is_tilting[1], _is_tilting[0]))

            logger_global[0].warning(e)
            logger_global[0].warning(f"No data for market {_ticker} : {_market}")
            if _ticker in _exclude_markets:
                _exclude_markets[_ticker].append(_market)
            else:
                _exclude_markets[_ticker] = [_market]
    _aggregated = {}
    for _setup in _golden_cross_markets:
        if _setup[0] not in _aggregated:
            _aggregated[_setup[0]] = [_setup]
        else:
            _aggregated[_setup[0]].append(_setup)

    _markets_sorted = list(sorted(_aggregated, key=lambda k: len(_aggregated[k]), reverse=True))
    _golden_cross_markets_out = []
    for _market in _markets_sorted:
        for _setup in _aggregated[_market]:
            _golden_cross_markets_out.append(_setup)

    _info = ' '.join(format_found_markets(_golden_cross_markets_out))
    logger_global[0].info(f"{_ticker} : {_info}")
    save_to_file(key_dir, _filename, _exclude_markets)
    return _golden_cross_markets_out


def compute_wider_interval(_func, _klines):
    _res = []
    for i in range(0, 48):
        if i == 0:
            _res.append(_func(_klines))
        else:
            _res.append(_func(_klines[:-i]))
    _trues = sum(i for i, _ in _res)
    return _trues, _res[0][1]


def filter_markets_by_volume(_markets, _vol_btc, _exchange, _exclude_markets, _ticker):
    _binance_ticker = "1d"
    _kucoin_ticker = "1day"
    _markets_out = []
    for _market in _markets:
        try:
            if _exchange == 'kucoin':
                _klines = try_get_klines("kucoin", f"{_market}-BTC", _kucoin_ticker,
                                         get_kucoin_interval_unit(_kucoin_ticker, 240))
            elif _exchange == "binance":
                _klines = try_get_klines("binance", _market, _binance_ticker, "10 days ago")
            _mean_vol = np.mean(list(map(lambda x: x.btc_volume, _klines)))
            if _mean_vol > _vol_btc:
                _markets_out.append(_market)
        except RuntimeError as e:
            logger_global[0].warning(e)
            logger_global[0].warning(f"No data for market : {_market}")
            if _ticker in _exclude_markets:
                _exclude_markets[_ticker].append(_market)
            else:
                _exclude_markets[_ticker] = [_market]

    return _markets_out


class Markets(object):
    def __init__(self, _binance_vol, _kucoin_vol):
        self.timestamp = datetime.datetime.now().timestamp()
        self.binance_btc_vol = _binance_vol
        self.kucoin_btc_vol = _kucoin_vol
        self.binance_markets = []
        self.kucoin_markets = []

    def set_binance_markets(self, _m):
        self.binance_markets = _m

    def set_kucoin_markets(self, _m):
        self.kucoin_markets = _m


def analyze_micro_markets(_filename, _ticker, _time_interval, _exchange, _markets_obj):
    logger_global[0].info(_exchange)
    _golden_cross_markets = []
    _exclude_markets = {}
    if path.isfile(key_dir + _filename + ".pkl"):
        _exclude_markets = get_pickled(key_dir, _filename)
    else:
        _exclude_markets[_ticker] = []
    if _ticker in _exclude_markets:
        _markets_raw = get_markets(_exchange, _ticker, _exclude_markets)
    else:
        _markets_raw = get_markets(_exchange)

    _markets = get_filtered_markets(_exchange, _markets_obj, _markets_raw, _exclude_markets, _ticker)
    logger_global[0].info(
        f"We are analyzing {len(_markets)} markets on {_exchange} ( with at least btc voulume: {_markets_obj.binance_btc_vol} <- binance, {_markets_obj.kucoin_btc_vol} <- kucoin )")

    for _market in _markets:
        try:
            # if _exchange == 'kucoin':
            #     _klines = get_kucoin_klines(f"{_market}-BTC", _ticker, _time_interval)
            # elif _exchange == "binance":
            #     _klines = get_binance_klines(_market, _ticker, _time_interval)

            if _exchange == 'kucoin':
                _klines = try_get_klines("kucoin", f"{_market}-BTC", _ticker, _time_interval)
            elif _exchange == "binance":
                _klines = try_get_klines("binance", _market, _ticker, _time_interval)

            _closes = np.array(list(map(lambda _x: float(_x.closing), _klines)))

            _is_bull_cross_in_bull_mode = is_tradeable_strategy(_closes)
            if _is_bull_cross_in_bull_mode[0]:
                _golden_cross_markets.append((_market, "_is_bull_cross_in_bull_mode", _is_bull_cross_in_bull_mode[1]))

        except Exception as e:

            if e.args[0] == 'invalid value encountered in int_scalars':

                _is_bull_cross_in_bull_mode = is_bull_cross_in_bull_mode(_closes)
                if _is_bull_cross_in_bull_mode[0]:
                    _golden_cross_markets.append(
                        (_market, "_is_bull_cross_in_bull_mode", _is_bull_cross_in_bull_mode[1]))

            logger_global[0].warning(e)
            logger_global[0].warning(f"No data for market : {_market}")
            if _ticker in _exclude_markets:
                _exclude_markets[_ticker].append(_market)
            else:
                _exclude_markets[_ticker] = [_market]
    logger_global[0].info(' '.join(format_found_markets(_golden_cross_markets)))
    save_to_file(key_dir, _filename, _exclude_markets)
    return _golden_cross_markets


def get_filtered_markets(_exchange, _markets_obj, _markets_raw, _exclude_markets, _ticker):
    if _exchange == "binance" and not _markets_obj.binance_markets:
        _markets = filter_markets_by_volume(_markets_raw, _markets_obj.binance_btc_vol, _exchange, _exclude_markets,
                                            _ticker)
        _markets_obj.set_binance_markets(_markets)
    elif _exchange == "kucoin" and not _markets_obj.kucoin_markets:
        _markets = filter_markets_by_volume(_markets_raw, _markets_obj.kucoin_btc_vol, _exchange, _exclude_markets,
                                            _ticker)
        _markets_obj.set_kucoin_markets(_markets)
    if _exchange == "binance" and _markets_obj.binance_markets:
        _markets = _markets_obj.binance_markets
    if _exchange == "kucoin" and _markets_obj.kucoin_markets:
        _markets = _markets_obj.kucoin_markets
    return _markets


def try_get_klines(_exchange, _market, _ticker, _time_interval):
    _ii = 0
    _klines = []
    for _ii in range(0, 5):
        try:
            if _exchange == 'kucoin':
                _klines = get_kucoin_klines(_market, _ticker, _time_interval)
            elif _exchange == "binance":
                _klines = get_binance_klines(_market, _ticker, _time_interval)
            if not _klines:
                logger_global[0].warning(f"Trying for market : {_market} {_ticker} ... {_ii}")
                time.sleep(randrange(180))
        except RuntimeError as re:
            logger_global[0].warning(re)
            logger_global[0].warning(f"Sleeping for 15 min : {_market} {_ticker} ... {_ii}")
            time.sleep(randrange(180))
        except ReadTimeoutError as e:
            logger_global[0].warning(e)
            time.sleep(randrange(180))
        except Exception as e:
            logger_global[0].warning(e)
            logger_global[0].warning(f"Exception : {_market} {_ticker} ... {_ii}")
            time.sleep(randrange(180))
        if _klines:
            return _klines
    raise RuntimeError(f"No klines for market {_market}")


class MailContent(object):
    def __init__(self, _mail_content):
        self.content = _mail_content


def format_found_markets(_markets_tuple):
    return [f"{x[0]} : {x[1]}({x[3]})" for x in _markets_tuple]


def format_found_tuples(_tuples, _mark=""):
    return [f"{x[1]} : {get_format_price(x[0])} {_mark}" for x in _tuples]


def process_setups(_setup_tuples, _collection, _ticker, _mail_content):
    _mail_content.content += f"<BR/><hr/><BR/><B>{_ticker}</B><BR/>"
    process_setup_tuples(_setup_tuples[0], _collection, _ticker)
    process_setup_tuples(_setup_tuples[1], _collection, _ticker)

    for _setup_tuple in _setup_tuples:
        _setup = _setup_tuple[0]
        # _setup = sorted(_setup, key=lambda x: x[3], reverse=True)
        _exchange = _setup_tuple[1]
        if len(_setup) > 0:
            _mail_content.content += f"<BR/><B>{_exchange}</B><BR/>"
            _mail_content.content += ' '.join(format_found_markets(_setup))


def process_entries(_entries_tuples, _exchange, _collection, _ticker, _mail_content):
    if len(_entries_tuples)>0:
        _mail_content.content += f"<BR/><hr/><BR/><B>{_ticker}</B><BR/>"
        process_entry_tuples(_entries_tuples, _exchange, _collection, _ticker)

        if len(_entries_tuples) > 0:
            _mail_content.content += f"<BR/><B>{_exchange}</B><BR/>"
            _mail_content.content += ' '.join(format_found_tuples(_entries_tuples, "<BR/>"))


def process_valuable_alts(_valuable_tuples, _exchange, _ticker, _mail_content):
    _mail_content.content += f"<BR/><hr/><BR/><B>{_ticker}</B><BR/>"
    _mail_content.content += f"<BR/><B>{_exchange}</B><BR/>"
    for _valuable_tuple in _valuable_tuples:
        _market = _valuable_tuple[0]
        _val = _valuable_tuple[1]
        _mail_content.content += f"{_market} :{_val}<BR/>"


def process_setup_tuples(_setup_tuples, _collection, _ticker):
    _assets = _setup_tuples[0]
    _exchange = _setup_tuples[1]
    for _st in _assets:
        persist_setup((_st, _exchange), _collection, _ticker)


def setup_to_mongo(_setup_tuple, _ticker):
    _setup = _setup_tuple[0]
    _exchange = _setup_tuple[1]
    _timestamp = datetime.datetime.now().timestamp()
    return {
        'start_time': _timestamp,
        'start_time_str': get_time(_timestamp),
        'close_price': _setup[2],
        'type': _setup[1],
        'market': _setup[0],
        'exchange': _exchange,
        'ticker': _ticker,
        'times': [_timestamp],
        'types': [_setup[1]],
        'trues': [int(_setup[3])]
    }


def process_entry_tuples(_entry_tuples, _exchange, _collection, _ticker):
    for _bid_tuple in _entry_tuples:
        persist_entry(_bid_tuple, _exchange, _collection, _ticker)


def entry_to_mongo(_bid_tuple, _exchange, _ticker):
    _bid_price = _bid_tuple[0]
    _market = _bid_tuple[1]
    _timestamp = datetime.datetime.now().timestamp()
    return {
        'start_time': _timestamp,
        'start_time_str': get_time(_timestamp),
        'bid_price': _bid_price,
        'market': _market,
        'exchange': _exchange,
        'ticker': _ticker,
    }


def persist_setup(_setup_tuple, _collection, _ticker):
    try:
        _setup = _setup_tuple[0]
        _exchange = _setup_tuple[1]
        _found = _collection.find_one(
            filter={'setup.market': _setup[0], 'setup.exchange': _exchange, 'setup.ticker': _ticker},
            sort=[('_id', DESCENDING)])

        if _found:
            _last_update = _found['setup']['times'][-1]
            _now = datetime.datetime.now().timestamp()
            _diff_in_days = (_now - _last_update) / 60 / 60 / 24
            if _diff_in_days < 5.0:
                _found['setup']['times'].append(_now)
                _found['setup']['types'].append(_setup[1])
                _found['setup']['trues'].append(int(_setup[3]))
                _collection.update_one({'_id': _found['_id']}, {'$set': {'setup': _found['setup']}})
            else:
                _collection.insert_one({'setup': setup_to_mongo(_setup_tuple, _ticker)})
        else:
            try:
                _collection.insert_one({'setup': setup_to_mongo(_setup_tuple, _ticker)})
            except InvalidDocument:
                _collection.insert_one({'setup': setup_to_mongo(_setup_tuple, _ticker)})
    except PyMongoError:
        time.sleep(5)
        persist_setup(_setup_tuple, _collection)


def persist_entry(_bid_tuple, _exchange, _collection, _ticker):
    _bid_price = _bid_tuple[0]
    _bid_market = _bid_tuple[1]
    try:
        _found = _collection.find_one(
            filter={'bid.market': _bid_market, 'bid.exchange': _exchange, 'setup.ticker': _ticker},
            sort=[('_id', DESCENDING)])

        if _found:
            _last_update = _found['bid']['timestamp']
            _now = datetime.datetime.now().timestamp()
            _diff_in_days = (_now - _last_update) / 60 / 60 / 24
            if _diff_in_days > 2.0:
                _collection.insert_one({'entry': entry_to_mongo(_bid_tuple, _exchange, _ticker)})
        else:
            try:
                _collection.insert_one({'entry': entry_to_mongo(_bid_tuple, _exchange, _ticker)})
            except InvalidDocument:
                _collection.insert_one({'entry': entry_to_mongo(_bid_tuple, _exchange, _ticker)})
    except PyMongoError:
        time.sleep(5)
        persist_entry(_bid_tuple, _exchange, _collection, _ticker)


def to_mongo_dict(_kline):
    return {
        'start_time': _kline.start_time,
        'opening': _kline.opening,
        'closing': _kline.closing,
        'lowest': _kline.lowest,
        'highest': _kline.highest,
        'volume': _kline.volume,
        'btc_volume': _kline.btc_volume,
        'time_str': _kline.time_str,
    }


def manage_verifying_setup(_collection):
    _scheduler = threading.Thread(target=_verify_setup, args=(_collection,),
                                  name='_verify_setup')
    _scheduler.start()


def _verify_setup(_collection):
    while True:
        _now = datetime.datetime.now().timestamp()
        _verified = []
        for _object in _collection.find({'verified': {'$exists': False}}):
            _last_update = _object['setup']['times'][-1]
            _diff_in_days = (_now - _last_update) / 60 / 60 / 24
            if _diff_in_days > 6.0:
                _close_price = _object['setup']['close_price']
                _start_time = _object['setup']['start_time']
                _market = _object['setup']['market']
                _exchange = _object['setup']['exchange']
                _hours_gap = int((_now - _start_time) / 60 / 60)
                if _exchange == "binance":
                    _klines = get_binance_klines(_market, BinanceClient.KLINE_INTERVAL_1HOUR,
                                                 "{} hours ago".format(_hours_gap))
                elif _exchange == "kucoin":
                    _klines = get_kucoin_klines("{}-BTC".format(_market), "1hour", round(_start_time))

                _closes = np.array(list(map(lambda _x: float(_x.closing), _klines)))
                if len(_closes) > 0:
                    _min = np.min(_closes)
                    _max = np.max(_closes)
                    _mean = round(np.mean(_closes), 10)
                    _median = np.median(_closes)
                    _max_up = round((_max - _close_price) / _close_price * 100, 4)
                    _max_down = round((_close_price - _min) / _close_price * 100, 4)

                    _max_kline = max(_klines, key=attrgetter('closing'))
                    _min_kline = min(_klines, key=attrgetter('closing'))

                    _object['verified'] = {
                        'max_up': _max_up,
                        'max_down': _max_down,
                        'min': _min,
                        'max': _max,
                        'mean': _mean,
                        'median': _median,
                        'time_str': get_time(_now),
                        'max_kline': to_mongo_dict(_max_kline),
                        'min_kline': to_mongo_dict(_min_kline)
                    }
                    _verified.append({
                        'exchange': _exchange,
                        'strategy': _object['setup']['type'],
                        'market': _market,
                        'max_up': _max_up,
                        'max_down': _max_down,
                        'ticker': _object['setup']['ticker'],
                        'close_price': _object['setup']['close_price'],
                        'start_time': _object['setup']['start_time_str']
                    })
                    _collection.update_one({'_id': _object['_id']}, {'$set': {'verified': _object['verified']}})
                else:
                    _collection.update_one({'_id': _object['_id']}, {'$set': {'verified': 'NA'}})
        _mail_content = ''
        if len(_verified) > 0:
            _binance = list(filter(lambda elem: elem['exchange'] == "binance", _verified))
            _kucoin = list(filter(lambda elem: elem['exchange'] == "kucoin", _verified))

            if len(_binance) > 0:
                _mail_content = add_mail_content_for_exchange(_mail_content, _binance, "binance")
            if len(_kucoin) > 0:
                _mail_content = add_mail_content_for_exchange(_mail_content, _kucoin, "kucoin")

        if len(_mail_content) > 0:
            send_mail("YYY Verified YYY", _mail_content)
    time.sleep(60 * 60 * 24)  # once a day


def add_mail_content_for_exchange(_mail_content, _data_list, _exchange):
    _mail_content += f"<BR/><B>{_exchange}</B><BR/>"
    _is_first = list(filter(lambda elem: elem['strategy'] == "is_first_golden_cross", _data_list))
    _is_second = list(filter(lambda elem: elem['strategy'] == "is_second_golden_cross", _data_list))
    _drop_below_ma50 = list(filter(lambda elem: elem['strategy'] == "drop_below_ma50_after_rally", _data_list))
    _drop_below_ma200 = list(filter(lambda elem: elem['strategy'] == "drop_below_ma200_after_rally", _data_list))
    _is_fw = list(filter(lambda elem: elem['strategy'] == "is_falling_wedge", _data_list))
    _is_bf = list(filter(lambda elem: elem['strategy'] == "is_bull_flag", _data_list))
    _is_tilting = list(filter(lambda elem: elem['strategy'] == "is_tilting", _data_list))
    _is_tradeable_strategy = list(filter(lambda elem: elem['strategy'] == "is_tradeable_strategy", _data_list))
    _mail_content = handle_verification_mailing(_is_first, _mail_content)
    _mail_content = handle_verification_mailing(_is_second, _mail_content)
    _mail_content = handle_verification_mailing(_drop_below_ma50, _mail_content)
    _mail_content = handle_verification_mailing(_drop_below_ma200, _mail_content)
    _mail_content = handle_verification_mailing(_is_fw, _mail_content)
    _mail_content = handle_verification_mailing(_is_bf, _mail_content)
    _mail_content = handle_verification_mailing(_is_tilting, _mail_content)
    _mail_content = handle_verification_mailing(_is_tradeable_strategy, _mail_content)
    return _mail_content


def handle_verification_mailing(_data_list, _mail_content):
    if _data_list:
        if _data_list[0]['strategy']:
            _mail_content += f"<BR/><B>{_data_list[0]['strategy']}</B><BR/>"
        for _v in _data_list:
            _mail_content += f"<BR/>{_v['market']} -- ticker : {_v['ticker']} : max up : {_v['max_up']} max down : {_v['max_down']} close price : {_v['close_price']} start_time : {_v['start_time']} <BR/>"
        return _mail_content


def is_wedge(_closes):
    _max_val, _index_max_val = find_first_maximum(_closes, 5)
    _max_val0, _index_max_val0 = find_first_maximum(_closes[:-_index_max_val], 5)
    _max_val2, _index_max_val2 = find_first_maximum(_closes[-_index_max_val:], 3)
    _min_va11, _index_min_val1 = find_first_minimum(_closes[-_index_max_val:], 3)
    _min_val, _index_min_val2 = find_first_minimum(_closes[-_index_max_val:-_index_max_val2][::-1], 3)
    _index_min_val = _index_max_val2 + _index_min_val2 + 1
    _magnitude = get_magnitude(_index_max_val, _max_val)

    if _index_max_val == _index_max_val2:
        return False

    if _magnitude < 0:
        return False

    _slope_max = slope(-_index_max_val, _max_val * np.power(10, _magnitude), -_index_max_val2,
                       _max_val2 * np.power(10, _magnitude))

    if _index_min_val == _index_min_val1:
        return False

    _slope_min = slope(-_index_min_val, _min_val * np.power(10, _magnitude), -_index_min_val1,
                       _min_va11 * np.power(10, _magnitude))

    _b_max = bias(-_index_max_val, _max_val * np.power(10, _magnitude), -_index_max_val2,
                  _max_val2 * np.power(10, _magnitude))
    _b_min = bias(-_index_min_val, _min_val * np.power(10, _magnitude), -_index_min_val1,
                  _min_va11 * np.power(10, _magnitude))

    _checked_max = check_wedge(_slope_max, _b_max, range(-_index_max_val, 0),
                               _closes[-_index_max_val:] * np.power(10, _magnitude))
    _checked_min = check_wedge(_slope_min, _b_min, range(-_index_max_val, 0),
                               _closes[-_index_max_val:] * np.power(10, _magnitude), True)

    _max0_cond = _max_val0 * np.power(10, _magnitude) <= -_slope_max * _index_max_val0 + _b_max

    return _max0_cond and _checked_min and _checked_max


def check_wedge(_a, _b, _x, _y, _greater=False, _ratio=0.66):
    _f = _a * _x + _b
    if _greater:
        _r = _f <= _y
    else:
        _r = _f >= _y
    return np.sum(_r) / len(_r) > _ratio


def is_bull_flag(_klines):
    _closes = np.array(list(map(lambda _x: float(_x.closing), _klines)))
    _true = True
    _rsi = relative_strength_index(_closes)
    _r_max_val_max, _r_max_ind = find_first_maximum(_rsi, 10)
    _r_min_val_max, _r_min_ind0 = find_first_minimum(_rsi[:-_r_max_ind], 10)
    _r_min_ind = _r_max_ind + _r_min_ind0
    _rsi_mean = np.mean(_rsi[-_r_min_ind:])
    _is_bullish = _rsi_mean > 58.0

    _c_last_max_val, _c_last_max_ind0 = find_first_maximum(_closes[-_r_max_ind + 1:][::-1], 2)
    _c_last_max_ind = len(_closes[-_r_max_ind:]) - _c_last_max_ind0
    _not_bullish_cond = _r_max_val_max > 75 and _c_last_max_val > _closes[-_r_max_ind] and _rsi[-_c_last_max_ind] < \
                        _rsi[-_r_max_ind]

    _rev_min_val, _rev_min_ind0 = find_first_minimum(_closes[-_r_max_ind:][::-1], 10)
    _rev_min_ind = len(_closes[-_r_max_ind:]) - _rev_min_ind0
    _rev_max_val, _rev_max_ind0 = find_first_maximum(_closes[-_rev_min_ind:][::-1], 10)
    if _rev_min_ind0 == -1 or _rev_max_ind0 == -1:
        _true = False
    _rev_max_ind = _rev_min_ind - _rev_max_ind0 + 1
    _min_after_max_rev = np.mean(_closes[-_rev_max_ind:])
    _is_min_existing = _rev_min_val < _min_after_max_rev
    _rsi_last_avg = np.mean(_rsi[-10:])
    _ma50 = talib.MA(_closes, timeperiod=50)
    _c_m = np.mean(_closes[-10:])
    _r_m = np.mean(_ma50[-10:])
    _closes_above_ma50 = _c_m > _r_m

    return _true and not _not_bullish_cond and _is_bullish and _is_min_existing and _rsi_last_avg > 48.0 and _closes_above_ma50, \
           _closes[-1]


def bear_cross(_closes):
    _ma200 = talib.MA(_closes, timeperiod=200)
    _ma50 = talib.MA(_closes, timeperiod=50)

    _ma200_rev = _ma200[::-1]
    _ma50_rev = _ma50[::-1]
    if _ma50_rev[0] > _ma200_rev[0]:
        return -1, -1
    for _i in range(len(_closes)):
        if _ma50_rev[_i] > _ma200_rev[_i]:
            return _ma50_rev[_i], _i
    return -1, -1


def bull_cross(_closes):
    _ma200 = talib.MA(_closes, timeperiod=200)
    _ma50 = talib.MA(_closes, timeperiod=50)

    _ma200_rev = _ma200[::-1]
    _ma50_rev = _ma50[::-1]
    if _ma50_rev[0] < _ma200_rev[0]:
        return -1, -1
    for _i in range(len(_closes)):
        if _ma50_rev[_i] < _ma200_rev[_i]:
            return _ma50_rev[_i], _i
    return -1, -1


def is_bull_cross_in_bull_mode(_closes):
    _ma200 = talib.MA(_closes, timeperiod=200)

    _minv, _mini = find_minimum_2(_ma200, 10)
    _maxv, _maxi = find_maximum_2(_ma200[-_mini:], 10)

    _cond1 = True
    if _ma200[-1] < _maxv:
        _cond1 = (_maxv - _minv) / _minv > 0.05

    _cond2 = (_ma200[-1] - _minv) / _minv > 0.05 and _mini > 500

    _bc_val, _bc_ind = bull_cross(_closes)
    if _bc_ind == -1:
        return False, _closes[-1]
    _cond3 = _bc_ind < 10

    _fmax_v, _fmax_i = find_first_maximum(_ma200, 10)
    _fminv, _fmin_i0 = find_first_minimum(_ma200[:-_fmax_i], 10)
    _fmin_i = _fmax_i + _fmin_i0 - 1

    _fmax_v0, _fmax_i0_ = find_first_maximum(_ma200[:-_fmin_i], 10)
    _fmax_i0 = _fmax_i0_ + _fmin_i - 1

    _cond4_bear = not (_fmax_v - _fminv) / _fminv > 0.05 and _fmax_v - _fmax_v0 < 0

    return _cond1 and _cond2 and _cond3 and _cond4_bear, _closes[-1]


def index_of_max_mas_difference(_closes):
    _ma200 = talib.MA(_closes, timeperiod=200)
    _ma50 = talib.MA(_closes, timeperiod=50)
    _bv, _bi = bear_cross(_closes)
    if _bi == -1:
        return -1, -1, -1
    _ma200_in = _ma200[-_bi:]
    _ma50_in = _ma50[-_bi:]
    _last_diff = -1
    _ind_o = _rel_ind_o = _diff_o = _i_o = -1
    _diff_increasing = -1
    for _i in range(len(_ma200_in) - 1):
        _diff = abs(_ma200_in[_i] - _ma50_in[_i])
        if _diff < _last_diff:
            _ind = _bi - _i - 1
            _rel_ind = (len(_ma200_in) - _ind - 1) / _ind
            _ind_o = _ind
            _rel_ind_o = _rel_ind
            _i_o = _i
            _diff_o = _diff
        if _diff > _diff_o > 0 and _i > _i_o > 0:
            _diff_increasing = _diff
        _last_diff = _diff

    _r = (_diff_increasing - _diff_o) / _diff_increasing

    if _r > 0.2 and _ind_o > 0:
        return _ind_o, _rel_ind_o, _diff_o
    else:
        return -1, -1, -1


def is_tilting(_klines):
    _closes = np.array(list(map(lambda _x: float(_x.closing), _klines)))
    _ind, _rel_ind, _diff = index_of_max_mas_difference(_closes)
    _ma200 = talib.MA(_closes, timeperiod=200)
    _ma50 = talib.MA(_closes, timeperiod=50)
    _frac = (_closes[-1] - _ma50[-1]) / _diff
    _res = False
    if _rel_ind > 0.5 and (_closes[-1] < _ma50[-1] or _frac < 0.25):
        _res = True
    if _rel_ind > 0.8:
        _res = True
    if _rel_ind > 0.5 and _closes[-1] > _ma200[-1]:
        _res = True
    return _res, _closes[-1]


def find_valuable_alts(_highs):
    _min_val = np.min(_highs)
    _max = np.max(_highs)
    _ratio = round((_max - _min_val) / _min_val, 2)
    return _ratio > 3, _ratio


def analyze_valuable_alts(_filename, _exchange, _ticker, _time_interval, _markets_obj):
    _markets_raw = get_markets(_exchange)
    _exclude_markets = {}
    _valuable_markets = []
    if path.isfile(key_dir + _filename + ".pkl"):
        _exclude_markets = get_pickled(key_dir, _filename)
    else:
        _exclude_markets[_ticker] = []
    if _ticker in _exclude_markets:
        _markets_raw = get_markets(_exchange, _ticker, _exclude_markets)
    else:
        _markets_raw = get_markets(_exchange)

    _markets = get_filtered_markets(_exchange, _markets_obj, _markets_raw, _exclude_markets, _ticker)
    logger_global[0].info(
        f"We are analyzing {len(_markets)} markets on {_exchange} ( with at least btc volume: {_markets_obj.binance_btc_vol} <- binance, {_markets_obj.kucoin_btc_vol} <- kucoin )")
    for _market in _markets:
        try:
            if _exchange == 'kucoin':
                _klines = try_get_klines("kucoin", f"{_market}-BTC", _ticker, _time_interval)
            elif _exchange == "binance":
                _klines = try_get_klines("binance", _market, _ticker, _time_interval)

            _highs = np.array(list(map(lambda _x: float(_x.highest), _klines)))
            _is, _val = find_valuable_alts(_highs)
            if _is:
                _valuable_markets.append((_market, _val))
        except Exception as e:
            logger_global[0].warning(e)
            logger_global[0].warning(f"No data for market {_ticker} : {_market}")

    save_to_file(key_dir, _filename, _exclude_markets)
    return _valuable_markets


def adjust_purchase_fund(_assets):
    _assets_size = len(_assets)
    list(map(lambda _asset: _asset.set_ratio(_asset.ratio / _assets_size), _assets))
    list(map(lambda _asset: cancel_kucoin_current_orders(_asset.market), _assets))
    _remaining_btc = get_remaining_btc_kucoin()
    list(map(lambda _asset: _asset.set_purchase_fund(_remaining_btc), _assets))


def watch_orders(_assets):
    while 1:
        for _buy_asset in _assets:
            time.sleep(5)
            _filled = kucoin_client.get_fills(_buy_asset.order_id)
            if len(_filled['items']) > 0:
                _filled_market = _filled['items'][0]['symbol']
                list(filter(lambda x: x.market != _filled_market, _assets))
                list(map(lambda z: cancel_kucoin_current_orders(z),
                         map(lambda y: y.market, filter(lambda x: x.market != _filled_market, _assets))))
                logger_global[0].info(f"Creating one final order for {_filled_market}")
                _buy_asset.hidden_buy_order(compute_purchase_fund=True)
                while 1:
                    time.sleep(30)
                    _filled_buy_asset = is_order_filled(_buy_asset)
                    if _filled_buy_asset:
                        return _filled_buy_asset
        time.sleep(60)


def is_order_filled(_buy_asset):
    _filled = kucoin_client.get_fills(_buy_asset.order_id)
    _filled_fund = sum([float(_item['funds']) for _item in _filled['items']])
    _order_filled = _buy_asset.purchase_fund - _filled_fund < 1e-5  # we are ok with a little difference, it is negligible
    if _order_filled:
        _df = round(_buy_asset.purchase_fund - _filled_fund + delta, 10)
        logger_global[0].info(
            f"{_buy_asset.market} : the order : {_buy_asset.order_id} has been filled, df : {get_format_price(_df)}")
        return _buy_asset
    return False


def wait_until_order_is_filled(_asset):
    while is_order_filled(_asset):
        time.sleep(180)
    return True


def set_kucoin_buy_orders(_assets):
    for _buy_asset in _assets:
        _buy_asset.hidden_buy_order()


def check_price_slope(_asset, _klines):
    return break_line(_asset, _klines)


def get_line(_price_open1, _price_open2, _dt):
    _b = _price_open1
    _a = (_price_open2 - _price_open1) / _dt
    return _a, _b


def break_line(_asset, _klines):
    _line = _asset.line
    if not _line:
        return False
    _a, _b = get_line(_line.p1, _line.p2, _line.dt)
    _res = False
    _closing_price = get_last_closing_price2(_klines)
    if _line.market_type == "down":
        _res = True if 0 < _a * (_line.dt + 1) + _b - _closing_price else False
    else:
        _res = True if 0 > _a * (_line.dt + 1) + _b - _closing_price else False

    return _res


class Line(object):
    def __init__(self, _p1, _p2, _dt, _type) -> None:
        self.p1 = _p1
        self.p2 = _p2
        self.dt = _dt
        self.type = _type


class MovingAverage(object):
    def __init__(self, period, direction):
        self.period = period
        self.direction = direction

    def __repr__(self):
        return f"MA{self.period} {self.direction}"


def check_mas(_asset, _klines):
    if not _asset.mas:
        return False
    _closes = get_closes2(_klines)
    _last_close = get_last_closing_price2(_klines)
    _response = []
    for _ma in _asset.mas:
        _last_ma = talib.MA(_closes, timeperiod=_ma.period)[-2]
        if _ma.direction == "up" and _last_close > _last_ma:
            _response.append(_ma)
        if _ma.direction == "down" and _last_close < _last_ma:
            _response.append(_ma)
    return _response if len(_response)>0 else False


def read_pattern_matcher_csv():
    _csv_file = config.get_parameter('pattern_macther_csv')
    return file_reader(_csv_file)
        

def read_collections_file():
    _file = config.get_parameter('collections_file')
    return file_reader(_file)


def file_reader(_file):
    try:
        with open(_file, newline='') as f:
            reader = csv.reader(f)
            return skip_commented_lines(list(reader))
    except Exception as err:
        traceback.print_tb(err.__traceback__)
        logger_global[0].exception(err.__traceback__)


def skip_commented_lines(_lines):
    _res = []
    for _line in _lines:
        if _line and not _line[0][0] == "#":
            _res.append(_line)
    return _res


def create_observe_assets():
    _patterns = read_pattern_matcher_csv()
    _assets = []
    for _pattern in _patterns:
        _assets.append(ObserveAsset("binance", _pattern[0], float(_pattern[1]) * sat, line=extract_line(_pattern),
                     mas=extract_mas(_pattern), horizon=extract_horizon(_pattern)))
    return _assets


def extract_line(_pattern):
    return Line(float(_pattern[3]) * sat, float(_pattern[4]) * sat, float(_pattern[5]), _pattern[6]) if 'line' in _pattern else False


def extract_mas(_pattern):
    _mas = []
    if 'ma20' in _pattern:
        _ma20_index = _pattern.index("ma20")
        _mas.append(MovingAverage(20, _pattern[_ma20_index+1]))
    if 'ma50' in _pattern:
        _ma50_index = _pattern.index("ma50")
        _mas.append(MovingAverage(50, _pattern[_ma50_index+1]))
    if 'ma100' in _pattern:
        _ma100_index = _pattern.index("ma100")
        _mas.append(MovingAverage(100, _pattern[_ma100_index+1]))
    if 'ma200' in _pattern:
        _ma200_index = _pattern.index("ma200")
        _mas.append(MovingAverage(200, _pattern[_ma200_index+1]))

    return _mas if len(_mas)>0 else False


def extract_horizon(_pattern):
    if 'horizon' in _pattern:
        _horizon_index = _pattern.index("horizon")
        return _pattern[_horizon_index+1]
    return False


def log_assets(_assets):
    logger_global[0].info(f"Patterns currently being observed : {len(_assets)}")
    for _asset in _assets:
        if _asset.horizon:
            logger_global[0].info(f"{_asset.market} horizon {_asset.horizon}")
        if _asset.line:
            logger_global[0].info(f"{_asset.market} line {_asset.line.market_type}")
        if _asset.mas:
            logger_global[0].info(f"{_asset.market} {mas_to_string(_asset.mas)}")


def mas_to_string(_mas):
    return ' '.join([str(x) for x in _mas])


class TradeMsg(object):
    def __init__(self, msg) -> None:
        self.market = msg['s']
        self.type = msg['e']
        self.price = float(msg['p'])
        self.quantity = float(msg['q'])
        self.timestamp = msg['T']
        self.timestamp_str = get_time_from_binance_tmstmp(msg['T'])
        self.taker = not msg['m']
        self.maker = msg['m']
        self.base_volume = round(self.price * self.quantity, 8)
        

class VolumeUnit(object):

    def __init__(self):
        pass

    def __init__(self, _trade_msg_list) -> None:
        self.base_volume = 0.0
        self.quantity = 0.0
        self.l00 = 0
        self.l01 = 0
        self.l02 = 0
        self.l0 = 0
        self.l0236 = 0
        self.l0382 = 0
        self.l05 = 0
        self.l0618 = 0
        self.l0786 = 0
        self.l1 = 0
        self.l1382 = 0
        self.l162 = 0
        self.l2 = 0
        self.l5 = 0
        self.l10 = 0
        self.l20 = 0
        self.l50 = 0
        self.l100 = 0
        self.mean_price = 0.0
        self.avg_price = 0.0
        self.timestamp = 0
        self._compute_levels(_trade_msg_list)
        self.base_volume = round(self.base_volume, 8)
        self.quantity = round(self.quantity, 8)

    def _compute_levels(self, _trade_msg_list):
        _ii = 0
        for _trade_msg in _trade_msg_list:
            if _ii == 0:
                self.timestamp = _trade_msg.timestamp
            if _trade_msg.quantity > 0:
                _ii += 1
                self.base_volume += _trade_msg.base_volume
                self.quantity += _trade_msg.quantity
                self.mean_price += _trade_msg.price
            if 0.0 < _trade_msg.base_volume < 5000.0:  # between 0 and 5k USDT
                self.l00 += 1
            if 5000.0 < _trade_msg.base_volume < 10000.0:  # between 5k and 10k USDT
                self.l01 += 1
            if 10000.0 < _trade_msg.base_volume < 23600.0:  # between 10k and 23.6k USDT
                self.l02 += 1
            if _trade_msg.base_volume < 23600.0:  # between 0 and 23.6k USDT
                self.l0 += 1
            elif 23600.0 < _trade_msg.base_volume < 38200.0:  # between 23.6k and 38.2k USDT
                self.l0236 += 1
            elif 38200.0 < _trade_msg.base_volume < 50000.0:  # between 38.2k and 50k USDT
                self.l0382 += 1
            elif 50000.0 < _trade_msg.base_volume < 61800.0:  # between 50k and 61.8k USDT
                self.l05 += 1
            elif 61800.0 < _trade_msg.base_volume < 78600.0:  # between 61.8k and 78.6k USDT
                self.l0618 += 1
            elif 78600.0 < _trade_msg.base_volume < 100000.0:  # between 78.6k and 100k USDT
                self.l0786 += 1
            elif 100000.0 < _trade_msg.base_volume < 138200.0:  # between 100k and 138k USDT
                self.l1 += 1
            elif 138200.0 < _trade_msg.base_volume < 162000.0:  # between 138k and 162k USDT
                self.l1382 += 1
            elif 162000.0 < _trade_msg.base_volume < 200000.0:  # between 162k and 200k USDT
                self.l162 += 1
            elif 200000.0 < _trade_msg.base_volume < 500000.0:  # between 200k and 500k USDT
                self.l2 += 1
            elif 500000.0 < _trade_msg.base_volume < 1000000.0:  # between 500k and 1 mln USDT
                self.l5 += 1
            elif 1000000.0 < _trade_msg.base_volume < 2000000.0:  # between 1 and 2 mln USDT
                self.l10 += 1
            elif 2000000.0 < _trade_msg.base_volume < 5000000.0:  # between 2 and 5 mln USDT
                self.l20 += 1
            elif 5000000.0 < _trade_msg.base_volume < 10000000.0:  # between 5 and 10 mln USDT
                self.l50 += 1
            elif 10000000.0 < _trade_msg.base_volume:  # greater than 10 mln USDT
                self.l100 += 1

        if self.quantity > 0:
            self.avg_price = round(self.base_volume / self.quantity, 8)
        if _ii > 0:
            self.mean_price = round(self.mean_price / _ii, 8)

        if self.mean_price + 0.02 < self.avg_price:
            kk =1


class VolumeContainer(object):
    def __init__(self, _market, _ticker, _start_time, _maker_volume : VolumeUnit, _taker_volume : VolumeUnit) -> None:
        self.market = _market
        self.ticker = _ticker
        self.start_time = _start_time
        self.start_time_str = 0
        self.avg_weighted_maker_price = 0
        self.avg_weighted_taker_price = 0
        self.avg_price = 0
        self.mean_price = 0
        self.maker_volume = _maker_volume
        self.taker_volume = _taker_volume
        self.total_base_volume = 0
        self.total_quantity = 0
    def print(self):
        logger_global[0].info("market: {}, total_base_volume: {}, total_quantity: {}, start_time_str: {}".format(self.market, self.maker_volume.base_volume+self.taker_volume.base_volume, self.maker_volume.quantity+self.taker_volume.quantity, get_time(self.maker_volume.timestamp)))
    def round(self):
        if self.avg_weighted_maker_price > 100:
            self.avg_weighted_maker_price = round(self.avg_weighted_maker_price, 3)
        elif self.avg_weighted_maker_price > 10:
            self.avg_weighted_maker_price = round(self.avg_weighted_maker_price, 4)
        if self.avg_weighted_taker_price > 100:
            self.avg_weighted_taker_price = round(self.avg_weighted_taker_price, 3)
        elif self.avg_weighted_taker_price > 10:
            self.avg_weighted_taker_price = round(self.avg_weighted_taker_price, 4)
        if self.avg_price > 100:
            self.avg_price = round(self.avg_price, 3)
        elif self.avg_price > 10:
            self.avg_price = round(self.avg_price, 4)
        if self.mean_price > 100:
            self.mean_price = round(self.mean_price, 3)
        elif self.mean_price > 10:
            self.mean_price = round(self.mean_price, 4)
        if self.maker_volume.avg_price > 100:
            self.maker_volume.avg_price = round(self.maker_volume.avg_price, 3)
        elif self.maker_volume.avg_price > 10:
            self.maker_volume.avg_price = round(self.maker_volume.avg_price, 4)
        if self.maker_volume.mean_price > 100:
            self.maker_volume.mean_price = round(self.maker_volume.mean_price, 3)
        elif self.maker_volume.mean_price > 10:
            self.maker_volume.mean_price = round(self.maker_volume.mean_price, 4)

        if self.taker_volume.avg_price > 100:
            self.taker_volume.avg_price = round(self.taker_volume.avg_price, 3)
        elif self.taker_volume.avg_price > 10:
            self.taker_volume.avg_price = round(self.taker_volume.avg_price, 4)
        if self.taker_volume.mean_price > 100:
            self.taker_volume.mean_price = round(self.taker_volume.mean_price, 3)
        elif self.taker_volume.mean_price > 10:
            self.taker_volume.mean_price = round(self.taker_volume.mean_price, 4)
        self.total_base_volume = round(self.total_base_volume)
        self.total_quantity = round(self.total_quantity)
        self.maker_volume.base_volume = round(self.maker_volume.base_volume)
        self.maker_volume.quantity = round(self.maker_volume.quantity)


def add_volume_containers(_c1: VolumeContainer, _c2: VolumeContainer):
    __vc = VolumeContainer(_c1.market, None, _c1.start_time, None, None)
    try:
        __vc.maker_volume = add_volume_units(_c1.maker_volume, _c2.maker_volume)
        __vc.taker_volume = add_volume_units(_c1.taker_volume, _c2.taker_volume)
    except AttributeError:
        pass
    if __vc.start_time == 0:
        __vc.start_time = _c2.start_time
    return __vc


def add_volume_units(_a: VolumeUnit, _b: VolumeUnit):
    _vu = VolumeUnit([])
    _vu.l00 = _a.l00 + _b.l00
    _vu.l01 = _a.l01 + _b.l01
    _vu.l02 = _a.l02 + _b.l02
    _vu.l0 = _a.l0 + _b.l0
    _vu.l0236 = _a.l0236 + _b.l0236
    _vu.l0382 = _a.l0382 + _b.l0382
    _vu.l05 = _a.l05 + _b.l05
    _vu.l0618 = _a.l0618 + _b.l0618
    _vu.l0786 = _a.l0786 + _b.l0786
    _vu.l1 = _a.l1 + _b.l1
    _vu.l1382 = _a.l1382 + _b.l1382
    _vu.l162 = _a.l162 + _b.l162
    _vu.l2 = _a.l2 + _b.l2
    _vu.l5 = _a.l5 + _b.l5
    _vu.l10 = _a.l10 + _b.l10
    _vu.l20 = _a.l20 + _b.l20
    _vu.l50 = _a.l50 + _b.l50
    _vu.l100 = _a.l100 + _b.l100
    _vu.mean_price = round((_a.mean_price + _b.mean_price)/2, 8) if _a.mean_price > 0 and _b.mean_price > 0 else max(_a.mean_price, _b.mean_price)
    _vu.base_volume = round(_a.base_volume + _b.base_volume, 8)
    _vu.quantity = round(_a.quantity + _b.quantity, 8)
    _vu.timestamp = _a.timestamp
    if _vu.quantity > 0:
        _vu.avg_price = round(_vu.base_volume / _vu.quantity, 8)
    return _vu


class MakerVolumeUnit(VolumeUnit):
    def __init__(self, _trade_msg_list):
        super().__init__(list(_trade_msg_list))


class TakerVolumeUnit(VolumeUnit):
    def __init__(self, _trade_msg_list):
        super().__init__(_trade_msg_list)


def flatten(_list):
    return [item for sublist in _list for item in sublist]


def get_last_db_record(_collection):
    try:
        return _collection.find_one(sort=[('_id', DESCENDING)])
    except Exception as err:
        traceback.print_tb(err.__traceback__)
        logger_global[0].exception("get_last_db_record : find_one: {}".format(err.__traceback__))
        time.sleep(30)
        return get_last_db_record(_collection)


def extract_collection_name(_text):
    _f = _text.index("\"") + 1
    _l = len(_text) - 1 - _text[::-1].index("\"")

    if "binance" in _text:
        _exchange = "binance"
    elif "kucoin" in _text:
        _exchange = "kucoin"
    return _text[_f:_l], _exchange


def check_ma_crossing(_ma, _highs, _n=5):
    for _i in range(_n):
        _f = 0.015
        if (_highs[-_i]-_ma[-_i])/_ma[-_i] < _f or (_ma[-_i] - _highs[-_i])/_highs[-_i] < _f:
            return True
    return False


def find_zero(_data):
    for _i in range(len(_data)):
        if _data[len(_data) - _i - 1] > 0 > _data[len(_data) - _i - 2]:
            return _i
    return -1


def get_bid_price(_lows, _ind):
    return np.min(_lows[len(_lows)-_ind-3:len(_lows)-_ind+1])


def check_macd(_data, _span=48):
    _ind = find_zero(_data)
    if _ind > _span or _ind < 2:
        return False
    else:
        return _ind


def get_setup_entry(_klines):
    _closes = np.array(list(map(lambda _x: float(_x.closing), _klines)))
    _opens = np.array(list(map(lambda _x: float(_x.opening), _klines)))
    _highs = list(map(lambda _x: float(_x.highest), _klines))
    _lows = list(map(lambda _x: float(_x.lowest), _klines))

    _ma40 = talib.MA(_closes, timeperiod=40)

    _current_low_price = get_bid_price(_lows, 1)
    _lesser_than_ma40 = (_ma40[-1] - _current_low_price) / _current_low_price > 0.05
    if _lesser_than_ma40:
        return _current_low_price
    _crossed = check_ma_crossing(_ma40, _highs)
    _macd, _macdsignal, _macdhist = talib.MACD(_closes, fastperiod=12, slowperiod=26, signalperiod=9)
    _macd_ind = check_macd(_macd - _macdsignal)

    _min_val, _min_ind = find_first_minimum(_macd - _macdsignal, _window=1)

    _index = _macd_ind
    if _closes[-_min_ind] < _lows[-_macd_ind]:
        _index = _min_ind
        return get_bid_price(_closes, _index)
    elif _crossed and _macd_ind:
        return get_bid_price(_lows, _index)
    return -1


def analyze_40ma(_filename, _exchange, _ticker, _time_interval, _markets_obj):
    _market_entries = []
    _exclude_markets = {}
    if path.isfile(key_dir + _filename + ".pkl"):
        _exclude_markets = get_pickled(key_dir, _filename)
    else:
        _exclude_markets[_ticker] = []
    if _ticker in _exclude_markets:
        _markets_raw = get_markets(_exchange, _ticker, _exclude_markets)
    else:
        _markets_raw = get_markets(_exchange)

    _markets = get_filtered_markets(_exchange, _markets_obj, _markets_raw, _exclude_markets, _ticker)

    logger_global[0].info(
        f"We are analyzing {len(_markets)} markets on {_exchange} ( with at least btc volume: {_markets_obj.binance_btc_vol} <- binance, {_markets_obj.kucoin_btc_vol} <- kucoin )")

    for _market in _markets:
        # if "AVAX" in _market:
        #     i=1
        try:
            if _exchange == 'kucoin':
                _klines = try_get_klines("kucoin", f"{_market}-BTC", _ticker, _time_interval)
            elif _exchange == "binance":
                _klines = try_get_klines("binance", _market, _ticker, _time_interval)
        except Exception as err:
            traceback.print_tb(err.__traceback__)
            logger_global[0].exception("{} ".format(err.__traceback__))

            logger_global[0].warning(f"No data for market {_ticker} : {_market}")
            if _ticker in _exclude_markets:
                _exclude_markets[_ticker].append(_market)
            else:
                _exclude_markets[_ticker] = [_market]

        _bid_price = get_setup_entry(_klines)
        if _bid_price>0:
            _market_entries.append((_bid_price, _market))

    if len(_market_entries):
        _info = ' '.join(format_found_tuples(_market_entries, "\n"))
        logger_global[0].info(f"{_ticker} : {_info}")
        return _market_entries

    save_to_file(key_dir, _filename, _exclude_markets)

    return False


def get_files(_path):
    for _file in os.listdir(_path):
        if os.path.isfile(os.path.join(_path, _file)):
            yield _file


def round_same_as(_to_round, _val):
    return round(_to_round, len(str(_val).split(".")[1]))
