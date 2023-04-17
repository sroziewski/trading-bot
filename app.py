import json

from flask import Flask
from flask import jsonify
from flask_caching import Cache

from config import config

flask_config = {
    "DEBUG": True,          # some Flask specific configs
    "CACHE_TYPE": "SimpleCache",  # Flask-Caching related configs
    "CACHE_DEFAULT_TIMEOUT": 30
}

app = Flask(__name__)
# tell Flask to use the above defined config
app.config.from_mapping(flask_config)
cache = Cache(app)

import datetime
import threading
from functools import reduce
from time import sleep

import schedule
from binance.websockets import BinanceSocketManager

from library import binance_obj, logger_global
from bson import CodecOptions
from bson.codec_options import TypeRegistry
from library import setup_logger, DecimalCodec, get_time
from mongodb import mongo_client


depth_crawl_dict = {}


class DepthCrawl(object):
    def __init__(self, _market, _type):
        self.market = _market
        self.type = _type
        self.sell_depth_5m = []
        self.buy_depth_5m = []
        self.sell_depth_15m = []
        self.buy_depth_15m = []
        self.sell_depth_1d = []
        self.buy_depth_1d = []

    def add_depths_1d(self, _bd, _sd):
        _size_1d = 20
        if len(self.buy_depth_1d) > _size_1d:
            self.buy_depth_1d = self.buy_depth_1d[-_size_1d:]
        if len(self.sell_depth_1d) > _size_1d:
            self.sell_depth_1d = self.sell_depth_1d[-_size_1d:]
        self.buy_depth_1d.append(_bd)
        self.sell_depth_1d.append(_sd)

    def add_depths_15m(self, _bd, _sd, _market):
        _size_15m = 200
        logger_global[0].info("add_depths_15m: {} {} {}".format(_market, _bd.timestamp, _bd.time_str))
        if len(self.buy_depth_15m) > _size_15m:
            self.buy_depth_15m = self.buy_depth_15m[-_size_15m:]
        if len(self.sell_depth_15m) > _size_15m:
            self.sell_depth_15m = self.sell_depth_15m[-_size_15m:]
        self.buy_depth_15m.append(_bd)
        self.sell_depth_15m.append(_sd)

    def add_depths_5m(self, _bd, _sd, _market):
        _size_5m = 96
        logger_global[0].info("add_depths_5m: {} {} {}".format(_market, _bd.timestamp, _bd.time_str))
        if len(self.buy_depth_5m) > _size_5m:
            self.buy_depth_5m = self.buy_depth_5m[-_size_5m:]
        if len(self.sell_depth_5m) > _size_5m:
            self.sell_depth_5m = self.sell_depth_5m[-_size_5m:]
        self.buy_depth_5m.append(_bd)
        self.sell_depth_5m.append(_sd)


class DepthMsg(object):
    def __init__(self, _msg):
        self.bids = list(map(lambda x: list(map(lambda y: float(y), x)), _msg['b']))
        self.asks = list(map(lambda x: list(map(lambda y: float(y), x)), _msg['a']))
        self.type = _msg['e']
        self.market = _msg['s'].lower()
        self.time = datetime.datetime.now().timestamp()


class MarketDepth(object):
    def __init__(self, _1p, _2p, _3p, _4p, _5p, _10p, _15p, _20p, _25p, _30p, _35p, _40p, _45p, _50p, _55p, _60p, _65p,
                 _70p):
        self.p1 = _1p
        self.p2 = _2p
        self.p3 = _3p
        self.p4 = _4p
        self.p5 = _5p
        self.p10 = _10p
        self.p15 = _15p
        self.p20 = _20p
        self.p25 = _25p
        self.p30 = _30p
        self.p35 = _35p
        self.p40 = _40p
        self.p45 = _45p
        self.p50 = _50p
        self.p55 = _55p
        self.p60 = _60p
        self.p65 = _65p
        self.p70 = _70p
        self.timestamp = int(datetime.datetime.now().timestamp())
        self.time_str = get_time(self.timestamp)

    def set_time(self, _t):
        self.timestamp = _t
        self.time_str = get_time(_t)


class SellDepth(MarketDepth):
    def __init__(self, _start_price, _1p, _2p, _3p, _4p, _5p, _10p, _15p, _20p, _25p, _30p, _35p, _40p, _45p, _50p,
                 _55p, _60p, _65p, _70p):
        super().__init__(_1p, _2p, _3p, _4p, _5p, _10p, _15p, _20p, _25p, _30p, _35p, _40p, _45p, _50p, _55p, _60p,
                         _65p, _70p)
        self.ask_price = _start_price


class BuyDepth(MarketDepth):
    def __init__(self, _start_price, _1p, _2p, _3p, _4p, _5p, _10p, _15p, _20p, _25p, _30p, _35p, _40p, _45p, _50p,
                 _55p, _60p, _65p, _70p):
        super().__init__(_1p, _2p, _3p, _4p, _5p, _10p, _15p, _20p, _25p, _30p, _35p, _40p, _45p, _50p, _55p, _60p,
                         _65p, _70p)
        self.bid_price = _start_price


def compute_depth_percentages(_depth, _type):
    _start_price = _depth[0][0]
    _1p_d = (0, 0)
    _2p_d = (0, 0)
    _3p_d = (0, 0)
    _4p_d = (0, 0)
    _5p_d = (0, 0)
    _10p_d = (0, 0)
    _15p_d = (0, 0)
    _20p_d = (0, 0)
    _25p_d = (0, 0)
    _30p_d = (0, 0)
    _35p_d = (0, 0)
    _40p_d = (0, 0)
    _45p_d = (0, 0)
    _50p_d = (0, 0)
    _55p_d = (0, 0)
    _60p_d = (0, 0)
    _65p_d = (0, 0)
    _70p_d = (0, 0)
    # if _start_price > 10000:  # we assume we have BTC here ;)
    #     _divisor = 100.0
    # else:
    #     _divisor = 1.0
    _divisor = 1.0
    for _price, _amount in _depth:
        _price = float(_price)
        _amount = float(_amount)
        if _type == "bids":
            _ratio = (_start_price - _price) / _start_price
        elif _type == "asks":
            _ratio = (_price - _start_price) / _start_price

        if _ratio < 0.01 / _divisor:
            _1p_d = (_1p_d[0] + _amount, _1p_d[1] + _amount * _price)
        if _ratio < 0.02 / _divisor:
            _2p_d = (_2p_d[0] + _amount, _2p_d[1] + _amount * _price)
        if _ratio < 0.03 / _divisor:
            _3p_d = (_3p_d[0] + _amount, _3p_d[1] + _amount * _price)
        if _ratio < 0.04 / _divisor:
            _4p_d = (_4p_d[0] + _amount, _4p_d[1] + _amount * _price)
        if _ratio < 0.05 / _divisor:
            _5p_d = (_5p_d[0] + _amount, _5p_d[1] + _amount * _price)
        if _ratio < 0.10 / _divisor:
            _10p_d = (_10p_d[0] + _amount, _10p_d[1] + _amount * _price)
        if _ratio < 0.15 / _divisor:
            _15p_d = (_15p_d[0] + _amount, _15p_d[1] + _amount * _price)
        if _ratio < 0.20 / _divisor:
            _20p_d = (_20p_d[0] + _amount, _20p_d[1] + _amount * _price)
        if _ratio < 0.25 / _divisor:
            _25p_d = (_25p_d[0] + _amount, _25p_d[1] + _amount * _price)
        if _ratio < 0.30 / _divisor:
            _30p_d = (_30p_d[0] + _amount, _30p_d[1] + _amount * _price)
        if _ratio < 0.35 / _divisor:
            _35p_d = (_35p_d[0] + _amount, _35p_d[1] + _amount * _price)
        if _ratio < 0.40 / _divisor:
            _40p_d = (_40p_d[0] + _amount, _40p_d[1] + _amount * _price)
        if _ratio < 0.45 / _divisor:
            _45p_d = (_45p_d[0] + _amount, _45p_d[1] + _amount * _price)
        if _ratio < 0.5 / _divisor:
            _50p_d = (_50p_d[0] + _amount, _50p_d[1] + _amount * _price)
        if _ratio < 0.55 / _divisor:
            _55p_d = (_55p_d[0] + _amount, _55p_d[1] + _amount * _price)
        if _ratio < 0.6 / _divisor:
            _60p_d = (_60p_d[0] + _amount, _60p_d[1] + _amount * _price)
        if _ratio < 0.65 / _divisor:
            _65p_d = (_65p_d[0] + _amount, _65p_d[1] + _amount * _price)
        if _ratio < 0.7 / _divisor:
            _70p_d = (_70p_d[0] + _amount, _70p_d[1] + _amount * _price)
    if _type == "bids":
        _md = BuyDepth(_start_price, _1p_d, _2p_d, _3p_d, _4p_d, _5p_d, _10p_d, _15p_d, _20p_d, _25p_d, _30p_d, _35p_d,
                       _40p_d, _45p_d, _50p_d,
                       _55p_d, _60p_d,
                       _65p_d,
                       _70p_d)
    elif _type == "asks":
        _md = SellDepth(_start_price, _1p_d, _2p_d, _3p_d, _4p_d, _5p_d, _10p_d, _15p_d, _20p_d, _25p_d, _30p_d, _35p_d,
                        _40p_d, _45p_d, _50p_d,
                        _55p_d, _60p_d,
                        _65p_d,
                        _70p_d)
    return _md


def divide_dc(_dc, _by):
    if isinstance(_dc, BuyDepth):
        return BuyDepth(round(_dc.bid_price / _by, 10),
                        (round(_dc.p1[0] / _by, 4), round(_dc.p1[1] / _by, 4)),
                        (round(_dc.p2[0] / _by, 4), round(_dc.p2[1] / _by, 4)),
                        (round(_dc.p3[0] / _by, 4), round(_dc.p3[1] / _by, 4)),
                        (round(_dc.p4[0] / _by, 4), round(_dc.p4[1] / _by, 4)),
                        (round(_dc.p5[0] / _by, 4), round(_dc.p5[1] / _by, 4)),
                        (round(_dc.p10[0] / _by, 4), round(_dc.p10[1] / _by, 4)),
                        (round(_dc.p15[0] / _by, 4), round(_dc.p15[1] / _by, 4)),
                        (round(_dc.p20[0] / _by, 4), round(_dc.p20[1] / _by, 4)),
                        (round(_dc.p25[0] / _by, 4), round(_dc.p25[1] / _by, 4)),
                        (round(_dc.p30[0] / _by, 4), round(_dc.p30[1] / _by, 4)),
                        (round(_dc.p35[0] / _by, 4), round(_dc.p35[1] / _by, 4)),
                        (round(_dc.p40[0] / _by, 4), round(_dc.p40[1] / _by, 4)),
                        (round(_dc.p45[0] / _by, 4), round(_dc.p45[1] / _by, 4)),
                        (round(_dc.p50[0] / _by, 4), round(_dc.p50[1] / _by, 4)),
                        (round(_dc.p55[0] / _by, 4), round(_dc.p55[1] / _by, 4)),
                        (round(_dc.p60[0] / _by, 4), round(_dc.p60[1] / _by, 4)),
                        (round(_dc.p65[0] / _by, 4), round(_dc.p65[1] / _by, 4)),
                        (round(_dc.p70[0] / _by, 4), round(_dc.p70[1] / _by, 4)))
    elif isinstance(_dc, SellDepth):
        return SellDepth(round(_dc.ask_price / _by, 10),
                         (round(_dc.p1[0] / _by, 4), round(_dc.p1[1] / _by, 4)),
                         (round(_dc.p2[0] / _by, 4), round(_dc.p2[1] / _by, 4)),
                         (round(_dc.p3[0] / _by, 4), round(_dc.p3[1] / _by, 4)),
                         (round(_dc.p4[0] / _by, 4), round(_dc.p4[1] / _by, 4)),
                         (round(_dc.p5[0] / _by, 4), round(_dc.p5[1] / _by, 4)),
                         (round(_dc.p10[0] / _by, 4), round(_dc.p10[1] / _by, 4)),
                         (round(_dc.p15[0] / _by, 4), round(_dc.p15[1] / _by, 4)),
                         (round(_dc.p20[0] / _by, 4), round(_dc.p20[1] / _by, 4)),
                         (round(_dc.p25[0] / _by, 4), round(_dc.p25[1] / _by, 4)),
                         (round(_dc.p30[0] / _by, 4), round(_dc.p30[1] / _by, 4)),
                         (round(_dc.p35[0] / _by, 4), round(_dc.p35[1] / _by, 4)),
                         (round(_dc.p40[0] / _by, 4), round(_dc.p40[1] / _by, 4)),
                         (round(_dc.p45[0] / _by, 4), round(_dc.p45[1] / _by, 4)),
                         (round(_dc.p50[0] / _by, 4), round(_dc.p50[1] / _by, 4)),
                         (round(_dc.p55[0] / _by, 4), round(_dc.p55[1] / _by, 4)),
                         (round(_dc.p60[0] / _by, 4), round(_dc.p60[1] / _by, 4)),
                         (round(_dc.p65[0] / _by, 4), round(_dc.p65[1] / _by, 4)),
                         (round(_dc.p70[0] / _by, 4), round(_dc.p70[1] / _by, 4)))


def add_dc(_dc1, _dc2):
    if isinstance(_dc1, BuyDepth):
        return BuyDepth(_dc1.bid_price + _dc2.bid_price,
                        (_dc1.p1[0] + _dc2.p1[0], _dc1.p1[1] + _dc2.p1[1]),
                        (_dc1.p2[0] + _dc2.p2[0], _dc1.p2[1] + _dc2.p2[1]),
                        (_dc1.p3[0] + _dc2.p3[0], _dc1.p3[1] + _dc2.p3[1]),
                        (_dc1.p4[0] + _dc2.p4[0], _dc1.p4[1] + _dc2.p4[1]),
                        (_dc1.p5[0] + _dc2.p5[0], _dc1.p5[1] + _dc2.p5[1]),
                        (_dc1.p10[0] + _dc2.p10[0], _dc1.p10[1] + _dc2.p10[1]),
                        (_dc1.p15[0] + _dc2.p15[0], _dc1.p15[1] + _dc2.p15[1]),
                        (_dc1.p20[0] + _dc2.p20[0], _dc1.p20[1] + _dc2.p20[1]),
                        (_dc1.p25[0] + _dc2.p25[0], _dc1.p25[1] + _dc2.p25[1]),
                        (_dc1.p30[0] + _dc2.p30[0], _dc1.p30[1] + _dc2.p30[1]),
                        (_dc1.p35[0] + _dc2.p35[0], _dc1.p35[1] + _dc2.p35[1]),
                        (_dc1.p40[0] + _dc2.p40[0], _dc1.p40[1] + _dc2.p40[1]),
                        (_dc1.p45[0] + _dc2.p45[0], _dc1.p45[1] + _dc2.p45[1]),
                        (_dc1.p50[0] + _dc2.p50[0], _dc1.p50[1] + _dc2.p50[1]),
                        (_dc1.p55[0] + _dc2.p55[0], _dc1.p55[1] + _dc2.p55[1]),
                        (_dc1.p60[0] + _dc2.p60[0], _dc1.p60[1] + _dc2.p60[1]),
                        (_dc1.p65[0] + _dc2.p65[0], _dc1.p65[1] + _dc2.p65[1]),
                        (_dc1.p70[0] + _dc2.p70[0], _dc1.p70[1] + _dc2.p70[1]))
    elif isinstance(_dc1, SellDepth):
        return SellDepth(_dc1.ask_price + _dc2.ask_price,
                         (_dc1.p1[0] + _dc2.p1[0], _dc1.p1[1] + _dc2.p1[1]),
                         (_dc1.p2[0] + _dc2.p2[0], _dc1.p2[1] + _dc2.p2[1]),
                         (_dc1.p3[0] + _dc2.p3[0], _dc1.p3[1] + _dc2.p3[1]),
                         (_dc1.p4[0] + _dc2.p4[0], _dc1.p4[1] + _dc2.p4[1]),
                         (_dc1.p5[0] + _dc2.p5[0], _dc1.p5[1] + _dc2.p5[1]),
                         (_dc1.p10[0] + _dc2.p10[0], _dc1.p10[1] + _dc2.p10[1]),
                         (_dc1.p15[0] + _dc2.p15[0], _dc1.p15[1] + _dc2.p15[1]),
                         (_dc1.p20[0] + _dc2.p20[0], _dc1.p20[1] + _dc2.p20[1]),
                         (_dc1.p25[0] + _dc2.p25[0], _dc1.p25[1] + _dc2.p25[1]),
                         (_dc1.p30[0] + _dc2.p30[0], _dc1.p30[1] + _dc2.p30[1]),
                         (_dc1.p35[0] + _dc2.p35[0], _dc1.p35[1] + _dc2.p35[1]),
                         (_dc1.p40[0] + _dc2.p40[0], _dc1.p40[1] + _dc2.p40[1]),
                         (_dc1.p45[0] + _dc2.p45[0], _dc1.p45[1] + _dc2.p45[1]),
                         (_dc1.p50[0] + _dc2.p50[0], _dc1.p50[1] + _dc2.p50[1]),
                         (_dc1.p55[0] + _dc2.p55[0], _dc1.p55[1] + _dc2.p55[1]),
                         (_dc1.p60[0] + _dc2.p60[0], _dc1.p60[1] + _dc2.p60[1]),
                         (_dc1.p65[0] + _dc2.p65[0], _dc1.p65[1] + _dc2.p65[1]),
                         (_dc1.p70[0] + _dc2.p70[0], _dc1.p70[1] + _dc2.p70[1]))

depths = {}
depths1m = {}
depths_locker = {}
markets_5m = {}
types = {}


def process_depth_socket_message(_msg):
    _depth_msg = DepthMsg(_msg)
    while _depth_msg.market in depths_locker:
        sleep(1)
    for _ask in _depth_msg.asks:
        if _ask[1] > 0:
            depths[_depth_msg.market]['asks'][_ask[0]] = _ask[1]
        elif _ask[0] in depths[_depth_msg.market]['asks']:
            del depths[_depth_msg.market]['asks'][_ask[0]]

    for _bid in _depth_msg.bids:
        if _bid[1] > 0:
            depths[_depth_msg.market]['bids'][_bid[0]] = _bid[1]
        elif _bid[0] in depths[_depth_msg.market]['bids']:
            del depths[_depth_msg.market]['bids'][_bid[0]]


def freeze_order_book(_market):
    __asks = []
    __bids = []
    for _price, _amount in depths[_market]['asks'].items():
        __asks.append((_price, _amount))
    for _price, _amount in depths[_market]['bids'].items():
        __bids.append((_price, _amount))
    __asks.sort(key=lambda x: x[0])
    __bids.sort(key=lambda x: x[0], reverse=True)

    return __asks, __bids


def unlock(_locker, _key):
    del _locker[_key]


def do_freeze():
    for _market_c in depths.keys():
        depths_locker[_market_c] = True
        if len(depths[_market_c]['asks']) > 0 and len(depths[_market_c]['bids']) > 0:
            _as, _bs = freeze_order_book(_market_c)
            _bd = compute_depth_percentages(_bs, "bids")
            _sd = compute_depth_percentages(_as, "asks")
            depths1m[_market_c]['bd'].append(_bd)
            depths1m[_market_c]['sd'].append(_sd)
            #   15m section
            _sec = int(depths1m[_market_c]['bd'][0].time_str.split(":")[-1])
            _min = int(depths1m[_market_c]['bd'][0].time_str.split(":")[-2])
            _t0_quarter = int(_min / 15)
            _t1_quarter = int(int(depths1m[_market_c]['bd'][-1].time_str.split(":")[-2])/15)

            _t0_5m = int(_min / 5)
            _t1_5m = int(int(depths1m[_market_c]['bd'][-1].time_str.split(":")[-2]) / 5)

            if _market_c in markets_5m[types[_market_c]] and _t0_5m != _t1_5m:
                _bdt_5m = depths1m[_market_c]['bd'][0]
                _sdt_5m = depths1m[_market_c]['sd'][0]
                _current_timestamp = _bdt_5m.timestamp - (_min - _t0_5m * 5) * 60 - _sec
                _bds_f_5m = list(filter(lambda x: _t0_5m == int(int(x.time_str.split(":")[-2])/5),
                                     depths1m[_market_c]['bd']))
                _sds_f_5m = list(filter(lambda x: _t0_5m == int(int(x.time_str.split(":")[-2])/5),
                                     depths1m[_market_c]['sd']))
                _bd_5m = reduce(add_dc, _bds_f_5m)
                _sd_5m = reduce(add_dc, _sds_f_5m)
                _bd_5m = divide_dc(_bd_5m, len(_bds_f_5m))
                _sd_5m = divide_dc(_sd_5m, len(_sds_f_5m))
                _bd_5m.set_time(_current_timestamp)
                _sd_5m.set_time(_current_timestamp)
                if not any(filter(lambda x: x.timestamp == _current_timestamp, depth_crawl_dict[_market_c].buy_depth_5m)):
                    depth_crawl_dict[_market_c].add_depths_5m(_bd_5m, _sd_5m, _market_c)
                else:
                    _tmts_tmp = list(map(lambda x: x.timestamp, depths1m[_market_c]['bd']))
                    _tmts = []
                    for __ts in _tmts_tmp:
                        __s = int(get_time(__ts).split(":")[-1])
                        _tmts.append(__ts-__s)
                    _bd_5_l = depth_crawl_dict[_market_c].buy_depth_5m[-1]
                    _next_5m_tmstmp = int(_bd_5_l.timestamp + 5 * 60)
                    try:
                        _idx_5m = _tmts.index(_next_5m_tmstmp)
                        __in_list = True
                    except ValueError:
                        __in_list = False
                    if __in_list and len(depths1m[_market_c]['bd'][_idx_5m:_idx_5m + 5]) == 5:
                        _bd_5m_l = reduce(add_dc, depths1m[_market_c]['bd'][_idx_5m:_idx_5m + 5])
                        _sd_5m_l = reduce(add_dc, depths1m[_market_c]['sd'][_idx_5m:_idx_5m + 5])
                        _bd_5m_l = divide_dc(_bd_5m_l, 5)
                        _sd_5m_l = divide_dc(_sd_5m_l, 5)
                        _bd_5m_l.set_time(_next_5m_tmstmp)
                        _sd_5m_l.set_time(_next_5m_tmstmp)
                        depth_crawl_dict[_market_c].add_depths_5m(_bd_5m_l, _sd_5m_l, _market_c)
            if _t0_quarter != _t1_quarter:
                _bdl_1m = depths1m[_market_c]['bd'][-1]
                _sdl_1m = depths1m[_market_c]['sd'][-1]
                _bdt_5m = depths1m[_market_c]['bd'][0]
                _sdt_5m = depths1m[_market_c]['sd'][0]
                _current_timestamp = _bdt_5m.timestamp - (_min - _t0_quarter * 15) * 60 - _sec
                __size = len(depths1m[_market_c]['bd']) - 1
                for _ii in range(1, __size):
                    _bdt_5m = add_dc(_bdt_5m, depths1m[_market_c]['bd'][_ii])
                    _sdt_5m = add_dc(_sdt_5m, depths1m[_market_c]['sd'][_ii])
                _bdt_5m = divide_dc(_bdt_5m, __size)
                _sdt_5m = divide_dc(_sdt_5m, __size)
                _bdt_5m.set_time(_current_timestamp)
                _sdt_5m.set_time(_current_timestamp)
                depth_crawl_dict[_market_c].add_depths_15m(_bdt_5m, _sdt_5m, _market_c)
            #  day section
            _t0_day = int(depths1m[_market_c]['bd'][0].time_str.split(" ")[0])
            _t1_day = int(depths1m[_market_c]['bd'][-1].time_str.split(" ")[0])
            _hour = int(depths1m[_market_c]['bd'][0].time_str.split(":")[0].split(" ")[-1])
            if _t0_day != _t1_day:
                _bdt_5m = depths1m[_market_c]['bd'][0]
                _sdt_5m = depths1m[_market_c]['sd'][0]
                _current_timestamp = _bdt_5m.timestamp - _hour * 60 * 60 - _min * 60 - _sec
                _bds_f_5m = list(filter(lambda x: _t0_day == int(x.time_str.split(" ")[0]), depth_crawl_dict[_market_c].buy_depth_15m))
                _sds_f_5m = list(filter(lambda x: _t0_day == int(x.time_str.split(" ")[0]), depth_crawl_dict[_market_c].sell_depth_15m))
                _bd_5m = reduce(add_dc, _bds_f_5m)
                _sd_5m = reduce(add_dc, _sds_f_5m)
                _bd_5m = divide_dc(_bd_5m, len(_bds_f_5m))
                _sd_5m = divide_dc(_sd_5m, len(_sds_f_5m))
                _bd_5m.set_time(_current_timestamp)
                _sd_5m.set_time(_current_timestamp)
                depth_crawl_dict[_market_c].add_depths_1d(_bd_5m, _sd_5m)
            if _t0_quarter != _t1_quarter:
                depths1m[_market_c]['bd'].clear()
                depths1m[_market_c]['sd'].clear()
                depths1m[_market_c]['bd'].append(_bdl_1m)
                depths1m[_market_c]['sd'].append(_sdl_1m)
        unlock(depths_locker, _market_c)


def _do_depth_scan(_dc: DepthCrawl):
    # logger.info("Start scanning market {}".format(_vc.market))
    depths[_dc.market] = {}
    depths[_dc.market]['asks'] = {}
    depths[_dc.market]['bids'] = {}
    depths1m[_dc.market] = {}
    depths1m[_dc.market]['bd'] = []
    depths1m[_dc.market]['sd'] = []
    types[_dc.market] = _dc.type
    _bm = BinanceSocketManager(binance_obj.client)
    _conn_key = _bm.start_depth_socket(_dc.market.upper(), process_depth_socket_message)
    _bm.start()


def manage_depth_scan(_dc):
    _crawler = threading.Thread(target=_do_depth_scan, args=(_dc,),
                                name='_do_depth_scan : {}'.format("ABC"))
    _crawler.start()


def run_schedule():
    while True:
        # Checks whether a scheduled task
        # is pending to run or not
        schedule.run_pending()
        sleep(1)


def manage_schedule():
    _thread = threading.Thread(target=run_schedule, name='manage_depth_crawl')
    _thread.start()


def _stuff(_market_type):
    filename = "Binance-OrderBook-Scanner"
    logger = setup_logger(filename)
    logger.info("Starting Order Book Depth Crawl...")
    schedule.every(1).minutes.do(do_freeze)
    manage_schedule()

    db_markets_info = mongo_client.markets_info
    decimal_codec = DecimalCodec()
    type_registry = TypeRegistry([decimal_codec])
    codec_options = CodecOptions(type_registry=type_registry)
    usdt_markets_collection = db_markets_info.get_collection(_market_type, codec_options=codec_options)
    _market_info_cursor = usdt_markets_collection.find()
    _market_info_list = [e for e in _market_info_cursor]

    _5m_markets_info_cursor = usdt_markets_collection.find({"tickers": {"$elemMatch": {"$eq": "5m"}}})
    _5m_market_name_list = [e['name'] for e in _5m_markets_info_cursor]
    markets_5m[usdt_markets_collection.name] = list(
        map(lambda x: x + usdt_markets_collection.name, _5m_market_name_list))

    for _market_s in _market_info_list:
        _market = "{}{}".format(_market_s['name'], usdt_markets_collection.name)
        _dc = DepthCrawl(_market, usdt_markets_collection.name)
        depth_crawl_dict[_market] = _dc
        manage_depth_scan(_dc)


if __name__ == "app":
    _stuff("usdt")
    _stuff("btc")
    _stuff("busd")


flask_token = config.get_parameter('flask_token')


@app.route("/qu3ry/{}".format(flask_token))
@cache.cached(timeout=30)
def markets():
    return jsonify(json.dumps(depth_crawl_dict, default=lambda o: o.__dict__, sort_keys=True))


@app.route("/qu3ry/<market>/{}".format(flask_token))
@cache.cached(timeout=30)
def market(market):
    return jsonify(json.dumps(depth_crawl_dict[market], default=lambda o: o.__dict__, sort_keys=True))

