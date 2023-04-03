import datetime
import threading
from time import sleep

import schedule
from binance.websockets import BinanceSocketManager

from library import binance_obj
from market_scanner import compute_depth_percentages, add_dc, divide_dc

binance_obj


class DepthCrawl(object):
    def __init__(self, _market, _exchange):
        self.market = _market
        self.exchange = _exchange
        self.sell_depth = []
        self.buy_depth = []

    def add_depths(self, _bd, _sd):
        _size = 500
        self.buy_depth.append(_bd)
        self.sell_depth.append(_sd)
        if len(self.buy_depth) > _size:
            self.buy_depth = self.buy_depth[-_size:]
        if len(self.sell_depth) > _size:
            self.sell_depth = self.sell_depth[-_size:]


class DepthMsg(object):
    def __init__(self, _msg):
        self.bids = list(map(lambda x: list(map(lambda y: float(y), x)), _msg['b']))
        self.asks = list(map(lambda x: list(map(lambda y: float(y), x)), _msg['a']))
        self.type = _msg['e']
        self.market = _msg['s'].lower()
        self.time = datetime.datetime.now().timestamp()


class DepthCrawl(object):
    def __init__(self, _market):
        self.market = _market


depths = {}
depths1m = {}
depths15m = {}
depths_locker = {}


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
    for _k in depths.keys():
        depths_locker[_k] = True
        if len(depths[_k]['asks']) > 0 and len(depths[_k]['bids']) > 0:
            _as, _bs = freeze_order_book(_k)
            _bd = compute_depth_percentages(_bs, "bids")
            _sd = compute_depth_percentages(_as, "asks")
            depths1m[_dc.market]['bd'].append(_bd)
            depths1m[_dc.market]['sd'].append(_sd)
            #   15m section
            _sec = int(depths1m[_dc.market]['bd'][0].time_str.split(":")[-1])
            _min = int(depths1m[_dc.market]['bd'][0].time_str.split(":")[-2])
            _t0 = int(_min / 15)
            _t1 = int(int(depths1m[_dc.market]['bd'][-1].time_str.split(":")[-2])/15)
            if _t0 != _t1:
                _bdl = depths1m[_dc.market]['bd'][-1]
                _sdl = depths1m[_dc.market]['sd'][-1]
                _bdt = depths1m[_dc.market]['bd'][0]
                _sdt = depths1m[_dc.market]['sd'][0]
                _current_timestamp = _bdt.timestamp - (_min - _t0 * 15) * 60 - _sec
                __size = len(depths1m[_dc.market]['bd']) - 1
                for _ii in range(1, __size):
                    _bdt = add_dc(_bdt, depths1m[_dc.market]['bd'][_ii])
                    _sdt = add_dc(_sdt, depths1m[_dc.market]['sd'][_ii])
                _bdt = divide_dc(_bdt, __size)
                _sdt = divide_dc(_sdt, __size)
                _bdt.set_time(_current_timestamp)
                _sdt.set_time(_current_timestamp)
                depths15m[_k]['bd'].append(_bdt)
                depths15m[_k]['sd'].append(_sdt)
                depths1m[_dc.market]['bd'].clear()
                depths1m[_dc.market]['sd'].clear()
                depths1m[_dc.market]['bd'].append(_bdl)
                depths1m[_dc.market]['sd'].append(_sdl)
                ierr = 1

        unlock(depths_locker, _k)
# exec('def process_depth_socket_message_avaxusdt(_msg):\n    _depth_msg = DepthMsg(_msg)\n    depths[\"avaxusdt\"].append(_depth_msg)\n    ')




def _do_depth_scan(_dc: DepthCrawl):
    # logger.info("Start scanning market {}".format(_vc.market))
    depths[_dc.market] = {}
    depths[_dc.market]['asks'] = {}
    depths[_dc.market]['bids'] = {}
    depths1m[_dc.market] = {}
    depths1m[_dc.market]['bd'] = []
    depths1m[_dc.market]['sd'] = []
    depths15m[_dc.market] = {}
    depths15m[_dc.market]['bd'] = []
    depths15m[_dc.market]['sd'] = []
    _bm = BinanceSocketManager(binance_obj.client)
    _conn_key = _bm.start_depth_socket(_dc.market.upper(), process_depth_socket_message)
    _bm.start()


def manage_volume_scan(_dc):
    _crawler = threading.Thread(target=_do_depth_scan, args=(_dc,),
                                name='_do_depth_scan : {}'.format("ABC"))
    _crawler.start()


_dc = DepthCrawl("avaxusdt")
manage_volume_scan(_dc)


schedule.every(1).minutes.do(do_freeze)

while True:
    # Checks whether a scheduled task
    # is pending to run or not
    schedule.run_pending()
    sleep(1)

sleep(300)

k =1

_asks, _bids = freeze_order_book(_dc.market)

compute_depth_percentages(_asks, "asks")
compute_depth_percentages(_bids, "bids")

# _bd = compute_depth_percentages(_order['bids'], "bids")
#         if _dc.exchange == "kucoin":
#             _order['asks'].reverse()
#         _sd = compute_depth_percentages(_order['asks'], "asks")
#         _dc.add_depths(_bd, _sd)