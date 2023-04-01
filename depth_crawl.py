import datetime
import threading
from time import sleep

from binance.websockets import BinanceSocketManager

from library import binance_obj
from market_scanner import compute_depth_percentages

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
        self.time = datetime.datetime.now().timestamp()


class DepthCrawl(object):
    def __init__(self, _market):
        self.market = _market


depths = {}


def process_depth_socket_message_avaxusdt(_msg):
    _depth_msg = DepthMsg(_msg)

    for _ask in _depth_msg.asks:
        if _ask[1] > 0:
            depths['avaxusdt']['asks'][_ask[0]] = _ask[1]
        elif _ask[0] in depths['avaxusdt']['asks']:
            del depths['avaxusdt']['asks'][_ask[0]]

    for _bid in _depth_msg.bids:
        if _bid[1] > 0:
            depths['avaxusdt']['bids'][_bid[0]] = _bid[1]
        elif _bid[0] in depths['avaxusdt']['bids']:
            del depths['avaxusdt']['bids'][_bid[0]]


def freeze_order_book(_market):
    depths[_market]['asks']
    depths[_market]['bids']
    _asks = []
    _bids = []
    for _price, _amount in depths[_market]['asks'].iteritems():
        _asks.append((_price, _amount))
    for _price, _amount in depths[_market]['bids'].iteritems():
        _bids.append((_price, _amount))
    _asks.sort(key=lambda x: x[0])
    _bids.sort(key=lambda x: x[0], reverse=True)


# exec('def process_depth_socket_message_avaxusdt(_msg):\n    _depth_msg = DepthMsg(_msg)\n    depths[\"avaxusdt\"].append(_depth_msg)\n    ')




def _do_depth_scan(_dc: DepthCrawl):
    # logger.info("Start scanning market {}".format(_vc.market))
    depths[_dc.market] = {}
    depths[_dc.market]['asks'] = {}
    depths[_dc.market]['bids'] = {}
    _bm = BinanceSocketManager(binance_obj.client)
    _f = eval("process_depth_socket_message_{}".format(_dc.market))
    _conn_key = _bm.start_depth_socket(_dc.market.upper(), _f)
    _bm.start()



def manage_volume_scan(_dc):
    _crawler = threading.Thread(target=_do_depth_scan, args=(_dc,),
                                name='_do_depth_scan : {}'.format("ABC"))
    _crawler.start()


_dc = DepthCrawl("avaxusdt")
manage_volume_scan(_dc)

sleep(300)

k =1
# _bd = compute_depth_percentages(_order['bids'], "bids")
#         if _dc.exchange == "kucoin":
#             _order['asks'].reverse()
#         _sd = compute_depth_percentages(_order['asks'], "asks")
#         _dc.add_depths(_bd, _sd)