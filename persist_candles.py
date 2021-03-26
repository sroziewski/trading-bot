import datetime
import threading
import traceback
from random import randrange
from time import sleep

from binance.client import Client as BinanceClient
from binance.exceptions import BinanceAPIException
from binance.websockets import BinanceSocketManager
from bson import CodecOptions
from bson.codec_options import TypeRegistry
from kucoin.exceptions import KucoinAPIException
from pymongo.errors import PyMongoError

from library import get_binance_klines, get_binance_interval_unit, setup_logger, get_kucoin_klines, \
    get_kucoin_interval_unit, binance_obj, kucoin_client, DecimalCodec, try_get_klines, TradeMsg, get_last_db_record, get_time, get_time_from_binance_tmstmp
from mongodb import mongo_client

logger = setup_logger("Kline-Crawl-Manager-HTF")

db = mongo_client.klines
decimal_codec = DecimalCodec()
type_registry = TypeRegistry([decimal_codec])
codec_options = CodecOptions(type_registry=type_registry)

trades = {}


def to_mongo_binance(_kline):
    _data = to_mongo(_kline)
    _data['buy_btc_volume'] = _kline.buy_btc_volume
    _data['buy_quantity'] = _kline.buy_quantity
    _data['sell_btc_volume'] = _kline.sell_btc_volume
    _data['sell_quantity'] = _kline.sell_quantity
    return _data


def to_mongo(_kline):
    return {
        'exchange': _kline.exchange,
        'ticker': _kline.ticker,
        'start_time': _kline.start_time,
        'opening': _kline.opening,
        'closing': _kline.closing,
        'lowest': _kline.lowest,
        'highest': _kline.highest,
        'volume': round(_kline.volume, 4),
        'btc_volume': round(_kline.btc_volume, 4),
        'time_str': _kline.time_str,
        'market': _kline.market,
        'bid_price': _kline.bid_depth.bid_price,
        'ask_price': _kline.ask_depth.ask_price,
        'bid_depth': {
            'p5': _kline.bid_depth.p5,
            'p10': _kline.bid_depth.p10,
            'p15': _kline.bid_depth.p15,
            'p20': _kline.bid_depth.p20,
            'p25': _kline.bid_depth.p25,
            'p30': _kline.bid_depth.p30,
            'p35': _kline.bid_depth.p35,
            'p40': _kline.bid_depth.p40,
            'p45': _kline.bid_depth.p45,
            'p50': _kline.bid_depth.p50,
            'p55': _kline.bid_depth.p55,
            'p60': _kline.bid_depth.p60,
            'p65': _kline.bid_depth.p65,
            'p70': _kline.bid_depth.p70
        },
        'ask_depth': {
            'p5': _kline.ask_depth.p5,
            'p10': _kline.ask_depth.p10,
            'p15': _kline.ask_depth.p15,
            'p20': _kline.ask_depth.p20,
            'p25': _kline.ask_depth.p25,
            'p30': _kline.ask_depth.p30,
            'p35': _kline.ask_depth.p35,
            'p40': _kline.ask_depth.p40,
            'p45': _kline.ask_depth.p45,
            'p50': _kline.ask_depth.p50,
            'p55': _kline.ask_depth.p55,
            'p60': _kline.ask_depth.p60,
            'p65': _kline.ask_depth.p65,
            'p70': _kline.ask_depth.p70
        }
    }


def persist_kline(_kline, _collection):
    try:
        if _kline.exchange == "binance":
            _collection.insert_one({'kline': to_mongo_binance(_kline), 'timestamp': _kline.start_time})
        else:
            _collection.insert_one({'kline': to_mongo(_kline), 'timestamp': _kline.start_time})
    except PyMongoError as err:
        traceback.print_tb(err.__traceback__)
        logger.exception("{} {}".format(_kline['market'], err.__traceback__))
        sleep(5)
        persist_kline(_kline, _collection)


def filter_current_klines(_klines, _collection_name, _collection):
    _last_record = get_last_db_record(_collection)
    logger.info(
        "{} : last kline : {} ".format(_collection_name, _last_record['kline']['time_str'] if _last_record else "None"))
    _out = None
    if _last_record:
        _out = list(filter(lambda x: int(x.start_time) > int(_last_record['timestamp']), _klines))
    else:
        _out = _klines
        logger.info("{} : first kline : {} ".format(_collection_name, _out[0].time_str if _out else "None"))
    return _out


def persist_klines(_klines, _collection):
    return list(map(lambda x: persist_kline(x, _collection), _klines))


class Schedule(object):
    def __init__(self, _market, _collection_name, _ticker, _sleep, _exchange, _dc, _no_depths, _vc=False):
        self.market = _market
        self.collection_name = _collection_name
        self.ticker = _ticker
        self.sleep = _sleep
        self.exchange = _exchange
        self.depth_crawl = _dc
        self.no_depths = _no_depths
        self.volume_crawl = _vc


class MarketDepth(object):
    def __init__(self, _5p, _10p, _15p, _20p, _25p, _30p, _35p, _40p, _45p, _50p, _55p, _60p, _65p, _70p):
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
        self.timestamp = datetime.datetime.now().timestamp()


class SellDepth(MarketDepth):
    def __init__(self, _start_price, _5p, _10p, _15p, _20p, _25p, _30p, _35p, _40p, _45p, _50p, _55p, _60p, _65p, _70p):
        super().__init__(_5p, _10p, _15p, _20p, _25p, _30p, _35p, _40p, _45p, _50p, _55p, _60p, _65p, _70p)
        self.ask_price = _start_price


class BuyDepth(MarketDepth):
    def __init__(self, _start_price, _5p, _10p, _15p, _20p, _25p, _30p, _35p, _40p, _45p, _50p, _55p, _60p, _65p, _70p):
        super().__init__(_5p, _10p, _15p, _20p, _25p, _30p, _35p, _40p, _45p, _50p, _55p, _60p, _65p, _70p)
        self.bid_price = _start_price


def manage_crawling(_schedules):
    sleep(5)
    for _schedule in _schedules:
        _scheduler = threading.Thread(target=_do_schedule, args=(_schedule,),
                                      name='_do_schedule : {}'.format(_schedule.collection_name))
        _scheduler.start()


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


class VolumeCrawl(object):
    def __init__(self, _market, _exchange):
        self.market = _market
        self.exchange = _exchange
        self.buy_btc_volume = False
        self.buy_quantity = False
        self.sell_btc_volume = False
        self.sell_quantity = False
    #
    # def add_volumes(self, _bv, _sv):
    #     _size = 1440010 # 1000 per minute, 24hrs -- that's a maximum
    #     self.buy_volume.append(_bv)
    #     self.sell_volume.append(_sv)
    #     if len(self.buy_volume) > _size:
    #         self.buy_volume = self.buy_volume[-_size:]
    #     if len(self.sell_volume) > _size:
    #         self.sell_volume = self.sell_volume[-_size:]


def _do_depth_crawl(_dc):
    while True:
        sleep(randrange(60))
        try:
            if _dc.exchange == "binance":
                _order = binance_obj.client.get_order_book(symbol=_dc.market, limit=1000)
            elif _dc.exchange == "kucoin":
                _order = kucoin_client.get_full_order_book(_dc.market)
        except BinanceAPIException as err:
            traceback.print_tb(err.__traceback__)
            logger.exception("BinanceAPIException -> sleeping{} {}".format(_dc.market, err.__traceback__))
            sleep(60)
        except ConnectionError as err:
            traceback.print_tb(err.__traceback__)
            logger.exception("ConnectionError -> sleeping{} {}".format(_dc.market, err.__traceback__))
            sleep(60)
        except KucoinAPIException as err:
            traceback.print_tb(err.__traceback__)
            logger.exception("KucoinAPIException -> sleeping{} {}".format(_dc.market, err.__traceback__))
            _order = "sleeping"
            sleep(60)
            _order = kucoin_client.get_full_order_book(_dc.market)
        except Exception as err:
            traceback.print_tb(err.__traceback__)
            logger.exception("{} {}".format(_dc.market, err.__traceback__))

        _bd = compute_depth_percentages(_order['bids'], "bids")
        if _dc.exchange == "kucoin":
            _order['asks'].reverse()
        _sd = compute_depth_percentages(_order['asks'], "asks")
        _dc.add_depths(_bd, _sd)
        sleep(3 * 60)


def filter_current_trades(_vc):
    _yesterday_time = (datetime.datetime.now().timestamp() - 24*60*60)*1000
    trades[_vc.market] = list(filter(lambda x: x.timestamp > _yesterday_time, trades[_vc.market]))


def _do_volume_crawl(_vc):
    trades[_vc.market] = []
    last_tmstmp = datetime.datetime.now().timestamp()
    _bm = BinanceSocketManager(binance_obj.client)
    _conn_key = _bm.start_aggtrade_socket(_vc.market, process_trade_socket_message)
    _bm.start()
    while True:
        _tmstmp_diff = datetime.datetime.now().timestamp() - last_tmstmp
        if _tmstmp_diff > 60*5:
            logger.warn(f"last trading volume tmstmp ({last_tmstmp}) is older than 5 minutes, diff = {_tmstmp_diff}")
        filter_current_trades(_vc)
        if len(trades[_vc.market]) > 0:
            logger.info(f"last trading volume : {trades[_vc.market][-1].timestamp_str} {_vc.market}")
        sleep(60*60)


def manage_depth_crawling(_dc):
    _crawler = threading.Thread(target=_do_depth_crawl, args=(_dc,),
                                name='_do_depth_crawl : {}'.format(_dc.market))
    _crawler.start()


def set_trade_volume(_schedule, _kline):
    _diff = None
    if _schedule.ticker == "15m":
        _diff = 15 * 60
    if _schedule.ticker == "30m":
        _diff = 30 * 60
    if _schedule.ticker == "1h":
        _diff = 60 * 60
    if _schedule.ticker == "4h":
        _diff = 4 * 60 * 60
    if _schedule.ticker == "8h":
        _diff = 8 * 60 * 60
    if _schedule.ticker == "12h":
        _diff = 12 * 60 * 60
    if _schedule.ticker == "1d":
        _diff = 24 * 60 * 60
    _diff *= 1000 # to binance msec timestamp

    _kline_timestamp = _kline.start_time
    if len(trades[_schedule.volume_crawl.market]) > 0:
        _trades_list = list(filter(lambda x: 0 <= x.timestamp - _kline_timestamp <= _diff, trades[_schedule.volume_crawl.market]))
        # _trades_list = []
        # for _trade in trades[_schedule.volume_crawl.market]:
        #     if 0 <= _trade.timestamp - _kline_timestamp <= _diff:
        #         _trades_list.append(_trade)
        #         logger.info(f"{_trade.market} trade: {_trade.timestamp_str} kline: {_kline_timestamp} trade: {_trade.timestamp} kline: {get_time_from_binance_tmstmp(_kline_timestamp)} diff: {_kline_timestamp-_trade.timestamp} {_trade.price} {_trade.quantity}")
        # logger.info("AAAAAAAAAAAAAA")
        # for _trade in _trades_list0:
        #     logger.info(
        #         f"{_trade.market} trade: {_trade.timestamp_str} kline: {_kline_timestamp} trade: {_trade.timestamp} kline: {get_time_from_binance_tmstmp(_kline_timestamp)} diff: {_kline_timestamp - _trade.timestamp} {_trade.price} {_trade.quantity}")
        add_volumes(_trades_list, _kline)


def add_volumes(_trades_msgs, _kline):
    _buys = list(filter(lambda x: x.buy, _trades_msgs))
    _sells = list(filter(lambda x: x.sell, _trades_msgs))
    _buy_btc_volume = round(sum([_msg.btc_volume for _msg in _buys]), 4)
    _buy_quantity = round(sum([_msg.quantity for _msg in _buys]), 4)
    _sell_btc_volume = round(sum([_msg.btc_volume for _msg in _sells]), 4)
    _sell_quantity = round(sum([_msg.quantity for _msg in _sells]), 4)
    _kline.add_trade_volumes(_buy_btc_volume, _buy_quantity, _sell_btc_volume, _sell_quantity)
    # logger.info(f"kline_time: {get_time_from_binance_tmstmp(_kline.start_time)} current_time:{get_time(datetime.datetime.now().timestamp())} _buy_btc_volume: {_buy_btc_volume} _sell_btc_volume: {_sell_btc_volume} _buy_quantity: {_buy_quantity} _sell_quantity {_sell_quantity}")


def process_trade_socket_message(_msg):
    _trade_msg = TradeMsg(_msg)
    trades[_trade_msg.market].append(_trade_msg)
    last_tmstmp = datetime.datetime.now().timestamp()


def manage_volume_crawling(_vc):
    _crawler = threading.Thread(target=_do_volume_crawl, args=(_vc,),
                                name='_do_volume_crawl : {}'.format(_vc.market))
    _crawler.start()


def compute_depth_percentages(_depth, _type):
    _start_price = float(_depth[0][0])
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
    if _start_price > 10000:  # we assume we have BTC here ;)
        _divisor = 100.0
    else:
        _divisor = 1.0

    for _price, _amount in _depth:
        _price = float(_price)
        _amount = float(_amount)
        if _type == "bids":
            _ratio = (_start_price - _price) / _start_price
        elif _type == "asks":
            _ratio = (_price - _start_price) / _start_price
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
        _md = BuyDepth(_start_price, _5p_d, _10p_d, _15p_d, _20p_d, _25p_d, _30p_d, _35p_d, _40p_d, _45p_d, _50p_d,
                       _55p_d, _60p_d,
                       _65p_d,
                       _70p_d)
    elif _type == "asks":
        _md = SellDepth(_start_price, _5p_d, _10p_d, _15p_d, _20p_d, _25p_d, _30p_d, _35p_d, _40p_d, _45p_d, _50p_d,
                        _55p_d, _60p_d,
                        _65p_d,
                        _70p_d)
    return _md


def get_average_depths(_dc, _number_of_elements):
    while len(_dc.buy_depth) == 0:
        logger.info(f"{_dc.market} get_average_depths sleeping : len(_dc.buy_depth) == 0")
        sleep(20)
    if len(_dc.buy_depth) < _number_of_elements:
        _number_of_elements = len(_dc.buy_depth)
    if _number_of_elements == 1:
        return divide_dc(_dc.buy_depth[0], 1), divide_dc(_dc.sell_depth[0], 1)
    _bd = add_dc(_dc.buy_depth[-_number_of_elements:][0], _dc.buy_depth[-_number_of_elements:][1])
    for _i in range(2, _number_of_elements):
        _bd = add_dc(_bd, _dc.buy_depth[-_number_of_elements:][_i])
    _sd = add_dc(_dc.sell_depth[-_number_of_elements:][0], _dc.sell_depth[-_number_of_elements:][1])
    for _i in range(2, _number_of_elements):
        _sd = add_dc(_sd, _dc.sell_depth[-_number_of_elements:][_i])
    return divide_dc(_bd, _number_of_elements), divide_dc(_sd, _number_of_elements)


def divide_dc(_dc, _by):
    if isinstance(_dc, BuyDepth):
        return BuyDepth(round(_dc.bid_price / _by, 10), (round(_dc.p5[0] / _by, 4), round(_dc.p5[1] / _by, 4)),
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
        return SellDepth(round(_dc.ask_price / _by, 10), (round(_dc.p5[0] / _by, 4), round(_dc.p5[1] / _by, 4)),
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
        return BuyDepth(_dc1.bid_price + _dc2.bid_price, (_dc1.p5[0] + _dc2.p5[0], _dc1.p5[1] + _dc2.p5[1]),
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
        return SellDepth(_dc1.ask_price + _dc2.ask_price, (_dc1.p5[0] + _dc2.p5[0], _dc1.p5[1] + _dc2.p5[1]),
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


def _do_schedule(_schedule):
    market = _schedule.market
    ticker = _schedule.ticker
    collection_name = _schedule.collection_name
    collection = db.get_collection(collection_name, codec_options=codec_options)
    sleep(1)
    while True:
        if ticker == BinanceClient.KLINE_INTERVAL_15MINUTE or BinanceClient.KLINE_INTERVAL_30MINUTE:
            sleep(randrange(100))
        else:
            sleep(randrange(200))
        if _schedule.exchange == "binance":
            try:
                klines = try_get_klines(_schedule.exchange, market, ticker, get_binance_interval_unit(ticker))
                klines = klines[:-1]  # we skip the last kline on purpose to have for it a crawling volume
            except Exception as err:
                traceback.print_tb(err.__traceback__)
                logger.exception("{} {} {}".format(_schedule.exchange, collection_name, err.__traceback__))
                sleep(randrange(30))
                klines = get_binance_klines(market, ticker, get_binance_interval_unit(ticker))
                klines = klines[:-1]  # we skip the last kline on purpose to have for it a crawling volume
        elif _schedule.exchange == "kucoin":
            try:
                klines = try_get_klines(_schedule.exchange, market, ticker, get_kucoin_interval_unit(ticker))
            except Exception:
                traceback.print_tb(err.__traceback__)
                logger.exception("{} {} {}".format(_schedule.exchange, collection_name, err.__traceback__))
                sleep(randrange(30))
                klines = get_kucoin_klines(market, ticker, get_kucoin_interval_unit(ticker))
        logger.info("Storing to collection : {} : {} ".format(_schedule.exchange, collection_name))
        current_klines = filter_current_klines(klines, collection_name, collection)
        sleep(15)
        bd, sd = get_average_depths(_schedule.depth_crawl, _schedule.no_depths)
        list(map(lambda x: x.add_buy_depth(bd), current_klines))
        list(map(lambda x: x.add_sell_depth(sd), current_klines))
        list(map(lambda x: x.add_market(market), current_klines))
        if _schedule.exchange == "binance":
            list(map(lambda x: set_trade_volume(_schedule, x), current_klines))
        list(map(lambda x: x.add_exchange(_schedule.exchange), current_klines))
        persist_klines(current_klines, collection)
        sleep(_schedule.sleep+randrange(round(_schedule.sleep/2)))


def get_binance_schedules(_asset):
    _exchange = "binance"
    if _asset == "btc":
        _market = "BTCUSDT"
    else:
        _market = "{}BTC".format(_asset.upper())
    _dc = DepthCrawl(_market, _exchange)
    _vc = VolumeCrawl(_market, _exchange)
    manage_depth_crawling(_dc)
    if _exchange == "binance":
        manage_volume_crawling(_vc)
    return [
        Schedule(_market, '{}1d'.format(_asset), BinanceClient.KLINE_INTERVAL_1DAY,
                 60 * 60 * 8, _exchange, _dc, 20 * 24, _vc),
        Schedule(_market, '{}12h'.format(_asset), BinanceClient.KLINE_INTERVAL_12HOUR, 60 * 60 * 8, _exchange, _dc,
                 20 * 12, _vc),
        Schedule(_market, '{}8h'.format(_asset), BinanceClient.KLINE_INTERVAL_8HOUR, 60 * 60 * 8, _exchange, _dc,
                 20 * 8, _vc),
        Schedule(_market, '{}4h'.format(_asset), BinanceClient.KLINE_INTERVAL_4HOUR, 60 * 60 * 8, _exchange, _dc,
                 20 * 4, _vc),
        Schedule(_market, '{}1h'.format(_asset), BinanceClient.KLINE_INTERVAL_1HOUR, 60 * 60 * 8, _exchange, _dc,
                 20, _vc)
        # Schedule(_market, '{}30m'.format(_asset), BinanceClient.KLINE_INTERVAL_30MINUTE, 60 * 60 * 8, _exchange, _dc,
        #          10, _vc,),
        # Schedule(_market, '{}15m'.format(_asset), BinanceClient.KLINE_INTERVAL_15MINUTE, 60 * 60 * 2, _exchange, _dc,
        #          5, _vc,)
    ]


def get_kucoin_schedules(_asset):
    _exchange = "kucoin"
    _market = "{}-BTC".format(_asset.upper())
    _dc = DepthCrawl(_market, _exchange)
    manage_depth_crawling(_dc)
    return [
        # Schedule(_market, '{}1d'.format(_asset), '1day', 60 * 60 * 23, _exchange, _dc, 20 * 24),
        # Schedule(_market, '{}12h'.format(_asset), '12hour', 60 * 60 * 12, _exchange, _dc, 20 * 12),
        # Schedule(_market, '{}8h'.format(_asset), '8hour', 60 * 60 * 8, _exchange, _dc, 20 * 8),
        # Schedule(_market, '{}4h'.format(_asset), '4hour', 60 * 60 * 8, _exchange, _dc, 20 * 4),
        # Schedule(_market, '{}1h'.format(_asset), '1hour', 60 * 60 * 8, _exchange, _dc, 20),
        Schedule(_market, '{}30m'.format(_asset), '30min', 60 * 60 * 8, _exchange, _dc, 10),
        Schedule(_market, '{}15m'.format(_asset), '15min', 60 * 60 * 8, _exchange, _dc, 5),
    ]


schedules = get_binance_schedules("btc")
schedules.extend(get_binance_schedules("coti"))
schedules.extend(get_binance_schedules("vet"))
schedules.extend(get_binance_schedules("sxp"))
schedules.extend(get_binance_schedules("dot"))
schedules.extend(get_binance_schedules("xem"))
schedules.extend(get_binance_schedules("bzrx"))
schedules.extend(get_binance_schedules("omg"))
schedules.extend(get_binance_schedules("ftm"))
schedules.extend(get_binance_schedules("bnb"))
schedules.extend(get_binance_schedules("ltc"))
schedules.extend(get_binance_schedules("wnxm"))
schedules.extend(get_binance_schedules("trx"))
schedules.extend(get_binance_schedules("srm"))
schedules.extend(get_binance_schedules("crv"))
schedules.extend(get_binance_schedules("band"))
schedules.extend(get_binance_schedules("trb"))
schedules.extend(get_binance_schedules("neo"))
schedules.extend(get_binance_schedules("xtz"))
schedules.extend(get_binance_schedules("zil"))
schedules.extend(get_binance_schedules("ren"))
schedules.extend(get_binance_schedules("fet"))
schedules.extend(get_binance_schedules("ocean"))
schedules.extend(get_binance_schedules("sc"))
schedules.extend(get_binance_schedules("rune"))
schedules.extend(get_binance_schedules("eth"))
schedules.extend(get_binance_schedules("theta"))
schedules.extend(get_binance_schedules("tomo"))
schedules.extend(get_binance_schedules("dgb"))
schedules.extend(get_binance_schedules("doge"))
schedules.extend(get_binance_schedules("sushi"))
schedules.extend(get_binance_schedules("sand"))
schedules.extend(get_binance_schedules("rsr"))
schedules.extend(get_binance_schedules("vidt"))
schedules.extend(get_binance_schedules("1inch"))
schedules.extend(get_binance_schedules("axs"))
schedules.extend(get_binance_schedules("audio"))
schedules.extend(get_binance_schedules("utk"))
schedules.extend(get_binance_schedules("chr"))
schedules.extend(get_binance_schedules("ankr"))
schedules.extend(get_binance_schedules("akro"))
schedules.extend(get_binance_schedules("ogn"))
schedules.extend(get_binance_schedules("xlm"))
schedules.extend(get_binance_schedules("ada"))
schedules.extend(get_binance_schedules("bch"))
schedules.extend(get_binance_schedules("inj"))
schedules.extend(get_binance_schedules("egld"))
schedules.extend(get_binance_schedules("iota"))
schedules.extend(get_binance_schedules("iotx"))
schedules.extend(get_binance_schedules("grt"))
schedules.extend(get_binance_schedules("waves"))
schedules.extend(get_binance_schedules("snx"))
schedules.extend(get_binance_schedules("aave"))
schedules.extend(get_binance_schedules("dia"))
schedules.extend(get_binance_schedules("tfuel"))
schedules.extend(get_binance_schedules("xvs"))
schedules.extend(get_binance_schedules("yfi"))
schedules.extend(get_binance_schedules("dock"))
schedules.extend(get_binance_schedules("perl"))
schedules.extend(get_binance_schedules("sun"))
schedules.extend(get_binance_schedules("lina"))
schedules.extend(get_binance_schedules("audio"))
schedules.extend(get_binance_schedules("idex"))
schedules.extend(get_binance_schedules("alice"))
schedules.extend(get_binance_schedules("chz"))
schedules.extend(get_binance_schedules("om"))
schedules.extend(get_binance_schedules("dego"))
schedules.extend(get_binance_schedules("front"))
schedules.extend(get_binance_schedules("super"))
schedules.extend(get_binance_schedules("lit"))

# schedules.extend(get_kucoin_schedules("bepro"))
# schedules.extend(get_kucoin_schedules("dag"))
# schedules.extend(get_kucoin_schedules("vra"))
# schedules.extend(get_kucoin_schedules("loki"))
# schedules.extend(get_kucoin_schedules("tel"))
# schedules.extend(get_kucoin_schedules("soul"))
# schedules.extend(get_kucoin_schedules("ewt"))
# schedules.extend(get_kucoin_schedules("nwc"))
# schedules.extend(get_kucoin_schedules("cro"))

manage_crawling(schedules)
