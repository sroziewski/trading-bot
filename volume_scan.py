import datetime
import functools
import sys
import threading
from time import sleep

import numpy as np
import schedule
from binance.websockets import BinanceSocketManager
from bson import CodecOptions
from bson.codec_options import TypeRegistry
from pymongo import DESCENDING

from library import DecimalCodec, TradeMsg, get_time_from_binance_tmstmp, get_time, round_price_s
from library import MakerVolumeUnit, TakerVolumeUnit, setup_logger, VolumeContainer, add_volume_containers, \
    lib_initialize, get_binance_obj
from mongodb import mongo_client

trades = {}
volumes = {}
locker = {}
volumes15 = {}
initialization = {}
part = sys.argv[1]  # 1, 2

logger = setup_logger("Binance-Volume-Scanner-{}".format(part))

db_volume = mongo_client.volume

decimal_codec = DecimalCodec()
type_registry = TypeRegistry([decimal_codec])
codec_options = CodecOptions(type_registry=type_registry)


class VolumeCrawl(object):
    def __init__(self, _market):
        self.market = _market


def process_trade_socket_message(_msg):
    try:
        _trade_msg = TradeMsg(_msg)
    except Exception:
        logger.error(_msg)
    # logger.info("{} : {}".format(_trade_msg.timestamp_str, _trade_msg.quantity))
    while "lock" in trades[_trade_msg.market]:
        sleep(1)
    trades[_trade_msg.market].append(_trade_msg)  # add lock here


def merge_volumes(_market):
    _merged = []

    for _v in volumes[_market].values():
        if len(_v) == 1:
            _merged.append(_v[0])
        elif len(_v) == 2:
            _merged.append(add_volume_containers(_v[0], _v[1]))
        elif len(_v) == 3:
            _r = add_volume_containers(_v[0], _v[1])
            _merged.append(add_volume_containers(_r, _v[2]))
    return _merged


def is_empty_volume(_vc: VolumeContainer):
    return not _vc or _vc.maker_volume.base_volume == 0.0 or _vc.taker_volume.base_volume == 0.0


def store_to_mongo(_counter, _volume_collection, _vc: VolumeContainer):
    if _counter > 5:
        try:
            _volume_collection.insert_one(to_mongo(_vc))
        except Exception as e:
            logger.exception(e.__traceback__)
            logger.error("WRITE ERROR --> {} {}".format(_vc.market, _vc.start_time_str))
        return
    try:
        _volume_collection.insert_one(to_mongo(_vc))
    except Exception as e:
        logger.warning("store_to_mongo: {}".format(_counter))
        sleep(5)
        store_to_mongo(_counter + 1, _volume_collection, _vc)


def post_process_volume_container(_vc: VolumeContainer):
    # if is_not_initialized(_vc.market):
    #     return
    handle_volume_container(_vc)
    try:
        _vc.start_time = _vc.maker_volume.timestamp
        _vc.start_time_str = get_time(_vc.maker_volume.timestamp)
    except Exception as e:
        logger.info("_vc.buy_volume.timestamp = {}".format(get_time_from_binance_tmstmp(_vc.maker_volume.timestamp)))
        logger.exception(e.__traceback__)

    volume_collection = db_volume.get_collection(_vc.market.lower(), codec_options=codec_options)

    _vc.ticker = '15m'
    _vc.round()
    store_to_mongo(0, volume_collection, _vc)
    # logger.info("Volume market {}_{} has been written to volume {} collection -- base_volume: {}, quantity: {}".format(_vc.market.lower(),
    #                                                                                       _vc.ticker.lower(),
    #                                                                                       volume_collection.name.upper(),
    #                                                                                       _vc.total_base_volume,
    #                                                                                       _vc.total_quantity))


def handle_volume_container(_vc: VolumeContainer):
    try:
        _vc.avg_weighted_maker_price = _vc.maker_volume.avg_price
        _vc.avg_weighted_taker_price = _vc.taker_volume.avg_price
        _vc.mean_price = (_vc.maker_volume.mean_price + _vc.taker_volume.mean_price) / 2 if _vc.maker_volume.mean_price > 0 and _vc.taker_volume.mean_price > 0 else max(_vc.maker_volume.mean_price, _vc.taker_volume.mean_price)
        _vc.total_quantity = _vc.maker_volume.quantity + _vc.taker_volume.quantity
        _vc.total_base_volume = _vc.maker_volume.base_volume + _vc.taker_volume.base_volume
        _vc.avg_price = round(_vc.total_base_volume / _vc.total_quantity, 8) if _vc.total_quantity > 0.0 else 0.0

        return _vc
    except AttributeError:
        logger.error("AttributeError {} total_quantity: {} total_base_volume: {} mean_price: {} avg_price: {}".format(_vc.market, _vc.total_quantity, _vc.total_base_volume, _vc.mean_price, _vc.avg_price))
        pass


def is_not_initialized(_market):
    return _market not in initialization


def handle_volume_containers_5m(_market):
    locker[_market] = True
    if len(volumes[_market]) < 5:
        del locker[_market]
        return
    _merged = merge_volumes(_market)
    if len(_merged) < 5:
        _merged.clear()
        del locker[_market]
        return
    # we are doing once in 5m
    _start_time0 = int(_merged[0].start_time)
    _start_time4 = int(_merged[4].start_time)
    if _start_time0 % 5 != 0:
        if _market not in initialization:
            initialization[_market] = 1
        else:
            initialization[_market] += 1
        del volumes[_market][_merged[0].start_time]
        if len(volumes[_market].keys()) < 5:
            sleep(1)
        return handle_volume_containers_5m(_market)
    if _start_time4 % 5 == 4:
        _keys = volumes[_market].keys()
        # print("{} {}".format(_start_time0, _start_time4))
        post_process_volume_container(functools.reduce(lambda x, y: add_volume_containers(x, y), _merged[0:5]))
        if len(_merged) > 5 and int(_merged[5].start_time) % 5 == 0:
            _keys = list(volumes[_market].keys())[:-1]
            for _k in _keys:
                del volumes[_market][_k]
        else:
            del volumes[_market]
    if len(_merged) > 4 and _start_time4 % 5 != 4:
        _res = []
        for _el in _merged[0:5]:
            if len(_res) == 0:
                _res.append(_el)
            elif int(_el.start_time) % 5 - int(_merged[-1].start_time) % 5 > 0:
                _res.append(_el)
        post_process_volume_container(functools.reduce(lambda x, y: add_volume_containers(x, y), _res))
        if len(_merged) > 5 and int(_merged[5].start_time) % 5 == 0:
            _keys = list(volumes[_market].keys())[:-1]
            for _k in _keys:
                del volumes[_market][_k]
        else:
            del volumes[_market]

    del locker[_market]
    _merged.clear()


def handle_volume_containers(_message):
    _market = _message['market']
    locker[_market] = True
    _merged = merge_volumes(_market)
    if len(_merged) == 0:
        del locker[_market]
        return
    _server_time = datetime.datetime.now().timestamp()
    _server_time_str = get_time(_server_time)
    _min = int(_server_time_str.split(":")[-2])
    _quarter = int(_min / 15)
    _persist = None

    if _market not in volumes15:
        volumes15[_market] = {}
        volumes15[_market]['quarter'] = _quarter
        volumes15[_market]['vc'] = None
    elif volumes15[_market]['quarter'] != _quarter:
        _persist = True
    _rc = None
    _stop = None
    _message['done'] = True

    _to_add = True

    if len(_merged) == 1:
        _entry_quarter = int(int(_merged[0].start_time) / 15)
        # logger.info("quarter: {} _entry_quarter: {}".format(volumes15[_market]['quarter'], _entry_quarter))
        _rc = _merged[0]
        set_rc_timestamp(_entry_quarter, _rc, _server_time, _server_time_str)

        if _entry_quarter != volumes15[_market]['quarter']:
            _stop = 0
            _rc = volumes15[_market]['vc']
            _to_add = False
            _persist = True

    if len(_merged) == 2:
        _entry_quarter_0 = int(int(_merged[0].start_time) / 15)
        _entry_quarter_1 = int(int(_merged[1].start_time) / 15)
        # logger.info("quarter: {} _entry_quarter_0: {} _entry_quarter_1: {}".format(volumes15[_market]['quarter'], _entry_quarter_0, _entry_quarter_1))
        if _entry_quarter_0 == volumes15[_market]['quarter'] and _entry_quarter_1 == volumes15[_market]['quarter']:
            _rc: VolumeContainer = add_volume_containers(_merged[0], _merged[1])
            set_rc_timestamp(_entry_quarter_0, _rc, _server_time, _server_time_str)
        if _entry_quarter_0 == volumes15[_market]['quarter'] and _entry_quarter_1 != volumes15[_market]['quarter']:
            _stop = 1
            _rc = _merged[0]
            set_rc_timestamp(_entry_quarter_0, _rc, _server_time, _server_time_str)
            _persist = True
        if _entry_quarter_0 != volumes15[_market]['quarter'] and _entry_quarter_1 != volumes15[_market]['quarter']:
            _stop = 0
            _rc = volumes15[_market]['vc']
            _to_add = False
            _persist = True
        if _entry_quarter_0 != volumes15[_market]['quarter'] and _entry_quarter_1 == volumes15[_market]['quarter']:  #  a new quarter (not persisting)
            _rc = _merged[1]
            set_rc_timestamp(_entry_quarter_1, _rc, _server_time, _server_time_str)

    if len(_merged) == 3:
        _entry_quarter_0 = int(int(_merged[0].start_time) / 15)
        _entry_quarter_1 = int(int(_merged[1].start_time) / 15)
        _entry_quarter_2 = int(int(_merged[2].start_time) / 15)
        if _entry_quarter_0 == volumes15[_market]['quarter'] and _entry_quarter_1 == volumes15[_market]['quarter'] and _entry_quarter_2 == volumes15[_market]['quarter']:
            _rc0: VolumeContainer = add_volume_containers(_merged[0], _merged[1])
            _rc: VolumeContainer = add_volume_containers(_rc0, _merged[2])
            set_rc_timestamp(_entry_quarter_0, _rc, _server_time, _server_time_str)
        if _entry_quarter_0 == volumes15[_market]['quarter'] and _entry_quarter_1 == volumes15[_market]['quarter'] and _entry_quarter_2 != volumes15[_market]['quarter']:
            _stop = 2
            _rc: VolumeContainer = add_volume_containers(_merged[0], _merged[1])
            set_rc_timestamp(_entry_quarter_0, _rc, _server_time, _server_time_str)
            _persist = True
        if _entry_quarter_0 == volumes15[_market]['quarter'] and _entry_quarter_1 != volumes15[_market]['quarter'] and _entry_quarter_2 != volumes15[_market]['quarter']:
            _stop = 1
            _rc = _merged[0]
            set_rc_timestamp(_entry_quarter_0, _rc, _server_time, _server_time_str)
            _persist = True
        if _entry_quarter_0 != volumes15[_market]['quarter'] and _entry_quarter_1 != volumes15[_market]['quarter'] and _entry_quarter_2 != volumes15[_market]['quarter']:
            _stop = 0
            _rc = volumes15[_market]['vc']
            _to_add = False
            _persist = True
        if _entry_quarter_0 != volumes15[_market]['quarter'] and _entry_quarter_1 == volumes15[_market]['quarter'] and _entry_quarter_2 == volumes15[_market]['quarter']:
            _rc: VolumeContainer = add_volume_containers(_merged[1], _merged[2])
            set_rc_timestamp(_entry_quarter_1, _rc, _server_time, _server_time_str)
        if _entry_quarter_0 != volumes15[_market]['quarter'] and _entry_quarter_1 != volumes15[_market]['quarter'] and _entry_quarter_2 == volumes15[_market]['quarter']:
            _rc = _merged[2]
            set_rc_timestamp(_entry_quarter_2, _rc, _server_time, _server_time_str)

    if volumes15[_market]['vc'] is None:
        volumes15[_market]['vc'] = _rc
    else:
        if _to_add:
            _rc = add_volume_containers(volumes15[_market]['vc'], _rc)
            volumes15[_market]['vc'] = _rc

    if _persist:
        try:
            post_process_volume_container(_rc)
            del volumes15[_market]
        except AttributeError:
            pass
        if _market not in initialization:
            initialization[_market] = 1
    del volumes[_market]
    if str(_stop) != "None" and _stop >= 0:
        volumes[_market] = {}
        for _el in _merged[_stop:]:
            volumes[_market][_el.start_time] = [_el]
    del locker[_market]
    # try:
    #     _rc.print()
    # except AttributeError:
    #     i = 1
    _merged.clear()


def set_rc_timestamp(_entry_quarter_0, _rc, _server_time, _server_time_str):
    _sec = int(_server_time_str.split(":")[-1])
    _current_candle_timestamp = _server_time - ((
            int(_server_time_str.split(":")[-2]) - _entry_quarter_0 * 15) * 60 + _sec)
    _rc.maker_volume.timestamp = _current_candle_timestamp
    _rc.taker_volume.timestamp = _current_candle_timestamp


def process_volume():
    _volume_ticker = '5m'

    _markets = list(trades.keys())

    _bag = {}

    for _market in _markets:
        _message = {}
        _message['market'] = _market
        _message['done'] = None
        while _market in locker:
            sleep(1)
        trades[_market].append("lock")
        _bag[_market] = trades[_market].copy()
        _lock_index = len(_bag[_market]) - _bag[_market].index("lock")
        _bag[_market] = _bag[_market][:-_lock_index]  # we skip the lock element
        _aggs = aggregate_by_minute(_bag[_market])
        _vcl = []
        if _market not in volumes:
            volumes[_market] = {}
        for _k, _v in _aggs.items():
            # logger.info("_k: {}".format(_k))
            _maker_volume = filter(lambda x: x.maker, _v)
            _taker_volume = filter(lambda x: x.taker, _v)
            _mv = MakerVolumeUnit(_maker_volume)
            _tv = TakerVolumeUnit(_taker_volume)
            if _mv.timestamp == 0:
                _mv.timestamp = _tv.timestamp
            if _tv.timestamp == 0:
                _tv.timestamp = _mv.timestamp

            if _mv.quantity == 0 or _tv.quantity == 0:
                sdf=1

            # logger.info("maker {} : {}".format(get_time_from_binance_tmstmp(_mv.timestamp), _mv.quantity))
            # logger.info("taker {} : {}".format(get_time_from_binance_tmstmp(_tv.timestamp), _tv.quantity))

            if _k not in volumes[_market]:
                try:
                    volumes[_market][_k] = [VolumeContainer(_market, _volume_ticker, _k, _mv, _tv)]
                except Exception as e:
                    pass
            else:
                volumes[_market][_k].append(VolumeContainer(_market, _volume_ticker, _k, _mv, _tv))

        if len(_aggs.items()) > 0 or all(x == "lock" for x in trades[_market]):
            trades[_market].clear()
            handle_volume_containers(_message)
            if not _message['done']:
                trades[_market] = _bag[_market].copy()
        _bag[_market].clear()


def aggregate_by_minute(_list):
    _aggs = {}
    if all(x == "lock" for x in _list):
        return _aggs
    for _el in _list:
        if _el.timestamp_str.split(":")[-2] not in _aggs:
            _aggs[_el.timestamp_str.split(":")[-2]] = []
        _aggs[_el.timestamp_str.split(":")[-2]].append(_el)
    return _aggs


def _do_volume_scan(_vc: VolumeCrawl):
    logger.info("Start scanning market {}".format(_vc.market))
    trades[_vc.market] = []
    _bm = BinanceSocketManager(get_binance_obj().client)
    _conn_key = _bm.start_aggtrade_socket(_vc.market, process_trade_socket_message)
    _bm.start()


def manage_volume_scan(_vc):
    _crawler = threading.Thread(target=_do_volume_scan, args=(_vc,),
                                name='_do_volume_crawl : {}'.format(_vc.market))
    _crawler.start()



def to_mongo(_vc: VolumeContainer):  # _volume_container
    return {
        'market': _vc.market,
        'ticker': _vc.ticker,
        'start_time': int(_vc.start_time),
        'start_time_str': _vc.start_time_str,
        'total_base_volume': _vc.total_base_volume,
        'total_quantity': _vc.total_quantity,
        'avg_weighted_maker_price': round_price_s(_vc.avg_weighted_maker_price),
        'avg_weighted_taker_price': round_price_s(_vc.avg_weighted_taker_price),
        'avg_price': round_price_s(_vc.avg_price),
        'mean_price': round_price_s(_vc.mean_price),
        'maker_volume': {
            'base_volume': _vc.maker_volume.base_volume,
            'quantity': _vc.maker_volume.quantity,
            'avg_price': round_price_s(_vc.maker_volume.avg_price),
            'mean_price': round_price_s(_vc.maker_volume.mean_price),
            '0_236k': _vc.maker_volume.n236,
            '0_618k': _vc.maker_volume.n618,
            '1k': _vc.maker_volume.n1000,
            '2_618k': _vc.maker_volume.n2618,
            '4_236k': _vc.maker_volume.n4236,
            '6_180k': _vc.maker_volume.n6180,
            '10k': _vc.maker_volume.l01,
            '23_6k': _vc.maker_volume.l02,
            '38_2k': _vc.maker_volume.l0236,
            '50k': _vc.maker_volume.l0382,
            '61_8k': _vc.maker_volume.l05,
            '78_6k': _vc.maker_volume.l0618,
            '100k': _vc.maker_volume.l0786,
            '138k': _vc.maker_volume.l1,
            '162k': _vc.maker_volume.l1382,
            '200k': _vc.maker_volume.l162,
            '500k': _vc.maker_volume.l2,
            '1M': _vc.maker_volume.l5,
            '2M': _vc.maker_volume.l10,
            '5M': _vc.maker_volume.l20,
            '10M': _vc.maker_volume.l50,
            '10M+': _vc.maker_volume.l100
        },
        'taker_volume': {
            'base_volume': _vc.taker_volume.base_volume,
            'quantity': _vc.taker_volume.quantity,
            'avg_price': round_price_s(_vc.taker_volume.avg_price),
            'mean_price': round_price_s(_vc.taker_volume.mean_price),
            '0_236k': _vc.taker_volume.n236,
            '0_618k': _vc.taker_volume.n618,
            '1k': _vc.taker_volume.n1000,
            '2_618k': _vc.taker_volume.n2618,
            '4_236k': _vc.taker_volume.n4236,
            '6_180k': _vc.taker_volume.n6180,
            '10k': _vc.taker_volume.l01,
            '23_6k': _vc.taker_volume.l02,
            '38_2k': _vc.taker_volume.l0236,
            '50k': _vc.taker_volume.l0382,
            '61_8k': _vc.taker_volume.l05,
            '78_6k': _vc.taker_volume.l0618,
            '100k': _vc.taker_volume.l0786,
            '138k': _vc.taker_volume.l1,
            '162k': _vc.taker_volume.l1382,
            '200k': _vc.taker_volume.l162,
            '500k': _vc.taker_volume.l2,
            '1M': _vc.taker_volume.l5,
            '2M': _vc.taker_volume.l10,
            '5M': _vc.taker_volume.l20,
            '10M': _vc.taker_volume.l50,
            '10M+': _vc.taker_volume.l100
        }
    }


market_type = "usdt"
db_markets_info = mongo_client.markets_info
usdt_markets_collection = db_markets_info.get_collection(market_type, codec_options=codec_options)
restart = {}


def get_market_info_list(_l):
    if part != "all" and int(part) in [1, 2]:
        return np.array_split(_l, 2)[int(part) - 1]
    elif part == "all":
        return _l


def check_markets():
    _el = usdt_markets_collection.find_one(sort=[('_id', DESCENDING)])
    _market_name_tmp = "{}{}".format(_el['name'], market_type).upper()
    if _market_name_tmp not in trades:
        _market_info_cursor = usdt_markets_collection.find()
        _market_info_list = [e for e in _market_info_cursor]
        for __market_s in _market_info_list:
            _market_name = "{}{}".format(__market_s['name'], market_type).upper()
            if _market_name not in trades:
                __vc = VolumeCrawl(_market_name)
                manage_volume_scan(__vc)
                logger.info("New market: {} added to volume scan...".format(_market_name))


def check_markets_scan():
    _market_info_cursor = usdt_markets_collection.find()
    _market_info_list = get_market_info_list([e for e in _market_info_cursor])
    for __market_s in _market_info_list:
        _market_name = "{}{}".format(__market_s['name'], market_type).lower()
        if _market_name in restart:
            sleep(16*60)
        _market_collection_s = db_volume.get_collection(_market_name, codec_options=codec_options)
        _el = _market_collection_s.find_one(sort=[('_id', DESCENDING)])
        _now = datetime.datetime.now().timestamp()
        if _el is None or _now - _el['start_time'] >= 45 * 60:
            _market_name_up = _market_name.upper()
            if _market_name_up in trades:
                del trades[_market_name_up]
            if _market_name_up in initialization:
                del initialization[_market_name_up]
            if _market_name_up in locker:
                del locker[_market_name_up]
            if _market_name_up in volumes15:
                del volumes15[_market_name_up]
            if _market_name_up in volumes:
                del volumes[_market_name_up]
            _vc = VolumeCrawl("{}".format(_market_name_up))
            manage_volume_scan(_vc)
            restart[_vc.market.lower()] = True
            logger.warning("Market {} restarted...".format(_vc.market))


def handle_init():
    _market_info_cursor = usdt_markets_collection.find()
    _market_info_list = get_market_info_list([e for e in _market_info_cursor])

    lib_initialize()
    __ii = 1
    for _market_s in _market_info_list:  # inf loop needed here
        _market = "{}{}".format(_market_s['name'], market_type).upper()
        _vc = VolumeCrawl(_market)
        manage_volume_scan(_vc)
        logger.info("{} {} scanning...".format(__ii, _market))
        __ii += 1


handle_init()

schedule.every(1).minutes.do(process_volume)
# schedule.every(60).minutes.do(check_markets)
# schedule.every(21).minutes.do(check_markets_scan)

while True:
    # Checks whether a scheduled task
    # is pending to run or not
    schedule.run_pending()
    sleep(1)
