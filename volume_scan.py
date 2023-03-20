import datetime
import functools
import threading
from time import sleep

import schedule

from binance.websockets import BinanceSocketManager
from bson import CodecOptions
from bson.codec_options import TypeRegistry

from library import TradeMsg, MakerVolumeUnit, TakerVolumeUnit, setup_logger, VolumeContainer, add_volume_containers

from library import get_binance_klines, get_binance_interval_unit, get_kucoin_klines, \
    get_kucoin_interval_unit, binance_obj, kucoin_client, DecimalCodec, try_get_klines, TradeMsg, get_last_db_record, \
    get_time_from_binance_tmstmp, logger_global
from market_scanner import VolumeCrawl
from mongodb import mongo_client

trades = {}
volumes = {}
locker = {}
volumes15 = {}
initialization = {}

logger = setup_logger("Binance-Volume-Scanner")

db_volume = mongo_client.volume

decimal_codec = DecimalCodec()
type_registry = TypeRegistry([decimal_codec])
codec_options = CodecOptions(type_registry=type_registry)


def process_trade_socket_message(_msg):
    _trade_msg = TradeMsg(_msg)
    while "lock" in trades[_trade_msg.market]:
        sleep(1)
    trades[_trade_msg.market].append(_trade_msg)  # add lock here
    last_tmstmp = datetime.datetime.now().timestamp()


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


def post_process_volume_container(_vc: VolumeContainer):
    if is_not_initialized(_vc.market):
        return
    handle_volume_container(_vc)
    try:
        _second = int(get_time_from_binance_tmstmp(_vc.maker_volume.timestamp).split(":")[-1])
        _vc.start_time = _vc.maker_volume.timestamp - 1000 * _second
        _vc.start_time_str = get_time_from_binance_tmstmp(_vc.maker_volume.timestamp - 1000 * _second)
    except Exception as e:
        logger.info("_vc.buy_volume.timestamp = {}".format(get_time_from_binance_tmstmp(_vc.maker_volume.timestamp)))
        logger.exception(e.__traceback__)

    volume_collection = db_volume.get_collection(_vc.market.lower(), codec_options=codec_options)
    if _vc.market == "BTCUSDT":
        volume_collection.insert_one(to_mongo(_vc))
        logger.info("Volume market {}_{} has been written to volume {} collection".format(_vc.market.lower(),
                                                                                          _vc.ticker.lower(),
                                                                                          volume_collection.name.upper()))
    else:
        _vc.ticker = '15m'
        volume_collection.insert_one(to_mongo(_vc))
        logger.info("Volume market {}_{} has been written to volume {} collection -- base_volume: {}, quantity: {}".format(_vc.market.lower(),
                                                                                          _vc.ticker.lower(),
                                                                                          volume_collection.name.upper(),
                                                                                          _vc.total_base_volume,
                                                                                          _vc.total_quantity))
        # if _vc.market not in volumes15:
        #     try:
        #         _minutes = int(_vc.start_time_str.split(":")[-2])
        #         volumes15[_vc.market] = []
        #         if _minutes % 15 == 0:
        #             volumes15[_vc.market].append(_vc)
        #     except AttributeError as e:
        #         logger.info("_minutes= {}".format(_vc.start_time_str))
        #         logger.exception(e.__traceback__)
        # else:
        #     volumes15[_vc.market].append(_vc)
        # if _vc.market in volumes15 and len(volumes15[_vc.market]) == 3:
        #     _vc = handle_volume_container(functools.reduce(lambda x, y: add_volume_containers(x, y), volumes15[_vc.market][0:3]))
        #     _vc.ticker = '15m'
        #     volume_collection.insert_one(to_mongo(_vc))
        #     logger.info("Volume market {}_{} has been written to volume {} collection".format(_vc.market.lower(),
        #                                                                                       _vc.ticker.lower(),
        #                                                                                       volume_collection.name.upper()))
        #     del volumes15[_vc.market]
    kk = 1


def handle_volume_container(_vc: VolumeContainer):
    try:
        _vc.avg_weighted_maker_price = _vc.maker_volume.avg_price
        _vc.avg_weighted_taker_price = _vc.taker_volume.avg_price
        _vc.mean_price = (_vc.maker_volume.mean_price + _vc.taker_volume.mean_price) / 2
        _vc.total_quantity = _vc.maker_volume.quantity + _vc.taker_volume.quantity
        _vc.total_base_volume = _vc.maker_volume.base_volume + _vc.taker_volume.base_volume
        _vc.avg_price = round(_vc.total_base_volume / _vc.total_quantity, 8) if _vc.total_quantity > 0.0 else 0.0

        return _vc
    except AttributeError:
        iiik = 1
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
    # if len(_merged) < 2:
    #     del locker[_market]
    #     _merged.clear()
    #     return
    _server_time = _merged[0].taker_volume.timestamp
    _server_time_str = get_time_from_binance_tmstmp(_server_time)
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
    if len(_merged) == 1:
        _entry_quarter = int(int(_merged[0].start_time) / 15)
        _current_candle_timestamp = _server_time - (
                    int(_server_time_str.split(":")[-2]) - _entry_quarter * 15) * 60 * 1000
        _rc = _merged[0]
        _rc.maker_volume.timestamp = _current_candle_timestamp
        _rc.taker_volume.timestamp = _current_candle_timestamp

    for _i in range(len(_merged) - 1):
        _entry_quarter = int(int(_merged[_i].start_time) / 15)
        if _entry_quarter == volumes15[_market]['quarter']:
            _rc: VolumeContainer = add_volume_containers(_merged[_i], _merged[_i + 1])
            _current_candle_timestamp = _server_time - (int(_server_time_str.split(":")[-2]) - _entry_quarter * 15) * 60 * 1000
            _rc.maker_volume.timestamp = _current_candle_timestamp
            _rc.taker_volume.timestamp = _current_candle_timestamp
        else:
            _stop = _i
            if _i == 0:
                _rc = volumes15[_market]['vc']
            logger.info("Stop: {} {}".format(_stop, _market))
            _persist = True
            break

    if volumes15[_market]['vc'] is None:
        volumes15[_market]['vc'] = _rc
    else:
        _rc = add_volume_containers(volumes15[_market]['vc'], _rc)

    if _persist:
        try:
            post_process_volume_container(_rc)
            del volumes15[_market]
        except AttributeError:
            asdasd = 123
            pass
        if _market not in initialization:
            initialization[_market] = 1
    del volumes[_market]
    if _stop:
        if _stop == 0:
            k = 1
        volumes[_market] = _merged[_stop:]
    del locker[_market]
    _merged.clear()
    # if not is_empty_volume(_rc):
    try:
        _rc.print()
    except AttributeError:
        asdasd=1
        pass


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
        _bag[_market] = _bag[_market][:-1]  # we skip the lock element
        _aggs = aggregate_by_minute(_bag[_market])
        _vcl = []
        if _market not in volumes:
            volumes[_market] = {}
        for _k, _v in _aggs.items():
            _maker_volume = filter(lambda x: x.maker, _v)
            _taker_volume = filter(lambda x: x.taker, _v)
            _mv = MakerVolumeUnit(_maker_volume)
            _tv = TakerVolumeUnit(_taker_volume)
            if _mv.timestamp == 0:
                _mv.timestamp = _tv.timestamp
            if _tv.timestamp == 0:
                _tv.timestamp = _mv.timestamp
            if _k not in volumes[_market]:
                try:
                    volumes[_market][_k] = [VolumeContainer(_market, _volume_ticker, _k, _mv, _tv)]
                except Exception:
                    jh=1
                    pass
            else:
                volumes[_market][_k].append(VolumeContainer(_market, _volume_ticker, _k, _mv, _tv))
        if len(_aggs.items()) > 0:
            trades[_market].clear()
            if _market == "BTCUSDT":
                handle_volume_containers_5m(_market)
            else:
                handle_volume_containers(_message)
                if not _message['done']:
                    trades[_market] = _bag[_market].copy()
        elif len(trades[_market]) > 0:
            trades[_market] = _bag[_market].copy()
        _bag[_market].clear()


def aggregate_by_minute(_list):
    _aggs = {}
    if len(_list) == 1 and _list[0] == "lock":
        return _aggs
    for _el in _list:
        if _el.timestamp_str.split(":")[-2] not in _aggs:
            _aggs[_el.timestamp_str.split(":")[-2]] = []
        _aggs[_el.timestamp_str.split(":")[-2]].append(_el)
    return _aggs


def _do_volume_scan(_vc: VolumeCrawl):
    logger.info("Start scanning market {}".format(_vc.market))
    trades[_vc.market] = []
    last_tmstmp = datetime.datetime.now().timestamp()
    _bm = BinanceSocketManager(binance_obj.client)
    _conn_key = _bm.start_aggtrade_socket(_vc.market, process_trade_socket_message)
    _bm.start()
    # while True:
    #     _tmstmp_diff = datetime.datetime.now().timestamp() - last_tmstmp
    #     # if _tmstmp_diff > 60 * 60:
    #     #     logger_global[0].warning(
    #     #         f"{_vc.market} last trading volume tmstmp ({last_tmstmp}) is older than 60 minutes, diff = {_tmstmp_diff}")
    #     if len(trades[_vc.market]) > 0:
    #         logger_global[0].info(f"{_vc.market} last trading volume : {trades[_vc.market][-1].timestamp_str} {_vc.market}")
    #     sleep(60 * 60)


def manage_volume_scan(_vc):
    _crawler = threading.Thread(target=_do_volume_scan, args=(_vc,),
                                name='_do_volume_crawl : {}'.format(_vc.market))
    _crawler.start()


def to_mongo(_vc: VolumeContainer):  # _volume_container
    return {
        'market': _vc.market,
        'ticker': _vc.ticker,
        'start_time': _vc.start_time,
        'start_time_str': _vc.start_time_str,
        'total_base_volume': _vc.total_base_volume,
        'total_quantity': _vc.total_quantity,
        'avg_weighted_maker_price': _vc.avg_weighted_maker_price,
        'avg_weighted_taker_price': _vc.avg_weighted_taker_price,
        'avg_price': _vc.avg_price,
        'mean_price': _vc.mean_price,
        'maker_volume': {
            'base_volume': _vc.maker_volume.base_volume,
            'quantity': _vc.maker_volume.quantity,
            'avg_price': _vc.maker_volume.avg_price,
            'mean_price': _vc.maker_volume.mean_price,
            '5k': _vc.maker_volume.l00,
            '10k': _vc.maker_volume.l01,
            '23_6k': _vc.maker_volume.l02,
            '0-23_6k': _vc.maker_volume.l0,
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
            'avg_price': _vc.taker_volume.avg_price,
            'mean_price': _vc.taker_volume.mean_price,
            '5k': _vc.taker_volume.l00,
            '10k': _vc.taker_volume.l01,
            '23_6k': _vc.taker_volume.l02,
            '0-23_6k': _vc.taker_volume.l0,
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


type = "usdt"

db_markets_info = mongo_client.markets_info
usdt_markets_collection = db_markets_info.get_collection(type, codec_options=codec_options)
market_info_cursor = usdt_markets_collection.find()
market_info_list = [e for e in market_info_cursor]

# for _market_s in market_info_list:  # inf loop needed here
#     _vc = VolumeCrawl("{}{}".format(_market_s['name'], type).upper())
#     manage_volume_scan(_vc)

# manage_volume_scan(VolumeCrawl("BTCUSDT"))
manage_volume_scan(VolumeCrawl("ETHUSDT"))
manage_volume_scan(VolumeCrawl("LTCUSDT"))
manage_volume_scan(VolumeCrawl("BNBUSDT"))
manage_volume_scan(VolumeCrawl("OMGUSDT"))
manage_volume_scan(VolumeCrawl("HOOKUSDT"))
manage_volume_scan(VolumeCrawl("NEARUSDT"))
manage_volume_scan(VolumeCrawl("SANDUSDT"))
manage_volume_scan(VolumeCrawl("OGNBTC"))

schedule.every(1).minutes.do(process_volume)

while True:
    # Checks whether a scheduled task
    # is pending to run or not
    schedule.run_pending()
    sleep(1)
