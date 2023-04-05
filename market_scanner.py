import datetime
import threading
import traceback
from functools import reduce
from random import randrange
from time import sleep

from binance.client import Client as BinanceClient
from binance.exceptions import BinanceAPIException
from bson import CodecOptions
from bson.codec_options import TypeRegistry
from kucoin.exceptions import KucoinAPIException

from depth_crawl import compute_depth_percentages, divide_dc, add_dc, depth_crawl_dict, manage_depth_scan, DepthCrawl
from library import get_binance_klines, get_binance_interval_unit, get_kucoin_klines, \
    get_kucoin_interval_unit, binance_obj, kucoin_client, DecimalCodec, try_get_klines, get_last_db_record, \
    get_time_from_binance_tmstmp, logger_global, save_to_file
from mongodb import mongo_client

db = mongo_client.klines
decimal_codec = DecimalCodec()
type_registry = TypeRegistry([decimal_codec])
codec_options = CodecOptions(type_registry=type_registry)

depth_locker = {}


def to_mongo_binance(_kline):
    _data = to_mongo(_kline)
    _data['buy_btc_volume'] = _kline.buy_btc_volume
    _data['buy_quantity'] = _kline.buy_quantity
    _data['sell_btc_volume'] = _kline.sell_btc_volume
    _data['sell_quantity'] = _kline.sell_quantity
    return _data


def to_mongo(_kline):
    if _kline.bid_depth:
        return {
        'exchange': _kline.exchange,
        'version': "2.0",
        'ticker': _kline.ticker,
        'start_time': int(_kline.start_time/1000),
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
            'p1': _kline.bid_depth.p1,
            'p2': _kline.bid_depth.p2,
            'p3': _kline.bid_depth.p3,
            'p4': _kline.bid_depth.p4,
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
            'p1': _kline.ask_depth.p1,
            'p2': _kline.ask_depth.p2,
            'p3': _kline.ask_depth.p3,
            'p4': _kline.ask_depth.p4,
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
    else:
        return {
            'exchange': _kline.exchange,
            'version': "2.0",
            'ticker': _kline.ticker,
            'start_time': int(_kline.start_time / 1000),
            'opening': _kline.opening,
            'closing': _kline.closing,
            'lowest': _kline.lowest,
            'highest': _kline.highest,
            'volume': round(_kline.volume, 4),
            'btc_volume': round(_kline.btc_volume, 4),
            'time_str': _kline.time_str,
            'market': _kline.market,
            'bid_price': None,
            'ask_price': None,
            'bid_depth': None,
            'ask_depth': None
        }


def persist_kline(_kline, _collection):
    try:
        if _kline.exchange == "binance":
            _collection.insert_one({'kline': to_mongo(_kline), 'timestamp': _kline.start_time,
                                    'timestamp_str': get_time_from_binance_tmstmp(_kline.start_time)})
        else:
            _collection.insert_one({'kline': to_mongo(_kline), 'timestamp': _kline.start_time})
    except Exception as err:
        traceback.print_tb(err.__traceback__)
        logger_global[0].exception("{} {}".format(_kline['market'], err.__traceback__))
        sleep(15)
        persist_kline(_kline, _collection)


def filter_current_klines(_klines, _collection_name, _collection):
    _last_record = get_last_db_record(_collection)
    logger_global[0].info(
        "{} : last kline : {} ".format(_collection_name, _last_record['kline']['time_str'] if _last_record else "None"))
    _out = None
    if _last_record:
        _out = list(filter(lambda x: int(x.start_time) > int(_last_record['timestamp']), _klines))
    else:
        _out = _klines
        logger_global[0].info("{} : first kline : {} ".format(_collection_name, _out[0].time_str if _out else "None"))
    return _out


def persist_klines(_klines, _collection):
    return list(map(lambda x: persist_kline(x, _collection), _klines))


class Schedule(object):
    def __init__(self, _asset, _market, _collection_name, _ticker, _sleep, _exchange, _dc, _journal, _no_such_market=False):
        self.asset = _asset
        self.market = _market
        self.collection_name = _collection_name
        self.ticker = _ticker
        self.sleep = _sleep
        self.exchange = _exchange
        self.depth_crawl = _dc
        self.journal = _journal
        self.no_such_market = _no_such_market


def manage_crawling(_schedule):
    sleep(5)
    _scheduler = threading.Thread(target=_do_schedule, args=(_schedule,),
                                  name='_do_schedule : {}'.format(_schedule.collection_name))
    _scheduler.start()


def _do_depth_crawl(_dc):
    sleep(randrange(600))
    while True:
        sleep(randrange(60))
        try:
            if _dc.exchange == "binance":
                _order = binance_obj.client.get_order_book(symbol=_dc.market, limit=1000)
            elif _dc.exchange == "kucoin":
                _order = kucoin_client.get_full_order_book(_dc.market)
        except BinanceAPIException as err:
            traceback.print_tb(err.__traceback__)
            logger_global[0].exception("BinanceAPIException -> sleeping 5 min {} {}".format(_dc.market, err.__traceback__))
            sleep(randrange(60))
        except ConnectionError as err:
            traceback.print_tb(err.__traceback__)
            logger_global[0].exception("ConnectionError -> sleeping{} {}".format(_dc.market, err.__traceback__))
            sleep(60)
        except KucoinAPIException as err:
            traceback.print_tb(err.__traceback__)
            logger_global[0].exception("KucoinAPIException -> sleeping{} {}".format(_dc.market, err.__traceback__))
            _order = "sleeping"
            sleep(60)
            _order = kucoin_client.get_full_order_book(_dc.market)
        except Exception as err:
            traceback.print_tb(err.__traceback__)
            logger_global[0].exception("{} {}".format(_dc.market, err.__traceback__))

        _bd = compute_depth_percentages(_order['bids'], "bids")
        if _dc.exchange == "kucoin":
            _order['asks'].reverse()
        _sd = compute_depth_percentages(_order['asks'], "asks")
        _dc.add_depths(_bd, _sd)
        sleep(3 * 60)


def set_average_depths(_dc: DepthCrawl, _ticker, _curr_kline):
    while len(_dc.buy_depth_15m) == 0:
        logger_global[0].info(f"{_dc.market} set_average_depths sleeping : len(_dc.buy_depth_15m) == 0")
        sleep(2*60 + randrange(100))
    # save_to_file("E://bin//data//", "dc", _dc)
    logger_global[0].info("current kline time: {} {}".format(_curr_kline.start_time, get_time_from_binance_tmstmp(_curr_kline.start_time)))
    # if _ticker == '15m':
    #     __bd_l = list(filter(lambda x: x.timestamp == _curr_kline.timestamp, _dc.buy_depth_15m))
    #     __sd_l = list(filter(lambda x: x.timestamp == _curr_kline.timestamp, _dc.sell_depth_15m))
    #     if len(__bd_l) > 0:
    #         logger_global[0].info("kline time: {} dc buy time: {}".format(_curr_kline.start_time, __bd_l[0].timestamp))
    #         logger_global[0].info("kline time: {} dc sell time: {}".format(_curr_kline.start_time, __sd_l[0].timestamp))
    #         _curr_kline.add_buy_depth(__bd_l[0])
    #         _curr_kline.add_sell_depth(__sd_l[0])
    #     else:
    #         logger_global[0].info("Not found {}".format(_ticker))
    # if _ticker == '30m':
    while _dc.market in depth_locker:
        sleep(5)
    inject_market_depth(_curr_kline, _dc, _ticker)


def inject_market_depth(_curr_kline, _dc, _ticker):
    depth_locker[_dc.market] = True
    _ticker_n = ticker2num(_ticker)
    _multiple_15 = int(_ticker_n * 4)
    _tmts = list(map(lambda x: x.timestamp, _dc.buy_depth_15m))
    _data_exist = True
    try:
        _idx = _tmts.index(int(_curr_kline.start_time/1000))
    except ValueError:
        _data_exist = None
    if _data_exist:
        for __el in _dc.buy_depth_15m[_idx:_idx + _multiple_15]:
            logger_global[0].info("{} dc buy time: {}".format(_ticker, __el.time_str))
        for __el in _dc.sell_depth_15m[_idx:_idx + _multiple_15]:
            logger_global[0].info("{} dc sell time: {}".format(_ticker, __el.time_str))
        __bd_r = reduce(add_dc, _dc.buy_depth_15m[_idx:_idx + _multiple_15])
        __sd_r = reduce(add_dc, _dc.sell_depth_15m[_idx:_idx + _multiple_15])
        __bd_r = divide_dc(__bd_r, _multiple_15)
        __sd_r = divide_dc(__sd_r, _multiple_15)
        __bd_r.set_time(int(_curr_kline.start_time/1000))
        __sd_r.set_time(int(_curr_kline.start_time/1000))
        _curr_kline.add_buy_depth(__bd_r)
        _curr_kline.add_sell_depth(__sd_r)
    else:
        logger_global[0].info("DC data not found {}".format(_ticker))
    del depth_locker[_dc.market]


def _do_schedule(_schedule):
    market = _schedule.market
    ticker = _schedule.ticker
    collection_name = _schedule.collection_name
    collection = db.get_collection(collection_name, codec_options=codec_options)
    cursor = collection.find_one()
    if cursor is None:
        _schedule.no_such_market = True
    sleep(1)
    while True:
        if ticker == BinanceClient.KLINE_INTERVAL_15MINUTE or BinanceClient.KLINE_INTERVAL_30MINUTE:
            sleep(randrange(60))
        elif ticker == BinanceClient.KLINE_INTERVAL_5MINUTE:
            sleep(randrange(1))
        else:
            sleep(randrange(200))
        if _schedule.exchange == "binance":
            try:
                klines = try_get_klines(_schedule.exchange, market, ticker, get_binance_interval_unit(ticker, _schedule.no_such_market))
                klines = klines[:-1]  # we skip the last kline on purpose since it has not been closed
            except BinanceAPIException as bae:
                logger_global[0].exception("{} {} {}".format(_schedule.exchange, collection_name, bae.__traceback__))
                logger_global[0].info("sleeping ...")
                sleep(randrange(500))
                klines = get_binance_klines(market, ticker, get_binance_interval_unit(ticker, _schedule.no_such_market))
                klines = klines[:-1]  # we skip the last kline on purpose since it has not been closed
            except Exception as err:
                traceback.print_tb(err.__traceback__)
                logger_global[0].exception("{} {} {}".format(_schedule.exchange, collection_name, err.__traceback__))
                sleep(randrange(30))
                klines = get_binance_klines(market, ticker, get_binance_interval_unit(ticker, _schedule.no_such_market))
                klines = klines[:-1]  # we skip the last kline on purpose since it has not been closed
        elif _schedule.exchange == "kucoin":
            try:
                klines = try_get_klines(_schedule.exchange, market, ticker, get_kucoin_interval_unit(ticker))
            except Exception:
                traceback.print_tb(err.__traceback__)
                logger_global[0].exception("{} {} {}".format(_schedule.exchange, collection_name, err.__traceback__))
                sleep(randrange(30))
                klines = get_kucoin_klines(market, ticker, get_kucoin_interval_unit(ticker))
        current_klines = filter_current_klines(klines, collection_name, collection)
        sleep(5)
        set_average_depths(_schedule.depth_crawl, ticker, current_klines[-1])
        list(map(lambda x: x.add_market(market), current_klines))
        list(map(lambda x: x.add_exchange(_schedule.exchange), current_klines))
        persist_klines(current_klines, collection)
        logger_global[0].info("Stored to collection : {} : {} ".format(_schedule.exchange, collection_name))
        _schedule.journal.update_one({'market': _schedule.asset, 'ticker': _schedule.ticker}, {'$set': {'running': False}})
        _schedule.journal.update_one({'market': _schedule.asset, 'ticker': _schedule.ticker}, {'$set': {'last_seen': round(datetime.datetime.now().timestamp())}})
        _schedule.no_such_market = False
        sleep(_schedule.sleep + randrange(500))


def ticker2sec(_ticker):
    if _ticker == "1m":
        return 1 * 60
    if _ticker == "5m":
        return 5 * 60
    if _ticker == "15m":
        return 15 * 60
    if _ticker == "30m":
        return 30 * 60
    if _ticker == "1h":
        return 1 * 60 * 60
    if _ticker == "2h":
        return 2 * 60 * 60
    if _ticker == "4h":
        return 4 * 60 * 60
    if _ticker == "6h":
        return 6 * 60 * 60
    if _ticker == "8h":
        return 8 * 60 * 60
    if _ticker == "12h":
        return 12 * 60 * 60
    if _ticker == "1d":
        return 24 * 60 * 60
    if _ticker == "2d":
        return 48 * 60 * 60
    if _ticker == "3d":
        return 72 * 60 * 60
    if _ticker == "4d":
        return 96 * 60 * 60
    if _ticker == "5d":
        return 120 * 60 * 60
    if _ticker == "1w":
        return 168 * 60 * 60


def ticker2num(_ticker):
    if _ticker == "1m":
        return 1.0/60
    if _ticker == "5m":
        return 1.0/12
    if _ticker == "15m":
        return 0.25
    if _ticker == "30m":
        return 0.5
    if _ticker == "1h":
        return 1
    if _ticker == "2h":
        return 2
    if _ticker == "4h":
        return 4
    if _ticker == "6h":
        return 6
    if _ticker == "8h":
        return 8
    if _ticker == "12h":
        return 12
    if _ticker == "1d":
        return 24
    if _ticker == "2d":
        return 48
    if _ticker == "3d":
        return 72
    if _ticker == "4d":
        return 96
    if _ticker == "5d":
        return 120
    if _ticker == "1w":
        return 168


def get_binance_schedule(_market_name, _market_type, _ticker_val, _journal, _no_such_market=False):
    _exchange = "binance"
    _market = (_market_name + _market_type).lower()

    if _market not in depth_crawl_dict:
        _dc = DepthCrawl(_market)
        depth_crawl_dict[_market] = _dc
        manage_depth_scan(_dc)

    _dc = depth_crawl_dict[_market]

    if _market_type == "btc":
        _collection_name = _market_name + _ticker_val
    else:
        _collection_name = _market_name + "_" + _market_type + "_" + _ticker_val

    return Schedule(_market_name, _market.upper(), _collection_name, _ticker_val,
                    ticker2sec(_ticker_val), _exchange, _dc, _journal, _no_such_market)


