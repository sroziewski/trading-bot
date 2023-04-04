import datetime
import threading
import traceback
from random import randrange
from time import sleep

from binance.client import Client as BinanceClient
from binance.exceptions import BinanceAPIException
from bson import CodecOptions
from bson.codec_options import TypeRegistry
from kucoin.exceptions import KucoinAPIException

from depth_crawl import compute_depth_percentages, divide_dc, add_dc
from library import get_binance_klines, get_binance_interval_unit, get_kucoin_klines, \
    get_kucoin_interval_unit, binance_obj, kucoin_client, DecimalCodec, try_get_klines, get_last_db_record, \
    get_time_from_binance_tmstmp, logger_global
from mongodb import mongo_client

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
    def __init__(self, _asset, _market, _collection_name, _ticker, _sleep, _exchange, _dc, _no_depths, _journal, _no_such_market=False):
        self.asset = _asset
        self.market = _market
        self.collection_name = _collection_name
        self.ticker = _ticker
        self.sleep = _sleep
        self.exchange = _exchange
        self.depth_crawl = _dc
        self.no_depths = _no_depths
        self.journal = _journal
        self.no_such_market = _no_such_market


def manage_crawling(_schedule):
    sleep(5)
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


def filter_current_trades(_vc):
    _yesterday_time = (datetime.datetime.now().timestamp() - 24 * 60 * 60) * 1000
    _tmp_trades = list(filter(lambda x: x.timestamp > _yesterday_time, trades[_vc.market])).copy()
    del trades[_vc.market]
    trades[_vc.market] = _tmp_trades


def manage_depth_crawling(_dc):
    _crawler = threading.Thread(target=_do_depth_crawl, args=(_dc,),
                                name='_do_depth_crawl : {}'.format(_dc.market))
    _crawler.start()


def get_average_depths(_dc, _number_of_elements):
    while len(_dc.buy_depth) == 0:
        logger_global[0].info(f"{_dc.market} get_average_depths sleeping : len(_dc.buy_depth) == 0")
        sleep(2*60 + randrange(100))
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
                # klines = klines[:-1]  # we skip the last kline on purpose to have for it a crawling volume
            except BinanceAPIException as bae:
                logger_global[0].exception("{} {} {}".format(_schedule.exchange, collection_name, bae.__traceback__))
                logger_global[0].info("sleeping ...")
                sleep(randrange(500))
                klines = get_binance_klines(market, ticker, get_binance_interval_unit(ticker, _schedule.no_such_market))
                klines = klines[:-1]  # we skip the last kline on purpose to have for it a crawling volume
            except Exception as err:
                traceback.print_tb(err.__traceback__)
                logger_global[0].exception("{} {} {}".format(_schedule.exchange, collection_name, err.__traceback__))
                sleep(randrange(30))
                klines = get_binance_klines(market, ticker, get_binance_interval_unit(ticker, _schedule.no_such_market))
                # klines = klines[:-1]  # we skip the last kline on purpose to have for it a crawling volume
        elif _schedule.exchange == "kucoin":
            try:
                klines = try_get_klines(_schedule.exchange, market, ticker, get_kucoin_interval_unit(ticker))
            except Exception:
                traceback.print_tb(err.__traceback__)
                logger_global[0].exception("{} {} {}".format(_schedule.exchange, collection_name, err.__traceback__))
                sleep(randrange(30))
                klines = get_kucoin_klines(market, ticker, get_kucoin_interval_unit(ticker))
        current_klines = filter_current_klines(klines, collection_name, collection)
        sleep(15)
        bd, sd = get_average_depths(_schedule.depth_crawl, _schedule.no_depths)
        list(map(lambda x: x.add_buy_depth(bd), current_klines))
        list(map(lambda x: x.add_sell_depth(sd), current_klines))
        list(map(lambda x: x.add_market(market), current_klines))
        # if _schedule.exchange == "binance":
        #     list(map(lambda x: set_trade_volume(_schedule, x), current_klines))
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


def get_binance_schedule(_market_name, _market_type, _ticker_val, _journal, _depth_scan_set, _no_such_market=False):
    _exchange = "binance"
    _market = (_market_name + _market_type).upper()

    if _market not in _depth_scan_set:
        _dc = DepthCrawl(_market, _exchange)
        _depth_scan_set[_market] = _dc
        manage_depth_crawling(_dc)

    _dc = _depth_scan_set[_market]

    if _market_type == "btc":
        _collection_name = _market_name + _ticker_val
    else:
        _collection_name = _market_name + "_" + _market_type + "_" + _ticker_val

    return Schedule(_market_name, _market, _collection_name, _ticker_val,
                    ticker2sec(_ticker_val), _exchange, _dc, round(20 * ticker2num(_ticker_val)), _journal, _no_such_market)


