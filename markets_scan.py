import datetime
import sys
import threading
from time import sleep

from bson import CodecOptions
from bson.codec_options import TypeRegistry

from depth_crawl import depth_crawl_dict
from library import setup_logger, DecimalCodec, get_time
from market_scanner import manage_crawling, get_binance_schedule, ticker2sec
from mongodb import mongo_client

market_type = sys.argv[1]
market_time_interval = sys.argv[2]
repair_mode = sys.argv[3]

filename = "Binance-Markets-Scanner-{}-{}".format(market_type.upper(), market_time_interval.upper())
logger = setup_logger(filename)

db_markets_info = mongo_client.markets_info
db_journal = mongo_client.journal

decimal_codec = DecimalCodec()
type_registry = TypeRegistry([decimal_codec])
codec_options = CodecOptions(type_registry=type_registry)

btc_markets_collection = db_markets_info.get_collection("btc", codec_options=codec_options)
usdt_markets_collection = db_markets_info.get_collection("usdt", codec_options=codec_options)
busd_markets_collection = db_markets_info.get_collection("busd", codec_options=codec_options)

thread_limit = 100


def do_scan_market(_market_info_collection):
    _market_info_cursor = _market_info_collection.find()
    _market_info_list = [e for e in _market_info_cursor]
    _journal_cn = _market_info_collection.name + "_" + market_time_interval.lower()
    _journal_collection = db_journal.get_collection(_journal_cn, codec_options=codec_options)

    for _market_s in _market_info_list:  # inf loop needed here
        process_market_info_entity(_market_s, _journal_collection)


def scanner(_market_info_collection):
    _crawler_s = threading.Thread(target=do_scan_market, args=(_market_info_collection,),
                                  name='do_scan_market : {}'.format(_market_info_collection.name))
    _crawler_s.start()


def guard(_data_collection_j):
    if repair_mode != "repair":
        while _data_collection_j.count_documents({
            "running": True
        }) > thread_limit:
            sleep(5 * 60)  # 5 min of sleep


# def guard(_data_collection_j, _market_name_j, _ticker_j):
#     while _data_collection_j.count_documents({
#         "running": True
#     }) > thread_limit:
#         _now = datetime.datetime.now().timestamp()
#         _delta_t = 2 * ticker2sec(_ticker_j)
#         __r = _data_collection_j.find({
#                     "market": _market_name_j,
#                     "ticker": _ticker_j
#                 })
#         len(list(filter(lambda x: _now - x['last_seen'] >= _delta_t,
#                                      __r))) > 0:
#         sleep(5 * 60)  # 5 min of sleep


def validate_time_interval(_ticker):
    if market_time_interval == "ltf":
        return _ticker in ['1m', '5m', '15m', '30m']
    if market_time_interval == "htf":
        return _ticker in ['1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w']


is_repaired = False
repair_set = {}


def process_market_info_entity(_market_entity, _journal_collection):
    global is_repaired
    _market_type = _journal_collection.name.replace("_htf", "").replace("_ltf", "")
    guard(_journal_collection)
    if _market_entity['active']:
        _market_name = _market_entity['name']
        for _ticker in _market_entity['tickers']:
            if _ticker not in ['2d', '4d', '5d'] and validate_time_interval(_ticker):
                _journal_name = _market_name + _market_type + "_" + _ticker
                _r = _journal_collection.find({
                    "market": _market_name,
                    "ticker": _ticker
                })
                _now = datetime.datetime.now().timestamp()
                try:
                    _delta_t = ticker2sec(_ticker) + 15 * 60  # plus 15 min
                except TypeError:
                    logger.error("TypeError ticker" + _ticker + _market_name + _market_type)

                if len(list(_r.clone())) < 1:  # there is no such a market yet
                    _journal_collection.insert_one({
                        'market': _market_name,
                        'ticker': _ticker,
                        'last_seen': round(_now),
                        'last_seen_str': get_time(round(_now)),
                        'running': True
                        # we set this as True only here, we will use it for keeping a limited number of threads running at startup
                    })
                    logger.info("Adding market {} to journal".format(_journal_name.upper()))
                    # run a thread here
                    manage_crawling(
                        get_binance_schedule(_market_name, _market_type, _ticker, _journal_collection))
                elif len(list(filter(lambda x: _now - x['last_seen'] >= _delta_t,
                                     _r))) > 0:  # market exists but it's not operating
                    if _market_name not in repair_set:
                        if repair_mode == "repair":
                            repair_set[_market_name] = True
                        logger.info("Market {} NOT OPERATING --> handled".format(_journal_name.upper()))
                        # run a thread here
                        manage_crawling(
                            get_binance_schedule(_market_name, _market_type, _ticker, _journal_collection,
                                                 depth_crawl_dict))
                elif not is_repaired and repair_mode == "repair" and len(list(filter(lambda x: x['running'], _r))) > 0:
                    is_repaired = True
                    logger.info("Market {} was running --> handled".format(_journal_name.upper()))
                    # run a thread here
                    manage_crawling(
                        get_binance_schedule(_market_name, _market_type, _ticker, _journal_collection))


while True:
    hr = 15 * 60
    if market_type == "btc":
        scanner(btc_markets_collection)
    elif market_type == "usdt":
        scanner(usdt_markets_collection)
    elif market_type == "busd":
        scanner(busd_markets_collection)
    sleep(hr)

# how to run?
# e.g. btc ltf repair
