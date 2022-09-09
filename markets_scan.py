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
    get_kucoin_interval_unit, binance_obj, kucoin_client, DecimalCodec, try_get_klines, TradeMsg, get_last_db_record, \
    get_time_from_binance_tmstmp
from market_scanner import manage_crawling, get_binance_schedule, ticker2sec
from mongodb import mongo_client

logger = setup_logger("Binance-Markets-Scanner")

db_markets_info = mongo_client.markets_info
db_journal = mongo_client.journal

decimal_codec = DecimalCodec()
type_registry = TypeRegistry([decimal_codec])
codec_options = CodecOptions(type_registry=type_registry)

btc_markets_cursor = db_markets_info.get_collection("btc", codec_options=codec_options)
usdt_markets_cursor = db_markets_info.get_collection("usdt", codec_options=codec_options)
busd_markets_cursor = db_markets_info.get_collection("busd", codec_options=codec_options)

data_btc = btc_markets_cursor.find()
l = [e for e in data_btc]

thread_limit = 200


def guard(_data_collection):
    while _data_collection.count_documents({
        "running": True
    }) > thread_limit / 2:
        sleep(5 * 60)  # 5 min of sleep


def process_market_info_entity(_market_entity, _journal_collection):
    _market_type = _journal_collection.name
    guard(_journal_collection)
    if _market_entity['active']:
        _market_name = _market_entity['name']
        # for _ticker in _market_entity['tickers']:
        for _ticker in ['1h']:
            _journal_name = _market_name + _market_type + "_" + _ticker
            _r = _journal_collection.find({
                "market": _market_name,
                "ticker": _ticker
            })
            _now = datetime.datetime.now().timestamp()
            _delta_t = ticker2sec(_ticker)
            if len(list(_r.clone())) < 1:  # there is no such a market yet
                _journal_collection.insert_one({
                    'market': _market_name,
                    'ticker': _ticker,
                    'last_seen': round(_now),
                    'running': True  # we set this as True only here, we will use it for keeping a limited number of threads running at startup
                })
                logger.info("Adding market {} to journal".format(_journal_name.upper()))
                # run a thread here
                manage_crawling(get_binance_schedule(_market_name, _market_type, _ticker, _journal_collection))
            # elif len(list(filter(lambda x: _now - x['last_seen'] >= _delta_t, _r))) > 0:  # market exists but it's not operating
                # run a thread here
                # manage_crawling(get_binance_schedule(_market_name, _market_type, _ticker, _journal_collection))

            l = 1


journal_collection = db_journal.get_collection(data_btc.collection.name, codec_options=codec_options)

# for _market in l:
#     process_market_info_entity(_market, journal_collection)

process_market_info_entity(l[0], journal_collection)

i = 1
