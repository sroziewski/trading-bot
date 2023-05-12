import threading
from time import sleep

from bson import CodecOptions
from bson.codec_options import TypeRegistry

from library import DecimalCodec, lib_initialize, try_get_klines, save_to_file, setup_logger
from mongodb import mongo_client

lib_initialize()

logger = setup_logger("Binance-History-Retriever")

class Retrieve(object):
    def __init__(self, _market, _ticker):
        self.market = _market
        self.ticker = _ticker


threads = {}


def _do_retrieve(_retrieve: Retrieve):
    klines = try_get_klines("binance", _retrieve.market.upper(), _retrieve.ticker, "3000 days ago")
    _filename = "{}_{}".format(_retrieve.market, _retrieve.ticker)
    save_to_file("/home/0agent1/store/history-klines/usdt/", _filename, klines)
    _name = "{}{}".format(_retrieve.market, _retrieve.ticker)
    logger.info(_name)
    del threads[_name]


def manage_retrieve_scan(_r: Retrieve):
    _crawler = threading.Thread(target=_do_retrieve, args=(_r,),
                                name='_do_retrieve : {}'.format(_r.market))
    _crawler.start()
    threads["{}{}".format(_r.market, _r.ticker)] = 1


db_markets_info = mongo_client.markets_info
db_journal = mongo_client.journal

decimal_codec = DecimalCodec()
type_registry = TypeRegistry([decimal_codec])
codec_options = CodecOptions(type_registry=type_registry)

btc_markets_collection = db_markets_info.get_collection("btc", codec_options=codec_options)
usdt_markets_collection = db_markets_info.get_collection("usdt", codec_options=codec_options)

_market_info_cursor = usdt_markets_collection.find()
_market_info_list = [e for e in _market_info_cursor]


for _market_s in _market_info_list:
    for _ticker in _market_s['tickers']:
        if _ticker not in ['2d', '4d', '5d']:
            _market = "{}{}".format(_market_s['name'], "usdt")
            manage_retrieve_scan(Retrieve(_market, _ticker))
            while len(threads.values()) > 5:
                sleep(1)
