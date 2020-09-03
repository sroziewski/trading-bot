import time
import traceback

import requests
from bson.codec_options import TypeRegistry, CodecOptions

from library import setup_logger, analyze_golden_cross, authorize, get_kucoin_interval_unit, process_setups, \
    DecimalCodec, manage_verifying_setup
from mongodb import mongo_client

logger = setup_logger("market-setup-finder")
logger.info("Starting Market-Setup-Finder...")

authorize()

db = mongo_client.setups
decimal_codec = DecimalCodec()
type_registry = TypeRegistry([decimal_codec])
codec_options = CodecOptions(type_registry=type_registry)
collection = db.get_collection("asset", codec_options=codec_options)
manage_verifying_setup(collection)

while 1:
    try:
        _tickers = ["1", "4", "8", "12"]
        for _t in _tickers:
            _binance_ticker = "{_t}h"
            _kucoin_ticker = "{_t}hour"
            market_setups_binance = analyze_golden_cross("exclude-markets-binance", _binance_ticker, "1600 hours ago", "binance")
            _kucoin_ticker = "1hour"
            market_setups_kucoin = analyze_golden_cross("exclude-markets-kucoin", _kucoin_ticker,
                                                        get_kucoin_interval_unit(_kucoin_ticker, 1600), "kucoin")
            setup_tuples = [(market_setups_binance, "binance"), (market_setups_kucoin, "kucoin")]
            process_setups(setup_tuples, collection, _binance_ticker)
        time.sleep(3500)
    except Exception as err:
        if isinstance(err, requests.exceptions.ConnectionError) or isinstance(err, requests.exceptions.ReadTimeout):
            logger.error("Connection problem...")
        else:
            traceback.print_tb(err.__traceback__)
            logger.exception(err.__traceback__)


