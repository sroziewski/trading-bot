import time
import traceback

import requests
from bson.codec_options import TypeRegistry, CodecOptions

from library import setup_logger, analyze_golden_cross, authorize, get_kucoin_interval_unit, process_setups, \
    DecimalCodec, manage_verifying_setup, send_mail, MailContent, analyze_micro_markets
from mongodb import mongo_client

logger = setup_logger("market-micro-setup-finder")
logger.info("Starting Market-Micro-Setup-Finder...")

authorize()

db = mongo_client.micro
decimal_codec = DecimalCodec()
type_registry = TypeRegistry([decimal_codec])
codec_options = CodecOptions(type_registry=type_registry)
collection = db.get_collection("asset", codec_options=codec_options)
manage_verifying_setup(collection)

while 1:
    try:
        _tickers = [0.5]

        mail_content = MailContent('')
        for _t in _tickers:
            if _t < 24:
                _binance_ticker = f"{_t}h"
                _kucoin_ticker = f"{_t}hour"
            elif _t == 24:
                _binance_ticker = "1d"
                _kucoin_ticker = "1day"
            elif _t == 30:
                _binance_ticker = "30m"
                _kucoin_ticker = "30min"
            elif _t == 0.5:
                _binance_ticker = "5m"
                _kucoin_ticker = "5min"
            market_setups_binance = analyze_micro_markets("exclude-markets-binance", _binance_ticker, "1600 hours ago", "binance")
            market_setups_kucoin = analyze_micro_markets("exclude-markets-kucoin", _kucoin_ticker,
                                                        get_kucoin_interval_unit(_kucoin_ticker, 1600), "kucoin")
            setup_tuples = [(market_setups_binance, "binance"), (market_setups_kucoin, "kucoin")]
            process_setups(setup_tuples, collection, _binance_ticker, mail_content)

        # if len(mail_content.content) > 0:
        #     send_mail("WWW Market Micro Setup Found WWW", mail_content.content)

        time.sleep(60)
    except Exception as err:
        if isinstance(err, requests.exceptions.ConnectionError) or isinstance(err, requests.exceptions.ReadTimeout):
            logger.error("Connection problem...")
        else:
            traceback.print_tb(err.__traceback__)
            logger.exception(err.__traceback__)


