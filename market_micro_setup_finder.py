import datetime
import time
import traceback

import requests
from bson.codec_options import TypeRegistry, CodecOptions

from library import setup_logger, get_kucoin_interval_unit, process_setups, \
    DecimalCodec, manage_verifying_setup, MailContent, analyze_micro_markets, Markets
from mongodb import mongo_client

logger = setup_logger("market-micro-setup-finder")
logger.info("Starting Market-Micro-Setup-Finder...")

# authorize()

db = mongo_client.micro
decimal_codec = DecimalCodec()
type_registry = TypeRegistry([decimal_codec])
codec_options = CodecOptions(type_registry=type_registry)
collection = db.get_collection("asset", codec_options=codec_options)
manage_verifying_setup(collection)

binance_vol_filter = 50.0
kucoin_vol_filter = 20.0
markets_obj = Markets(binance_vol_filter, kucoin_vol_filter)

while 1:
    try:
        _tickers = [0.5]

        mail_content = MailContent('')
        for _t in _tickers:
            if 1 < _t < 24:
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
            _24_hours_old = datetime.datetime.now().timestamp() - markets_obj.timestamp > 24*60*60
            if _24_hours_old:
                logger.info(f"Markets older than 24 hours {datetime.datetime.now().timestamp()} <- now : {markets_obj.timestamp} <- markets")
                markets_obj = Markets(binance_vol_filter, kucoin_vol_filter)
            market_setups_binance = analyze_micro_markets("exclude-micro-markets-binance", _binance_ticker, "200 hours ago", "binance", markets_obj)
            market_setups_kucoin = analyze_micro_markets("exclude-micro-markets-kucoin", _kucoin_ticker,
                                                        get_kucoin_interval_unit(_kucoin_ticker, 200), "kucoin", markets_obj)
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


