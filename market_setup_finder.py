import datetime
import sys
import time
import traceback

import requests
from bson.codec_options import TypeRegistry, CodecOptions

from library import setup_logger, analyze_markets, authorize, get_kucoin_interval_unit, process_setups, \
    DecimalCodec, manage_verifying_setup, send_mail, MailContent, Markets
from mongodb import mongo_client

arguments = len(sys.argv) - 1

if arguments == 0:
    print("You have to specify type of setups: for longer periods (long), and shorter (short)")
    exit(0)

type_of_scan = sys.argv[0]
tickers = []
if type_of_scan == "long":
    tickers = [4, 8, 12, 24]
elif type_of_scan == "short":
    tickers = [0.15, 0.30, 1]

logger = setup_logger(f"market-setup-finder-{type_of_scan}")
logger.info(f"Starting Market-Setup-Finder...{type_of_scan}")

authorize()

db = mongo_client.setups
decimal_codec = DecimalCodec()
type_registry = TypeRegistry([decimal_codec])
codec_options = CodecOptions(type_registry=type_registry)
collection = db.get_collection("asset", codec_options=codec_options)
manage_verifying_setup(collection)

binance_vol_filter = 20.0
kucoin_vol_filter = 2.0
markets_obj = Markets(binance_vol_filter, kucoin_vol_filter)

while 1:
    try:
        mail_content = MailContent('')
        for _t in tickers:
            if 1 <= _t < 24:
                _binance_ticker = f"{_t}h"
                _kucoin_ticker = f"{_t}hour"
            elif _t == 24:
                _binance_ticker = "1d"
                _kucoin_ticker = "1day"
            elif _t == 0.30:
                _binance_ticker = "30m"
                _kucoin_ticker = "30min"
            elif _t == 0.15:
                _binance_ticker = "15m"
                _kucoin_ticker = "15min"

            _24_hours_old = datetime.datetime.now().timestamp() - markets_obj.timestamp > 24 * 60 * 60
            if _24_hours_old:
                logger.info(
                    f"Markets older than 24 hours {datetime.datetime.now().timestamp()} <- now : {markets_obj.timestamp} <- markets")
                markets_obj = Markets(binance_vol_filter, kucoin_vol_filter)
            market_setups_binance = analyze_markets("exclude-markets-binance", _binance_ticker, "1600 hours ago", "binance", markets_obj)
            market_setups_kucoin = analyze_markets("exclude-markets-kucoin", _kucoin_ticker,
                                                   get_kucoin_interval_unit(_kucoin_ticker, 1600), "kucoin", markets_obj)
            setup_tuples = [(market_setups_binance, "binance"), (market_setups_kucoin, "kucoin")]
            process_setups(setup_tuples, collection, _binance_ticker, mail_content)

        if len(mail_content.content) > 0:
            send_mail(f"WWW Market Setup Found ({type_of_scan.upper()}) WWW", mail_content.content)

        time.sleep(3500)
    except Exception as err:
        if isinstance(err, requests.exceptions.ConnectionError) or isinstance(err, requests.exceptions.ReadTimeout):
            logger.error("Connection problem...")
        else:
            traceback.print_tb(err.__traceback__)
            logger.exception(err.__traceback__)


