import datetime
import time
import traceback

import requests
from binance.client import Client as BinanceClient
from bson.codec_options import TypeRegistry, CodecOptions

from library import analyze_40ma, setup_logger, authorize, DecimalCodec, manage_verifying_setup, send_mail, MailContent, \
    Markets, process_entries
from mongodb import mongo_client

logger = setup_logger(f"40-ma-analysis")
logger.info(f"Starting 40-ma-analysis")

authorize()

db = mongo_client.setups
decimal_codec = DecimalCodec()
type_registry = TypeRegistry([decimal_codec])
codec_options = CodecOptions(type_registry=type_registry)
collection = db.get_collection("40ma", codec_options=codec_options)
# manage_verifying_setup(collection)

binance_vol_filter = 20.0
kucoin_vol_filter = 2.0
markets_obj = Markets(binance_vol_filter, kucoin_vol_filter)

while 1:
    try:
        mail_content = MailContent('')

        _24_hours_old = datetime.datetime.now().timestamp() - markets_obj.timestamp > 24 * 60 * 60
        if _24_hours_old:
            logger.info(
                f"Markets older than 24 hours {datetime.datetime.now().timestamp()} <- now : {markets_obj.timestamp} <- markets")
            markets_obj = Markets(binance_vol_filter, kucoin_vol_filter)

        exchange = "binance"
        ticker = BinanceClient.KLINE_INTERVAL_1HOUR
        market_entries = analyze_40ma("exclude-markets-binance", exchange, ticker, "1 week ago", markets_obj)
        process_entries(market_entries, exchange, collection, ticker, mail_content)

        if len(mail_content.content) > 0:
            send_mail(f"EEE Market Entry Found EEE", mail_content.content)

        time.sleep(3600)

    except Exception as err:
        if isinstance(err, requests.exceptions.ConnectionError) or isinstance(err, requests.exceptions.ReadTimeout):
            logger.error("Connection problem...")
        else:
            traceback.print_tb(err.__traceback__)
            logger.exception(err.__traceback__)


