import sys
import time
import traceback
import requests

from binance.client import Client

from library import stop_signal, sat, sell_limit, setup_logger


asset = "HOT"
stop_price_in_satoshi = 25


market = "{}BTC".format(asset)
ticker = Client.KLINE_INTERVAL_1MINUTE
time_interval = "6 hours ago"
stop_price = stop_price_in_satoshi * sat

logger = setup_logger(asset)
logger.info("Starting {} stop-loss maker".format(market))
logger.info("Stop price is set up to : {:.8f} BTC".format(stop_price))

while 1:
    try:
        stop = stop_signal(market, ticker, time_interval, stop_price, 1)
        if stop:
            sell_limit(market, asset)
            logger.info("Stop-loss LIMIT order has been made, exiting")
            sys.exit(0)
        time.sleep(40)
    except Exception as err:
        if isinstance(err, requests.exceptions.ConnectionError):
            logger.error("Connection problem...")
        else:
            traceback.print_tb(err.__traceback__)
            logger.exception(err.__traceback__)