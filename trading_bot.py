import sys
import time
import traceback

from binance.client import Client

from library import stop_signal, sat, sell_limit, setup_logger


asset = "HOT"
price_in_satoshi = 23


market = "{}BTC".format(asset)
time_interval = Client.KLINE_INTERVAL_15MINUTE
stop_price = price_in_satoshi * sat

logger = setup_logger(asset)
logger.info("Starting {} stop-loss maker".format(market))
logger.info("Stop price is set up to : {:.8f} BTC".format(stop_price))

while 1:
    try:
        stop = stop_signal(market, time_interval, "12 hours ago", stop_price)
        if stop:
            sell_limit(market, asset)
            logger.info("Stop-loss LIMIT order has been made, exiting")
            sys.exit(0)
        time.sleep(10)
    except Exception as err:
        traceback.print_tb(err.__traceback__)
        logger.exception(err.__traceback__)