import sys
import time
import traceback
import requests

from library import stop_signal, sat, sell_limit_stop_loss, setup_logger, SellAsset, get_interval_unit

name = "XRP"
stop_price_in_satoshi = 2975

stop_price = stop_price_in_satoshi * sat
sell_asset = SellAsset(name, stop_price)
time_interval = get_interval_unit(sell_asset.ticker)

logger = setup_logger(sell_asset.name)
logger.info("Starting {} stop-loss maker".format(sell_asset.market))
logger.info("Stop price is set up to : {:.8f} BTC".format(stop_price))

while 1:
    try:
        stop = stop_signal(sell_asset.market, sell_asset.ticker, time_interval, stop_price, 1)
        if stop:
            sell_limit_stop_loss(sell_asset.market, sell_asset)
            logger.info("Stop-loss LIMIT order has been made, exiting")
            sys.exit(0)
        time.sleep(40)
    except Exception as err:
        if isinstance(err, requests.exceptions.ConnectionError):
            logger.error("Connection problem...")
        else:
            traceback.print_tb(err.__traceback__)
            logger.exception(err.__traceback__)