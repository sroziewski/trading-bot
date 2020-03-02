import sys
import time
import traceback

import requests

from library import stop_signal, sat, sell_limit_stop_loss, setup_logger, SellAsset, get_binance_interval_unit

name = "HOT"
stop_price_in_satoshi = 2975

stop_price = stop_price_in_satoshi * sat
sell_asset_binance = SellAsset('binance', name, stop_price)

logger = setup_logger(sell_asset_binance.name)
logger.info("Starting {} stop-loss maker on {}".format(sell_asset_binance.market, sell_asset_binance.exchange))
logger.info("Stop price is set up to : {:.8f} BTC".format(stop_price))

while 1:
    try:
        stop = stop_signal(sell_asset_binance.exchange, sell_asset_binance.market, sell_asset_binance.ticker, stop_price, 1)
        if stop:
            sell_limit_stop_loss(sell_asset_binance.market, sell_asset_binance)
            logger.info("Stop-loss LIMIT order has been made, exiting")
            sys.exit(0)
        time.sleep(40)
    except Exception as err:
        if isinstance(err, requests.exceptions.ConnectionError) or isinstance(err, requests.exceptions.ReadTimeout):
            logger.error("Connection problem...")
        else:
            traceback.print_tb(err.__traceback__)
            logger.exception(err.__traceback__)