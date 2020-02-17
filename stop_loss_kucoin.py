import sys
import time
import traceback

import requests

from library import stop_signal, sat, sell_limit_stop_loss, setup_logger, SellAsset, AccountHoldingZero

name = "VIDT"
stop_price_in_satoshi = 2600

stop_price = stop_price_in_satoshi * sat

sell_asset_kucoin = SellAsset("kucoin", name, stop_price, True)

logger = setup_logger(sell_asset_kucoin.name)
logger.info("Starting {} stop-loss maker on {}".format(sell_asset_kucoin.market, sell_asset_kucoin.exchange))
logger.info("Stop price is set up to : {:.8f} BTC".format(stop_price))

while 1:
    try:
        stop = stop_signal(sell_asset_kucoin.exchange, sell_asset_kucoin.market, sell_asset_kucoin.ticker, stop_price, 5)
        if stop:
            sell_limit_stop_loss(sell_asset_kucoin.market, sell_asset_kucoin)
            logger.info("Stop-loss LIMIT order has been made on {}, exiting".format(sell_asset_kucoin.exchange))
            sys.exit(0)
        time.sleep(40)
    except AccountHoldingZero as warn:
        logger.warning(warn)
        sys.exit("Exit")
    except Exception as err:
        if isinstance(err, requests.exceptions.ConnectionError):
            logger.error("Connection problem...")
        else:
            traceback.print_tb(err.__traceback__)
            logger.exception(err.__traceback__)