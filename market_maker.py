import sys
import time
import traceback
import requests

from binance.client import Client

from library import stop_signal, sat, sell_limit_stop_loss, setup_logger, BuyAsset, observe_lower_price, price_to_string

logger = setup_logger("WATCH TO BUY")

buy_assets = [BuyAsset('HOT', 0.00000020, 0.00000014, 100), BuyAsset('WTC', 0.0001823, 0.0001723), BuyAsset('NANO', 0.0001470, 0.0001570)]

logger.info("Starting observing assets:\n{}".format('\n'.join(map(lambda _a: "{} :\t{}".format(_a.name, price_to_string(_a.price)), buy_assets))))

observed = observe_lower_price(buy_assets)



while 1:
    try:
        stop = stop_signal(market, ticker, time_interval, stop_price)
        if stop:
            sell_limit_stop_loss(market, asset)
            logger.info("Stop-loss LIMIT order has been made, exiting")
            sys.exit(0)
        time.sleep(10)
    except Exception as err:
        if isinstance(err, requests.exceptions.ConnectionError):
            logger.error("Connection problem...")
        else:
            traceback.print_tb(err.__traceback__)
            logger.exception(err.__traceback__)
