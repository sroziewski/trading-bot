import sys
import time
import traceback

from binance.client import Client

from library import stop_signal, sat, sell_limit

asset = "HOT"
market = "{}BTC".format(asset)

stop_price = 22 * sat

while 1:
    try:
        stop = stop_signal(market, Client.KLINE_INTERVAL_15MINUTE, "12 hours ago", stop_price)
        if stop:
            sell_limit(market, asset)
            sys.exit(0)
        time.sleep(10)
    except Exception as err:
        traceback.print_tb(err.__traceback__)