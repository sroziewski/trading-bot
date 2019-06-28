import sys
import time
import traceback
import requests

from binance.client import Client

from library import stop_signal, sat, sell_limit_stop_loss, setup_logger, TradeAsset, is_bearish_setup

trade_assets = [
        TradeAsset('CELR')
    ]

logger = setup_logger("trader")

while 1:
    try:
        for trade_asset in trade_assets:
            if trade_asset.filled:  # now we want to sell with profit
                pass
            else:  # now we want to buy the bottom ;)
                is_bearish_setup(trade_asset)

        time.sleep(40)
    except Exception as err:
        if isinstance(err, requests.exceptions.ConnectionError):
            logger.error("Connection problem...")
        else:
            traceback.print_tb(err.__traceback__)
            logger.exception(err.__traceback__)