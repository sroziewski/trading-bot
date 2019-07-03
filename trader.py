import time
import traceback
import requests

from library import setup_logger, TradeAsset, get_remaining_btc, adjust_buy_asset_btc_volume, start_trading

trade_assets = [
        TradeAsset('CELR'),
        TradeAsset('FTM'),
        TradeAsset('ONE'),
        TradeAsset('MATIC')
    ]

logger = setup_logger("trader")

btc_value = get_remaining_btc()
adjust_buy_asset_btc_volume(trade_assets, btc_value)

while 1:
    try:
        for trade_asset in trade_assets:  # remove asset from here
            start_trading(trade_asset, btc_value)

        time.sleep(40)
    except Exception as err:
        if isinstance(err, requests.exceptions.ConnectionError):
            logger.error("Connection problem...")
        else:
            traceback.print_tb(err.__traceback__)
            logger.exception(err.__traceback__)