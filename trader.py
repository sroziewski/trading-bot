import time
import traceback
import requests

from library import setup_logger, TradeAsset, get_remaining_btc, adjust_buy_asset_btc_volume, start_trading

trade_assets = [
        TradeAsset('CELR'),
        TradeAsset('FTM'),
        TradeAsset('ONE'),
        TradeAsset('MATIC'),
        TradeAsset('ALGO')
    ]

logger = setup_logger("trader")

while 1:
    try:
        _btc = get_remaining_btc()
        btc_value = 0.004 if _btc > 0.004 else _btc
        adjust_buy_asset_btc_volume(trade_assets, btc_value)

        for trade_asset in trade_assets:  # remove asset from here
            start_trading(trade_asset, btc_value)

        time.sleep(40)
    except Exception as err:
        if isinstance(err, requests.exceptions.ConnectionError):
            logger.error("Connection problem...")
        else:
            traceback.print_tb(err.__traceback__)
            logger.exception(err.__traceback__)