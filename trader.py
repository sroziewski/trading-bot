import sys
import time
import traceback
import requests

from binance.client import Client

from library import stop_signal, sat, sell_limit_stop_loss, setup_logger, TradeAsset, is_bullish_setup, \
    get_remaining_btc, adjust_buy_asset_btc_volume, BullishStrategy, get_lot_size_params

trade_assets = [
        TradeAsset('CELR')
    ]

logger = setup_logger("trader")

btc_value = get_remaining_btc()
adjust_buy_asset_btc_volume(trade_assets, btc_value)

while 1:
    try:
        for trade_asset in trade_assets:
            if not trade_asset.trading and is_bullish_setup(trade_asset):
                _params = get_lot_size_params(trade_asset.market)
                BullishStrategy(trade_asset, btc_value, _params).run()

        time.sleep(40)
    except Exception as err:
        if isinstance(err, requests.exceptions.ConnectionError):
            logger.error("Connection problem...")
        else:
            traceback.print_tb(err.__traceback__)
            logger.exception(err.__traceback__)