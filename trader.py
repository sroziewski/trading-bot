import time
import traceback
import requests

from library import setup_logger, TradeAsset, is_bullish_setup, \
    get_remaining_btc, adjust_buy_asset_btc_volume, BullishStrategy, get_lot_size_params, is_trading_possible

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
    _last_asset = 'START'
    try:
        for trade_asset in trade_assets:  # remove asset from here
            _c = is_trading_possible(trade_assets)
            if _c and is_bullish_setup(trade_asset):
                if _last_asset and _last_asset != trade_asset.name:
                    _last_asset = trade_asset.name
                    trade_asset.running = True
                    _params = get_lot_size_params(trade_asset.market)
                    BullishStrategy(trade_asset, btc_value, _params).run()
                time.sleep(1)

        time.sleep(40)
    except Exception as err:
        if isinstance(err, requests.exceptions.ConnectionError):
            logger.error("Connection problem...")
        else:
            traceback.print_tb(err.__traceback__)
            logger.exception(err.__traceback__)