import time

from binance.client import Client as BinanceClient

from library import BuyAsset, sat, setup_logger, cancel_binance_current_orders


logger = setup_logger("binance-observe-make-order-loop")

buy1 = BuyAsset("binance", "TRB", 100 * sat, 29.3 * sat, 44.4 * sat, ticker=BinanceClient.KLINE_INTERVAL_5MINUTE, stop_loss=False)
buy2 = BuyAsset("binance", "CHR", 100 * sat, 191 * sat, 265 * sat, ticker=BinanceClient.KLINE_INTERVAL_5MINUTE, stop_loss=False)
buy3 = BuyAsset("binance", "COTI", 100 * sat, 232 * sat, 298 * sat, ticker=BinanceClient.KLINE_INTERVAL_5MINUTE, stop_loss=False)

assets = [buy1, buy2, buy3]

prev_asset = None

while 1:
    for buy_asset in assets:
        if prev_asset:
            cancel_binance_current_orders(prev_asset.market)
        _id = buy_asset.hidden_buy_order()
        if _id:
            logger.info("{} : Make hidden {} LIMIT order for {} SUCCESS".format(buy_asset.exchange, side, buy_asset.market))
            time.sleep(buy_asset.sleep)
        else:
            logger.info("{} : Make hidden {} LIMIT order for {} FAILED".format(buy_asset.exchange, side, buy_asset.market))
        prev_asset = buy_asset