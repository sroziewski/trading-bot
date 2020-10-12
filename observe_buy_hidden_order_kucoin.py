import time

from library import BuyAsset, sat, setup_logger, cancel_kucoin_current_orders
from binance.client import Client as BinanceClient
from kucoin.client import Client as KucoinClient


def observe_kucoin_asset(_assets):
    pass


side = KucoinClient.SIDE_BUY
logger = setup_logger("kucoin-observe-hidden-order-{}".format(side))

buy1 = BuyAsset("kucoin", "DIA", 12300 * sat, ratio=100, kucoin_side=side, ticker=BinanceClient.KLINE_INTERVAL_5MINUTE, sleep=1, stop_loss=False)
buy2 = BuyAsset("kucoin", "VRA", 5.1 * sat, ratio=100, kucoin_side=side, ticker=BinanceClient.KLINE_INTERVAL_5MINUTE, sleep=10, stop_loss=False)
buy3 = BuyAsset("kucoin", "BEPRO", 6.29 * sat, ratio=100, kucoin_side=side, ticker=BinanceClient.KLINE_INTERVAL_5MINUTE, sleep=20, stop_loss=False)

assets = [buy1, buy2, buy3]

prev_asset = None

while 1:
    for buy_asset in assets:
        if prev_asset:
            cancel_kucoin_current_orders(prev_asset.market)
        _id = buy_asset.limit_hidden_order()
        if _id:
            logger.info("Make hidden {} LIMIT order for {} SUCCESS".format(side, buy_asset.market))
            time.sleep(buy_asset.sleep)
        else:
            logger.info("Make hidden {} LIMIT order for {} FAILED".format(side, buy_asset.market))
        prev_asset = buy_asset