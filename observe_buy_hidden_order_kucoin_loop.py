import time

from library import BuyAsset, sat, setup_logger, cancel_kucoin_current_orders
from binance.client import Client as BinanceClient
from kucoin.client import Client as KucoinClient


def observe_kucoin_asset(_assets):
    pass


side = KucoinClient.SIDE_BUY
logger = setup_logger("kucoin-observe-hidden-order-{}".format(side))

buy1 = BuyAsset("kucoin", "VIDT", 4173 * sat, ratio=100, kucoin_side=side, ticker=BinanceClient.KLINE_INTERVAL_5MINUTE, sleep=180, stop_loss=False)
buy2 = BuyAsset("kucoin", "CHR", 243 * sat, ratio=100, kucoin_side=side, ticker=BinanceClient.KLINE_INTERVAL_5MINUTE, sleep=180, stop_loss=False)
buy3 = BuyAsset("kucoin", "OCEAN", 2896 * sat, ratio=100, kucoin_side=side, ticker=BinanceClient.KLINE_INTERVAL_5MINUTE, sleep=180, stop_loss=False)
buy4 = BuyAsset("kucoin", "VRA", 4.5 * sat, ratio=100, kucoin_side=side, ticker=BinanceClient.KLINE_INTERVAL_5MINUTE, sleep=180, stop_loss=False)

assets = [buy1, buy2, buy3, buy4]

prev_asset = None

while 1:
    for buy_asset in assets:
        if prev_asset:
            cancel_kucoin_current_orders(prev_asset.market)
        _id = buy_asset.hidden_buy_order()
        if _id:
            logger.info("{} : Make hidden {} LIMIT order for {} SUCCESS".format(buy_asset.exchange, side, buy_asset.market))
            time.sleep(buy_asset.sleep)
        else:
            logger.info("{} : Make hidden {} LIMIT order for {} FAILED".format(buy_asset.exchange, side, buy_asset.market))
        prev_asset = buy_asset