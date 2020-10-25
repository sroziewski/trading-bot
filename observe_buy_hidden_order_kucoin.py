import sys

from binance.client import Client as BinanceClient
from kucoin.client import Client as KucoinClient

from library import BuyAsset, sat, setup_logger, watch_orders, \
    set_kucoin_buy_orders, adjust_purchase_fund, wait_until_order_is_filled

side = KucoinClient.SIDE_BUY
logger = setup_logger("kucoin-observe-hidden-order-{}".format(side))

buy1 = BuyAsset("kucoin", "VIDT", 3000 * sat, 1000 * sat, 3200 * sat, kucoin_side=side, ticker=BinanceClient.KLINE_INTERVAL_5MINUTE, stop_loss=False)
buy2 = BuyAsset("kucoin", "CHR", 143 * sat, 100 * sat, 200 * sat, kucoin_side=side, ticker=BinanceClient.KLINE_INTERVAL_5MINUTE, stop_loss=False)
buy3 = BuyAsset("kucoin", "BEPRO", 2.5 * sat, 2 * sat, 3 * sat, kucoin_side=side, ticker=BinanceClient.KLINE_INTERVAL_5MINUTE, stop_loss=False)
buy4 = BuyAsset("kucoin", "BEPRO", 2.5 * sat, 2 * sat, 3 * sat, kucoin_side=side, ticker=BinanceClient.KLINE_INTERVAL_5MINUTE, stop_loss=False)


assets = [buy1, buy2, buy3, buy4]

adjust_purchase_fund(assets)
set_kucoin_buy_orders(assets)

bought_asset = watch_orders(assets)
bought_asset.limit_hidden_order(is_profit=True)
if wait_until_order_is_filled(bought_asset):
    sys.exit(0)



