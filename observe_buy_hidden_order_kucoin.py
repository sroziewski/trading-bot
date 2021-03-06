import sys

from binance.client import Client as BinanceClient
from kucoin.client import Client as KucoinClient

from library import BuyAsset, sat, setup_logger, watch_orders, \
    set_kucoin_buy_orders, adjust_purchase_fund, wait_until_order_is_filled, authorize, send_mail, get_format_price

side = KucoinClient.SIDE_BUY
logger = setup_logger("kucoin-observe-and-make-hidden-orders-{}".format(side))
authorize()

buy1 = BuyAsset("kucoin", "OLT", 33.31 * sat, 29.3 * sat, 44.4 * sat, kucoin_side=side, ticker=BinanceClient.KLINE_INTERVAL_5MINUTE, stop_loss=False)
buy2 = BuyAsset("kucoin", "CHR", 212 * sat, 191 * sat, 265 * sat, kucoin_side=side, ticker=BinanceClient.KLINE_INTERVAL_5MINUTE, stop_loss=False)
buy3 = BuyAsset("kucoin", "COTI", 253 * sat, 232 * sat, 298 * sat, kucoin_side=side, ticker=BinanceClient.KLINE_INTERVAL_5MINUTE, stop_loss=False)
buy4 = BuyAsset("kucoin", "CRO", 763 * sat, 711 * sat, 843 * sat, kucoin_side=side, ticker=BinanceClient.KLINE_INTERVAL_5MINUTE, stop_loss=False)
buy5 = BuyAsset("kucoin", "FET", 348 * sat, 310 * sat, 385 * sat, kucoin_side=side, ticker=BinanceClient.KLINE_INTERVAL_5MINUTE, stop_loss=False)


assets = [buy1, buy2, buy3, buy4, buy5]

adjust_purchase_fund(assets)
set_kucoin_buy_orders(assets)

bought_asset = watch_orders(assets)
bought_asset.limit_hidden_order(is_profit=True)
if wait_until_order_is_filled(bought_asset):
    mail_content = f"XXX {bought_asset.market} Took Profit XXX", f"{bought_asset.size_profit} {bought_asset.name} @ {get_format_price(bought_asset.price_profit)} BTC : {get_format_price(bought_asset.adjusted_size * bought_asset.price_profit)} BTC"
    send_mail(mail_content)
    sys.exit(0)



