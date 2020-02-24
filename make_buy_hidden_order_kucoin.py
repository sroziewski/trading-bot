import time

from kucoin.client import Client as KucoinClient

from library import BuyAsset, setup_logger, \
    get_format_price

exchange = 'kucoin'
side = KucoinClient.SIDE_BUY
logger = setup_logger("kucoin-hidden-order-{}".format(side))
price = 0.0000001365
stop_loss_price = 0.000000129
profit_price = 0.0000004799
ratio = 100

buy_asset = BuyAsset(exchange, 'VRA', price, stop_loss_price, profit_price, ratio, 15)

logger.info("Make hidden {} LIMIT order for {}".format(side, buy_asset.market))

_id = buy_asset.limit_hidden_order(side)

if _id:
    time.sleep(5)
    logger.info("{} Stop-loss: {} VRA @ {} BTC in progress only...".format(buy_asset.market, buy_asset.adjusted_size,
         get_format_price(buy_asset.stop_loss_price).format(buy_asset.stop_loss_price)))
    logger.info("Hidden {} LIMIT order has been made, exiting.".format(side))
