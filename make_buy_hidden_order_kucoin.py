import time

from kucoin.client import Client as KucoinClient

from library import BuyAsset, setup_logger, \
    get_format_price, sat

exchange = 'kucoin'
side = KucoinClient.SIDE_BUY
logger = setup_logger("kucoin-hidden-order-{}".format(side))

currency = 'VRA'
price = 9.4
stop_loss_price = 7
profit_price = 4700.99
ratio = 100

buy_asset = BuyAsset(exchange, currency, price * sat, stop_loss_price * sat, profit_price * sat, ratio, 15)

# buy_asset.keep_existing_orders()

logger.info("Make hidden {} LIMIT order for {}".format(side, buy_asset.market))

_id = buy_asset.limit_hidden_order(side)

if _id:
    time.sleep(5)
    logger.info("{} Stop-loss: {} {} @ {} BTC in progress only...".format(buy_asset.market, buy_asset.adjusted_size,
         buy_asset.name, get_format_price(buy_asset.stop_loss_price).format(buy_asset.stop_loss_price)))
    logger.info("Hidden {} LIMIT order has been made, exiting.".format(side))