import time

from kucoin.client import Client as KucoinClient

from library import setup_logger, \
    get_format_price, SellAsset, sat, round_float_price

exchange = 'kucoin'
side = KucoinClient.SIDE_SELL
logger = setup_logger("kucoin-hidden-order-{}".format(side))

currency = 'VRA'
price = 11.2
stop_loss_price = 2
profit_price = 5700.99
ratio = 1

sell_asset = SellAsset(exchange, currency, stop_loss_price * sat, price=price * sat, ratio=ratio, kucoin_side=side)

logger.info("Make hidden {} LIMIT order for {}".format(side, sell_asset.market))

# _id = sell_asset.keep_lowest_ask(1e5)
_id = sell_asset.limit_hidden_order()

if _id:
    time.sleep(5)
    logger.info("{} Stop-loss: {} {} @ {} BTC in progress only...".format(sell_asset.market, sell_asset.adjusted_size,
          sell_asset.name, get_format_price(sell_asset.stop_loss_price).format(sell_asset.stop_loss_price)))
    logger.info("Hidden {} LIMIT order has been made, exiting.".format(side))
