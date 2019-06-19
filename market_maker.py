import sys

import numpy as np
import talib

from binance.client import Client

from library import setup_logger, BuyAsset, observe_lower_price, price_to_string, get_interval_unit, binance, \
    get_pickled, sell_limit, cancel_current_orders, take_profit


logger = setup_logger("price-observer")
ba = BuyAsset("NULS", 0.00000020, 0.00000014, 0.00000030, 100)
# take_profit(ba)


take_profit(ba)

buy_assets = [BuyAsset('HOT', 0.00000020, 0.00000014, 0.00000030, 100),
              BuyAsset('WTC', 0.0001823, 0.0001723, 0.0002023), BuyAsset('NANO', 0.0001470, 0.0001570, 0.0002570)]

logger.info("Starting observing assets:\n{}".format(
    '\n'.join(map(lambda _a: "{} :\t{}".format(_a.name, price_to_string(_a.price)), buy_assets))))

observed = observe_lower_price(buy_assets)
