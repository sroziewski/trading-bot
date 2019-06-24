from library import setup_logger, BuyAsset, observe_lower_price, price_to_string, take_profit, check_buy_assets, \
    get_remaining_btc, adjust_buy_asset_btc_volume

logger = setup_logger("price-observer")
ba = BuyAsset("XRP", 0.00004739, 0.00004639, 0.00009439, 100)
# take_profit(ba)


# take_profit(ba)

buy_assets = [BuyAsset('SYS', 0.00000419, 0.00000404, 0.00000832, 50),
              BuyAsset('XRP', 0.00004086, 0.00003956, 0.00011021, 100)]

btc_value = get_remaining_btc()

adjust_buy_asset_btc_volume(buy_assets, btc_value)

logger.info("Starting observing assets:\n{}".format(
    '\n'.join(map(lambda _a: "{} :\t{}".format(_a.name, price_to_string(_a.price)), buy_assets))))

check_buy_assets(buy_assets)

observe_lower_price(buy_assets)

logger.info("Stop loss and taking profits in progress only...")

