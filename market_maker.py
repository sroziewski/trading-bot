from library import setup_logger, BuyAsset, observe_lower_price, price_to_string, take_profit, check_buy_assets, \
    get_remaining_btc, adjust_buy_asset_btc_volume

logger = setup_logger("price-observer")
# ba = BuyAsset('ZRX', 0.00002520, 0.00002420, 0.00005520, 1)
# take_profit(ba)

buy_assets = [
    BuyAsset('CELR', 0.00000118, 0.00000101, 0.00000213, 100, 10)
]

btc_value = get_remaining_btc()

adjust_buy_asset_btc_volume(buy_assets, btc_value)

check_buy_assets(buy_assets)

logger.info("Starting observing assets:\n{}".format(
    '\n'.join(map(lambda _a: "{} :\t{}".format(_a.name, price_to_string(_a.price)), buy_assets))))
observe_lower_price(buy_assets)

logger.info("observe_lower_price -- has finished.\nStop-loss and taking profits in progress only...")

