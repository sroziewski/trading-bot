from library import setup_logger, BuyAsset, observe_lower_price_binance, price_to_string, take_profit, check_buy_assets, \
    get_remaining_btc_binance, adjust_buy_asset_btc_volume, ObserverStrategy, ObserveAsset, check_observe_assets, \
    get_remaining_btc_kucoin, observe_lower_price_kucoin

# ba = BuyAsset('ZRX', 0.00002520, 0.00002420, 0.00005520, 1)
# take_profit(ba)

exchange = 'kucoin'

def main():
    price_buy_strategy()


def price_buy_strategy():
    logger = setup_logger("price_buy_strategy")
    buy_assets = [
        BuyAsset(exchange, 'CHR', 0.00000274, 0.00000250, 0.00000294, 96, 15),
        BuyAsset(exchange, 'VRA', 0.0000000959, 0.0000000939, 0.0000000989, 96, 15),
    ]
    btc_value = get_remaining_btc_kucoin()
    adjust_buy_asset_btc_volume(buy_assets, btc_value)
    check_buy_assets(buy_assets)
    logger.info("{} -- Starting price_buy_strategy :\n{}".format(exchange,
        '\n'.join(map(lambda _a: "{}\tbuy : {}\tstop : {}\tprofit : {}".format(_a.name, price_to_string(_a.price),
                                                                               price_to_string(_a.stop_loss_price),
                                                                               price_to_string(_a.price_profit)),
                      buy_assets))))
    observe_lower_price_kucoin(buy_assets)
    logger.info("price_buy_strategy -- has finished")
    logger.info("Stop-loss and taking profits in progress only...")


def price_observer_strategy():
    logger = setup_logger("price_observer_strategy")
    buy_assets = [
        ObserveAsset('STORJ', 0.00002137, 0.00002080, 0.00004036, 15)
    ]
    check_observe_assets(buy_assets)
    logger.info("Starting price_observer_strategy :\n{}".format(
        '\n'.join(map(lambda _a: "{}\tbuy : {}\tstop : {}\tprofit : {}".format(_a.name, price_to_string(_a.buy_price),
                                                                               price_to_string(_a.stop_loss_price),
                                                                               price_to_string(_a.price_profit)),
                      buy_assets))))
    for buy_asset in buy_assets:
        ObserverStrategy(buy_asset).run()

    logger.info("price_observer_strategy -- has finished")
    logger.info("Stop-loss and taking profits in progress only...")


if __name__ == "__main__":
    main()
