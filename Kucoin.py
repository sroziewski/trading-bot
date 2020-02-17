
from kucoin.client import Client
from kucoin.exceptions import KucoinAPIException, KucoinRequestException, MarketOrderException, LimitOrderException
import pytest


# symbol (string) – Name of symbol e.g. KCS-BTC
# kline_type (string) – type of symbol, type of candlestick patterns: 1min, 3min, 5min, 15min, 30min, 1hour, 2hour, 4hour, 6hour, 8hour, 12hour, 1day, 1week
# start (int) – Start time as unix timestamp (optional) default start of day in UTC
# end (int) – End time as unix timestamp (optional) default now in UTC
from library import get_klines, get_kucoin_klines, get_kucoin_interval_unit, get_binance_klines, SellAsset, stop_signal, \
    sat, cancel_kucoin_current_orders, setup_logger, get_or_create_kucoin_trade_account, sell_limit_stop_loss

klines_k = get_kucoin_klines("LTC-BTC", "1hour")
klines_b = get_binance_klines("LTCBTC", "1h", '6 hours ago')
#
# klines = get_klines("BNBBTC", '1h', '6 hours ago')


stop_price_in_satoshi = 777999

stop_price = stop_price_in_satoshi * sat


acc = get_or_create_kucoin_trade_account('KCS')

sell_asset_binance = SellAsset("binance", "LTC", stop_price, True, '1h')
sell_asset_kucoin = SellAsset("kucoin", "VIDT", stop_price, True, '1h')

logger = setup_logger(sell_asset_kucoin.name)
logger.info("Starting {} stop-loss maker on {}".format(sell_asset_kucoin.market, sell_asset_kucoin.exchange))
logger.info("Stop price is set up to : {:.8f} BTC".format(stop_price))

# cancel_kucoin_current_orders("VIDT-BTC")


sell_limit_stop_loss(sell_asset_kucoin.market, sell_asset_kucoin)

out1 = stop_signal("binance", sell_asset_binance.market, sell_asset_binance.ticker, stop_price, 1)
out2 = stop_signal("kucoin", sell_asset_kucoin.market, sell_asset_kucoin.ticker, stop_price, 1)


i = 1