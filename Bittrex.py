from binance.client import Client
from bittrex.bittrex import *

bittrex_obj = Bittrex("5c62462a57f54503915699ebe3829d1a", "22ccfeddb3d246888d8e125b5eaea5cb", api_version=API_V2_0)

a = bittrex_obj.get_balance('ETH')


def get_klines_1(_asset, _time_interval):
    try:
        return bittrex_obj.get_klines_currency(_asset.market, _asset.ticker, _time_interval)
    except TypeError:
        time.sleep(2)
        get_klines_1(_asset, _time_interval)


def get_klines_2(_market, _ticker, _time_interval):
    try:
        return bittrex_obj.get_candles(_market, _time_interval)
    except TypeError:
        time.sleep(2)
        get_klines_2(_market, _ticker, _time_interval)


r = get_klines_2("BTCETH", Client.KLINE_INTERVAL_1HOUR, 'oneMin')

i = 1
