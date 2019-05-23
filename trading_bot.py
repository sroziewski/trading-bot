from binance.client import Client
from binance.enums import KLINE_INTERVAL_30MINUTE, KLINE_INTERVAL_1DAY
from binance.exceptions import BinanceAPIException
import threading
import talib
import numpy as np

from Binance import Binance

token1 = "I8HDSd7SuucmdsmwmTrsF8tI3UJcowFwY8iaLXhC7t1ZPbpPfD87VLIJJGkbSWL9"
token2 = "glbvXdxiDZgcspm0AV4Yu2fw08p4yXEUZcLxkUpu3DIsjqtPG0oPxnyDbZ06sgi7"


client = Client(token1, token2)

# get market depth
market = 'WTCBTC'
depth = client.get_order_book(symbol=market)

prices = client.get_ticker(symbol=market)
candles = client.get_klines(symbol=market, interval=KLINE_INTERVAL_1DAY)

orders = client.get_open_orders()

candles = client.get_klines(symbol=market, interval=KLINE_INTERVAL_30MINUTE)

i = 1