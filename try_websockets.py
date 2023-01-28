import asyncio
import time

from binance import ThreadedWebsocketManager
from library import binance_obj

symbol = 'BNBBTC'


loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

twm = ThreadedWebsocketManager(api_key=binance_obj.key_api, api_secret=binance_obj.key_secret)
# start is required to initialise its internal loop
twm.start()


def handle_socket_message(msg):
    print(f"message type: {msg['e']}")
    print(msg)


twm.start_kline_socket(callback=handle_socket_message, symbol=symbol)