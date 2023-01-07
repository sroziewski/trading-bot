from binance.client import Client as BinanceClient
from binance.exceptions import BinanceAPIException
from bson import CodecOptions
from bson.codec_options import TypeRegistry
from kucoin.exceptions import KucoinAPIException
from pymongo.errors import PyMongoError

import pickle

from library import get_binance_klines, get_binance_interval_unit, setup_logger, get_kucoin_klines, \
    get_kucoin_interval_unit, binance_client, kucoin_client, DecimalCodec, try_get_klines, TradeMsg, get_last_db_record, \
    get_time_from_binance_tmstmp

positions = binance_client.futures_account()['positions']

# x = {}
#
# x['binance'] = ['xFGUEfa3vVrbBvvmNQL3E8A5Mu1Zsr4fApspT2oQgA2L3H1oKjfmWs2mzk8Yi9KJ', 'xBDbBuGorVcl3hEH2yWsVHBFzVoGa59UmVZQ7EIOQoRJKFiEhVDdeLPWG9CUEb9X']
#
# with open('e://bin//data//keys.pkl', 'wb') as handle:
#     pickle.dump(x, handle, protocol=pickle.HIGHEST_PROTOCOL)


# https://github.com/sammchardy/python-binance/issues/536

# https://binance-docs.github.io/apidocs/futures/en/#position-information-v2-user_data

i = 1