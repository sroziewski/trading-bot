from time import sleep

from bson import CodecOptions
from bson.codec_options import TypeRegistry

from library import get_pickled, save_to_file, DecimalCodec
from mongodb import mongo_client

path = "/home/0agent1/store/klines/"


db_markets_info = mongo_client.markets_info

decimal_codec = DecimalCodec()
type_registry = TypeRegistry([decimal_codec])
codec_options = CodecOptions(type_registry=type_registry)
usdt_markets_collection = db_markets_info.get_collection("usdt", codec_options=codec_options)
_market_info_cursor = usdt_markets_collection.find()
_market_info_list = [e for e in _market_info_cursor]


def get_klines(_path, _market, _ticker):
    _fname = "{}_{}".format(_market, _ticker)
    _klines = get_pickled(_path, _fname)
    return _klines


def test():
    while True:
        sleep(1)


for _market_s in _market_info_list:
    data = {}
    _market = "{}{}".format(_market_s['name'], "usdt")
    for _ticker in _market_s['tickers']:  # we did till hft
        if _ticker in ['15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w']:
            data[_ticker] = get_klines(path, _market, _ticker)
            if _ticker not in ['3d']:
                indices = [index for (index, item) in enumerate(data[_ticker]) if item.start_time == 1687132800000]
                _dt = 0
            else:
                indices = [index for (index, item) in enumerate(data[_ticker]) if item.start_time == 1686960000000]
                _dt = 1
            save_to_file(path + "/start/", "{}_{}".format(_market, _ticker), data[_ticker][0:indices[0] + _dt])
            print("{} {}".format(_ticker, get_pickled(path + "start/", "{}_{}".format(_market, _ticker))[-1].time_str))
    if _market == "hftusdt":
        break


print("END")
#
# l = []
#
# _tickers = ['15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w']
#
# data = {}
#
# for _t in _tickers:
#     data[_t] = get_klines(path, "adausdt", _t)
#
# for _t in _tickers:
#     if _t not in ['3d']:
#         indices = [index for (index, item) in enumerate(data[_t]) if item.start_time == 1687132800000]
#         _dt = 0
#     else:
#         indices = [index for (index, item) in enumerate(data[_t]) if item.start_time == 1686960000000]
#         _dt = 1
#     save_to_file(path+"/start/", "adausdt_{}".format(_t), data[_t][0:indices[0]+_dt])
#     print("{} {}".format(_t, get_pickled(path+"start/", "adausdt_{}".format(_t))[-1].time_str))
#
#
# i = 1
