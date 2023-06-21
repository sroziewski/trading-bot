import threading
from time import sleep

from library import get_pickled, save_to_file

path = "E:/bin/data/klines/"


def get_klines(_path, _market, _ticker):
    _fname = "{}_{}".format(_market, _ticker)
    _klines = get_pickled(_path, _fname)
    return _klines


def test():
    while True:
        sleep(1)


l = []

_tickers = ['15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w']

data = {}

for _t in _tickers:
    data[_t] = get_klines(path, "adausdt", _t)

for _t in _tickers:
    indices = [index for (index, item) in enumerate(data[_t]) if item.start_time == 1687132800000]
    save_to_file("E:/bin/data/klines/start/", "adausdt_{}".format(_t), data[_t][0:indices[0]])


i = 1
