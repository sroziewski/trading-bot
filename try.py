from time import sleep

from library import get_pickled, save_to_file

path = "/home/0agent1/store/klines/"


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
    if _t not in ['3d']:
        indices = [index for (index, item) in enumerate(data[_t]) if item.start_time == 1687132800000]
        _dt = 0
    else:
        indices = [index for (index, item) in enumerate(data[_t]) if item.start_time == 1686960000000]
        _dt = 1
    save_to_file(path+"/start/", "adausdt_{}".format(_t), data[_t][0:indices[0]+_dt])
    print("{} {}".format(_t, get_pickled(path+"start/", "adausdt_{}".format(_t))[-1].time_str))


i = 1
