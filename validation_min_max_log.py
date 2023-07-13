

filename = 'E:/moba/ta-min_max_finder_ada.log'

with open(filename) as file:
    lines = [line.rstrip() for line in file]


def post_processing(_list):
    _filtered = list(map(lambda x: x.split(" i: ")[1], filter(lambda x: 'Computation time ' not in x, _list)))
    sei_list = []
    for _el in _filtered:
        _split_0 = _el.split(" ")
        _market = _split_0[1]
        _ticker = _split_0[2]
        _signal_start = _split_0[4].replace("(", "")
        _split_1 = _el.split(") ")
        _buys_count_tmp = _split_1[1]
        if 'to' not in _buys_count_tmp:
            _price = float(_buys_count_tmp.split(" ")[1])
            sei = SetupEntryItem(_market, _ticker, "buy", _signal_start)
            sei.price = _price
            sei.buys_count = int(_buys_count_tmp.split(" ")[0])
            asf = 1
        else:
            sei = SetupEntryItem(_market, _ticker, "sell", _signal_start)
            sei.signal_stop = int(_buys_count_tmp.split("(")[1].split(" ")[0])
            _vfi = int(_buys_count_tmp.split("(")[1].split(" ")[0])
            sei.vfi = _vfi
            asf = 1
        sei_list.append(sei)
    return _filtered


class SetupEntryItem(object):
    def __init__(self, market, ticker, signal_type, signal_start):
        self.market = market
        self.ticker = ticker
        self.signal_type = signal_type
        self.signal_start = int(signal_start)
        self.signal_stop = None
        self.price = None
        self.buys_count = None
        self.vfi = None


data = post_processing(lines)

a = 1