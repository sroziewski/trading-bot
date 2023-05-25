import threading

from library import get_pickled, round_price
from library import ticker2num
from min_max_finder import extract_buy_entry_setup, SetupEntry, to_offline_kline

path = "E:/data/binance/klines/usdt/"


class ProcessingEntry(object):
    def __init__(self, _market, _ticker):
        self.market = _market
        self.ticker = _ticker


def manage_validation_processing(_pe):
    _crawler = threading.Thread(target=validate, args=(_pe,),
                                name='validate : {}')
    _crawler.start()
    print("Thread for {}".format(_pe.ticker))
    return _crawler


def get_klines(_path, _market, _ticker):
    _fname = "{}_{}".format(_market, _ticker)
    _klines = get_pickled(_path, _fname)
    return _klines


def validate(_pe):
    _klines = list(map(lambda x: to_offline_kline(x), get_klines(path, _pe.market, _pe.ticker)))
    _klines.reverse()
    _length = len(_klines) - 400 - 1
    for _i in range(1100, _length):
        _data = _klines[_i:400+_i]
        _se: SetupEntry = extract_buy_entry_setup(_data, _pe.market, _pe.ticker)
        if _i % 1000 == 0:
            print("{} {}".format(_pe.ticker, _i))
        if _se:
            if abs(_data[0]['kline']['start_time']-_se.time) < 2*ticker2num(_se.ticker)*60*60*1000:
                print("{} {} {} {} {}".format(_pe.ticker, _i, _se.time_str, _data[0]['kline']['time_str'], round_price(_se.buy_price)))


_tickers = ['15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d']

for _ticker in _tickers:
    # if _ticker == '4h':
    _pe = ProcessingEntry("avaxusdt", _ticker)
    manage_validation_processing(_pe)
    # extract_buy_entry_setup()

