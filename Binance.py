from functools import reduce

from binance.client import Client
import pandas as pd
import numpy as np
import threading
from binance.websockets import BinanceSocketManager
from scipy.signal import gaussian
from scipy.ndimage import filters
from scipy import signal
import operator


class Binance:
    def __init__(self, key_api, key_secret):
        self.client = Client(key_api, key_secret)

    def get_all_currencies(self):
        return list(map(lambda x: x['symbol'], self.client.get_orderbook_tickers()))

    def get_all_bnb_currencies(self):
        return list(filter(lambda x: "BNB" in x, self.get_all_currencies()))

    def get_all_btc_currencies(self, _exclude_markets=[]):
        return list(filter(lambda y: y not in _exclude_markets, filter(lambda x: "BTC" in x, self.get_all_currencies())))

    # def get_klines_currency2(self, currency, datetime):
    #     return self.client.get_historical_klines(currency, Client.KLINE_INTERVAL_15MINUTE, datetime)

    def get_klines_currency(self, currency, kline, datetime):
        return self.client.get_historical_klines(currency, kline, datetime)

    def get_margin_bnb_currencies(self):
        return self.get_margin_currencies(self.get_all_bnb_currencies())

    def get_margin_btc_currencies(self, datetime="1 day ago", level=35.0):
        return self.get_margin_currencies_2(self.get_all_btc_currencies(), datetime, level)

    def currency_efficiency_measure(self, mean_rsi, mean_candle_height, level):
        relative_rsi = (-mean_rsi+level)/100.0
        return 2/(1/relative_rsi + 1/mean_candle_height)

    def get_margin_currencies_2(self, currencies, datetime, level=35.0):
        r = []
        for currency in currencies:
            lower, upper, closes, diff_high_low, highs, lows = self.bollinger_bands_currency(currency, datetime)

            last_lower_band = lower['col'].loc[len(lower.index) - 2]
            last_lows = list(map(lambda x: x < last_lower_band, [float(lows[-2]), float(lows[-1]), float(lows[-3]), float(lows[-4])]))

            if reduce(operator.or_, last_lows):
                mean_candle_height = np.mean(np.divide(diff_high_low[len(closes) - 25:-1], np.array(list(map(lambda x: float(x), highs[len(closes) - 25:-1])))))
                rsi = self.rsi(currency, datetime)
                mean_rsi = np.mean(rsi[len(rsi)-25:])
                r.append((currency, mean_rsi, self.currency_efficiency_measure(mean_rsi, mean_candle_height, level)))

        top_currencies = map(lambda x: (x[0], x[1], x[2]), sorted(r, key=lambda tup: tup[2], reverse=True))
        top_currencies = list(filter(lambda x: x[1] < level, top_currencies))[:10]
        print(top_currencies)

        return list(map(lambda x: x[0],  top_currencies))

    def get_margin_currencies(self, currencies, datetime, level=35.0):
        r = []
        for currency in currencies:
            lower, upper, closes, diff_high_low, highs, lows = self.bollinger_bands_currency(currency, datetime)
            rsi = self.rsi(currency, datetime)
            # print(currency, rsi[-1])
            if rsi[-1] < level:
                last_lower_band = lower['col'].loc[len(lower.index) - 2]
                last_close = closes[-2]
                if last_close < last_lower_band:
                    last_relative_change = diff_high_low[-2] / float(highs[-2])
                    # print(currency, "diff", diff_high_low[-2], "high", highs[-2], "last_relative_change", last_relative_change)
                    # mean_price = np.mean((closes[len(closes)-12:-2] - closes[-2]) / closes[-2])
                    # mean_price = np.mean(closes[len(closes)-12:-2])
                    # subs = upper['col'][len(lower.index)-12:-2] - lower['col'][len(lower.index)-12:-2]
                    # smp = np.mean(subs / mean_price)
                    if last_relative_change > 0.02: # change must be greater than 2%
                        mean_candle_height = np.mean(diff_high_low[len(closes)-22:-2] / float(highs[-2]))
                        # we want to now the mean variability during 10 hrs
                        # print(currency, np.mean((closes[10:-4]-closes[-4])/closes[-4]))
                        # print(currency, mean_candle_height/mean_price)
                        r.append((currency, mean_candle_height))
        # sort by highest priceChangePercent
        # price_changes = list(map(lambda x: (x, self.client.get_ticker(symbol=x)['priceChangePercent']), r))
        # price_changes = list(map(lambda x: (x, mean_candle_height), r))

        return list(map(lambda x: x[0], sorted(r, key=lambda tup: tup[1], reverse=True)))

    def _threading_listen(self, currency, po):
        bm = BinanceSocketManager(self.client)
        bm.start_trade_socket(currency, po.start_observing)
        bm.start()

    def listen_to_currency(self, currency):
        po = PriceObserver()

        t = threading.Thread(target=self._threading_listen, args=(currency, po))
        t.start()
        order = Order(self.client, currency, po)
        price_buy = order.buy()
        order.sell()
        # feed data cont.
        # buy
        # sell
        # repeat process

    def _left_gaussian(self, y):
        npts = len(y)
        b = gaussian(npts, 5)
        s1 = int(npts / 2)
        s2 = s1
        if npts % 2 == 1:
            s1 += 1
            s2 = s1 - 1
        b1 = np.ones(s1)
        b2 = np.zeros(s2)
        b *= np.concatenate([b1, b2])
        ga = filters.convolve1d(y, b / b.sum())

        return ga

    def filter_signal(self, rsi):
        rsi_filter = self._left_gaussian(rsi)
        return rsi_filter

    def find_extrema(self, sig):
        max_peakind = signal.find_peaks_cwt(sig, np.arange(3, 6))
        min_peakind = signal.find_peaks_cwt(-sig, np.arange(3, 6))
        return min_peakind, max_peakind

    def filter_extrema(self, min, max):
        d_min = set(min)
        d_max = set(max)
        s = np.concatenate([min, max])
        s.sort()
        mins = []
        maxs = []
        for i in range(len(s)):
            if s[i] in d_min:
                if i==0:
                    mins.append(s[i])
                elif s[i-1] not in d_min:
                    mins.append(s[i])
            if s[i] in d_max:
                if i==0:
                    maxs.append(s[i])
                elif s[i-1] not in d_max:
                    maxs.append(s[i])
        return mins, maxs

    def filter_signal(self, min, max, rsi):
        i = 0


    def bollinger_bands_currency(self, currency, datetime):
        klines = self.get_klines_currency(currency, datetime)
        diff_high_low = np.array(list(map(lambda x: float(x[2])-float(x[3]), klines)), dtype=np.float32)
        highs = list(map(lambda x: x[2], klines))
        lows = list(map(lambda x: x[3], klines))
        closes = np.array(list(map(lambda x: x[4], klines)), dtype=np.float32)


        lows_pd = pd.DataFrame(lows, columns=['col'])
        closes_pd = pd.DataFrame(closes, columns=['col'])

        movavg = pa.rolling_mean(closes_pd, 20, min_periods=20)
        movstddev = pa.rolling_std(closes_pd, 20, min_periods=20)

        upperband = movavg + 2 * movstddev
        lowerband = movavg - 2 * movstddev

        return lowerband, upperband, closes, diff_high_low, highs, lows

    def bollinger_bands_currency2(self, currency, datetime):
        klines = self.get_klines_currency(currency, datetime)
        diff_high_low = np.array(list(map(lambda x: float(x[2])-float(x[3]), klines)), dtype=np.float32)
        highs = list(map(lambda x: x[2], klines))
        lows = list(map(lambda x: x[3], klines))
        closes = np.array(list(map(lambda x: x[4], klines)), dtype=np.float32)


        lows_pd = pd.DataFrame(lows, columns=['col'])
        closes_pd = pd.DataFrame(closes, columns=['col'])

        movavg = pa.rolling_mean(closes_pd, 20, min_periods=20)
        movstddev = pa.rolling_std(closes_pd, 20, min_periods=20)

        upperband = movavg + 2 * movstddev
        lowerband = movavg - 2 * movstddev

        return lowerband, upperband, closes, diff_high_low

    def rsi(self, currency, datetime, n=14):

        klines = self.get_klines_currency(currency, datetime)
        closes = list(map(lambda x: x[4], klines))

        prices = np.array(closes, dtype=np.float32)

        deltas = np.diff(prices)
        seed = deltas[:n + 1]
        up = seed[seed >= 0].sum() / n
        down = -seed[seed < 0].sum() / n
        rs = up / down
        rsi = np.zeros_like(prices)
        rsi[:n] = 100. - 100. / (1. + rs)

        for i in range(n, len(prices)):
            delta = deltas[i - 1]  # cause the diff is 1 shorter

            if delta > 0:
                upval = delta
                downval = 0.
            else:
                upval = 0.
                downval = -delta

            up = (up * (n - 1) + upval) / n
            down = (down * (n - 1) + downval) / n

            rs = up / down
            rsi[i] = 100. - 100. / (1. + rs)

        return rsi

    def stoch_rsi(self, currency, n=14):
        rsi = self.rsi(currency)[n:]
        llow = np.min(rsi)
        hhigh = np.max(rsi)
        return 100 * (rsi - llow) / (hhigh - llow)

    def moving_average(self, values, window):
        weigths = np.repeat(1.0, window) / window
        smas = np.convolve(values, weigths, 'valid')
        return smas  # as a numpy array

    def exp_moving_average(self, values, window):
        weights = np.exp(np.linspace(-1., 0., window))
        weights /= weights.sum()
        a = np.convolve(values, weights, mode='full')[:len(values)]
        a[:window] = a[window]
        return a

    def computeMACD(self, x, slow=26, fast=12):
        """
        compute the MACD (Moving Average Convergence/Divergence) using a fast and slow exponential moving avg'
        return value is emaslow, emafast, macd which are len(x) arrays
        """
        emaslow = self.exp_moving_average(x, slow)
        emafast = self.exp_moving_average(x, fast)
        return emaslow, emafast, emafast - emaslow
