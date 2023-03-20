import datetime

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


df = pd.read_csv('D:\\bin\\data\\BINANCE_AVAXUSDT_240.csv')

def nz(x, y=None):
    '''
    RETURNS
    Two args version: returns x if it's a valid (not NaN) number, otherwise y
    One arg version: returns x if it's a valid (not NaN) number, otherwise 0
    ARGUMENTS
    x (val) Series of values to process.
    y (float) Value that will be inserted instead of all NaN values in x series.
    '''
    if isinstance(x, np.generic):
        return x.fillna(y or 0)
    if x != x:
        if y is not None:
            return y
        return 0
    return x


def n1(_v):
    _v1 = _v.copy()
    for _i in range(len(_v1)):
        if _i > 0:
            _v1[_i] = _v[_i - 1]
    return _v1


def f(a,b):
    df['l0'] = (1 - b) * a
    df['l0_1'] = df['l0'].shift(1)

    return df['l0'] + b*df['l0_1']


def compute_tr(_data):
    return _data['high']-_data['low']


def get_crossup(_data, Lower_Threshold_of_Approximability2): # +1
    return np.logical_and(_data[1:]['low'] > Lower_Threshold_of_Approximability2[:-1], _data.iloc[:-1, :]['low'] <= Lower_Threshold_of_Approximability2[:-1])
    # return list(map(lambda x: int(not x), _data[1:]['low'] > Lower_Threshold_of_Approximability2[:-1]))# , _data.iloc[:-1, :]['low'] <= Lower_Threshold_of_Approximability2[:-1])
    # return list(map(lambda x: int(x), _data.iloc[:-1, :]['low'] <= Lower_Threshold_of_Approximability2[:-1]))# , _data.iloc[:-1, :]['low'] <= Lower_Threshold_of_Approximability2[:-1])


def get_crossdn(_data, Upper_Threshold_of_Approximability2):
    return np.logical_and(_data[1:]['high'] < Upper_Threshold_of_Approximability2[:-1], _data.iloc[:-1, :]['high']   >= Upper_Threshold_of_Approximability2[:-1])


def smooth(_scalars, weight=0.8):  # Weight between 0 and 1
    l0 = np.zeros(len(_scalars))
    l1 = np.zeros(len(_scalars))
    l2 = np.zeros(len(_scalars))
    l3 = np.zeros(len(_scalars))

    for i in range(len(_scalars)):
        if i == 0:
            l0[i] = (1 - weight) * _scalars[i]
            l1[i] = -weight * l0[i]
            l2[i] = -weight * l1[i]
            l3[i] = -weight * l2[i]
        else:
            l0[i] = (1 - weight) * _scalars[i] + weight * l0[i-1]
            l1[i] = -weight * l0[i] + l0[i-1] + weight * l1[i-1]
            l2[i] = -weight * l1[i] + l1[i-1] + weight * l2[i-1]
            l3[i] = -weight * l2[i] + l2[i-1] + weight * l3[i-1]

    return (l0 + 2*l1 + 2*l2 + l3)/6


def lele(_open, _close, _high, _low, _val, _strength):
    _bindex = np.zeros(len(_open))
    _sindex = np.zeros(len(_open))
    _ret = np.zeros(len(_open))
    for i in range(len(_open)):
        if i > 0:
            _bindex[i] = _bindex[i-1]
            _sindex[i] = _sindex[i-1]
        if i > 3:
            if _close[i] > _close[i-4]:
                _bindex[i] = _bindex[i] + 1
            if _close[i] < _close[i-4]:
                _sindex[i] = _sindex[i] + 1

        if i == 52:
            k=2
        if _bindex[i] > _val and _close[i] < _open[i] and _high[i] >= np.max(_high[i-10:i]):
            _bindex[i] = 0
            _ret[i] = -1
        if _sindex[i] > _val and _close[i] > _open[i] and _low[i] <= np.min(_low[i-10:i]):
            _sindex[i] = 0
            _ret[i] = 1
    return _ret


def find_indices(list_to_check, item_to_find):
    indices = []
    for idx, value in enumerate(list_to_check):
        if value == item_to_find:
            indices.append(idx)
    return indices


conjectures = list(map(lambda x: smooth(df['open'], x), np.arange(0.1, 1.0, 0.05)))
amlag = np.mean(conjectures, axis=0)
tr = compute_tr(df)
inapproximability = np.mean(list(map(lambda x: smooth(tr, x), np.arange(0.1, 1.0, 0.05))), axis=0)

Upper_Threshold_of_Approximability1 = amlag + inapproximability*1.618
Upper_Threshold_of_Approximability2 = amlag + 2*inapproximability*1.618
Lower_Threshold_of_Approximability1 = amlag - inapproximability*1.618
Lower_Threshold_of_Approximability2 = amlag - 2*inapproximability*1.618

crossup = get_crossup(df, Lower_Threshold_of_Approximability2)
crossdn = get_crossdn(df, Upper_Threshold_of_Approximability2)

_out = lele(df['open'], df['close'], df['high'], df['low'], 2, 20)
indexes =  find_indices(_out, True)

times =[]
for i in indexes:
    time_s = datetime.datetime.fromtimestamp(df['time'].iloc[i]).strftime('%d %B %Y %H:%M:%S')
    times.append((i, time_s))

j=0
for c in crossup:
    if c and Lower_Threshold_of_Approximability2[j]:
        time_s = datetime.datetime.fromtimestamp(df['time'].iloc[j]).strftime('%d %B %Y %H:%M:%S')
        k=1

    j = j + 1