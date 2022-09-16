import datetime
import threading
from time import sleep

import schedule

from binance.websockets import BinanceSocketManager

from library import TradeMsg, BuyVolumeUnit, SellVolumeUnit, setup_logger, VolumeContainer

from library import get_binance_klines, get_binance_interval_unit, get_kucoin_klines, \
    get_kucoin_interval_unit, binance_obj, kucoin_client, DecimalCodec, try_get_klines, TradeMsg, get_last_db_record, \
    get_time_from_binance_tmstmp, logger_global
from market_scanner import VolumeCrawl
from mongodb import mongo_client

trades = {}

logger = setup_logger("Binance-Volume-Scanner")

def process_trade_socket_message(_msg):
    _trade_msg = TradeMsg(_msg)
    while "lock" in trades[_trade_msg.market]:
        sleep(1)
    trades[_trade_msg.market].append(_trade_msg)  # add lock here
    last_tmstmp = datetime.datetime.now().timestamp()


def process_volume():
    volume_ticker = '5m'

    _markets = list(trades.keys())

    _bag = {}

    for _market in _markets:
        trades[_market].append("lock")
        _bag[_market] = trades[_market].copy()
        _bag[_market] = _bag[_market][:-1]  # we skip the lock element
        trades[_market].clear()
        _buy_volume = filter(lambda x: x.buy, _bag[_market])
        _sell_volume = filter(lambda x: x.sell, _bag[_market])
        _bv = BuyVolumeUnit(_buy_volume)
        _sv = SellVolumeUnit(_sell_volume)
        VolumeContainer(_market, volume_ticker, _start_time, _bv, _sv)
        _bag[_market].clear()

    print("A")



def _do_volume_crawl(_vc):
    trades[_vc.market] = []
    last_tmstmp = datetime.datetime.now().timestamp()
    _bm = BinanceSocketManager(binance_obj.client)
    _conn_key = _bm.start_aggtrade_socket(_vc.market, process_trade_socket_message)
    _bm.start()
    while True:
        _tmstmp_diff = datetime.datetime.now().timestamp() - last_tmstmp
        if _tmstmp_diff > 60 * 60:
            logger_global[0].warning(
                f"{_vc.market} last trading volume tmstmp ({last_tmstmp}) is older than 60 minutes, diff = {_tmstmp_diff}")
        if len(trades[_vc.market]) > 0:
            logger_global[0].info(f"{_vc.market} last trading volume : {trades[_vc.market][-1].timestamp_str} {_vc.market}")
        sleep(60 * 60)


def manage_depth_crawling(_vc):
    _crawler = threading.Thread(target=_do_volume_crawl, args=(_vc,),
                                name='_do_volume_crawl : {}'.format(_vc.market))
    _crawler.start()


def to_mongo(_vc):
    return {
        'exchange': _vc.exchange,
        'ticker': _vc.ticker,
        'start_time': _vc.start_time,
        'total_base_volume': _vc.base_volume,
        'total_quantity': _vc.quantity,
        'start_time_str': _vc.time_str,
        'market': _vc.market,
        'avg_weighted_price': _vc.avg_weighted_price,
        'buy_volume': {
            'base_volume': _vc.base_volume,
            'quantity': _vc.quantity,
            'l00': _vc.l00,
            'l01': _vc.l01,
            'l02': _vc.l02,
            'l0': _vc.l0,
            'l0236': _vc.l0236,
            'l0382': _vc.l0382,
            'l05': _vc.l05,
            'l0618': _vc.l0618,
            'l0786': _vc.l0786,
            'l1': _vc.l1,
            'l1382': _vc.l1382,
            'l162': _vc.l162,
            'l2': _vc.l2,
            'l5': _vc.l5,
            'l10': _vc.l10,
            'l20': _vc.l20,
            'l50': _vc.l50,
            'l100': _vc.l100
        },
        'sell_volume': {
            'base_volume': _vc.base_volume,
            'quantity': _vc.quantity,
            'l00': _vc.l00,
            'l01': _vc.l01,
            'l02': _vc.l02,
            'l0': _vc.l0,
            'l0236': _vc.l0236,
            'l0382': _vc.l0382,
            'l05': _vc.l05,
            'l0618': _vc.l0618,
            'l0786': _vc.l0786,
            'l1': _vc.l1,
            'l1382': _vc.l1382,
            'l162': _vc.l162,
            'l2': _vc.l2,
            'l5': _vc.l5,
            'l10': _vc.l10,
            'l20': _vc.l20,
            'l50': _vc.l50,
            'l100': _vc.l100
        }
    }



_vc = VolumeCrawl("BTCUSDT", "binance")

manage_depth_crawling(_vc)


schedule.every(1).minutes.do(process_volume)

while True:
    # Checks whether a scheduled task
    # is pending to run or not
    schedule.run_pending()
    sleep(1)