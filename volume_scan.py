import datetime
from time import sleep

from binance.websockets import BinanceSocketManager

from library import TradeMsg

from library import get_binance_klines, get_binance_interval_unit, get_kucoin_klines, \
    get_kucoin_interval_unit, binance_obj, kucoin_client, DecimalCodec, try_get_klines, TradeMsg, get_last_db_record, \
    get_time_from_binance_tmstmp, logger_global
from market_scanner import VolumeCrawl
from mongodb import mongo_client

trades = {}

def process_trade_socket_message(_msg):
    _trade_msg = TradeMsg(_msg)
    trades[_trade_msg.market].append(_trade_msg)
    last_tmstmp = datetime.datetime.now().timestamp()


def do_volume_crawl(_vc):
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

_vc = VolumeCrawl("BTCUSDT", "binance")

do_volume_crawl(_vc)
