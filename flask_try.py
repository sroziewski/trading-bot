import requests
import json

from config import config
from depth_crawl import DepthCrawl, BuyDepth, SellDepth
from library import binance_obj, lib_initialize

flask_token = config.get_parameter('flask_token')
mongo_ip = config.get_parameter('mongo_ip')
flask_port = config.get_parameter('flask_port')


def extract_depth_crawl_dict():
    _response = requests.get("http://{}:{}/qu3ry/{}".format(mongo_ip, flask_port, flask_token))
    _response_dict = json.loads(json.loads(_response.text))

    _depth_crawl_dict = {}

    for _item in _response_dict.items():
        _market = _item[1]['market']
        _type = _item[1]['type']
        _dc = DepthCrawl(_market, _type)

        _buys = __extract_market_depths(_item[1], "buy_depth_5m")
        _sells = __extract_market_depths(_item[1], "sell_depth_5m")
        _dc.buy_depth_5m = _buys
        _dc.sell_depth_5m = _sells

        _buys = __extract_market_depths(_item[1], "buy_depth_15m")
        _sells = __extract_market_depths(_item[1], "sell_depth_15m")
        _dc.buy_depth_15m = _buys
        _dc.sell_depth_15m = _sells

        _buys = __extract_market_depths(_item[1], "buy_depth_1d")
        _sells = __extract_market_depths(_item[1], "sell_depth_1d")
        _dc.buy_depth_1d = _buys
        _dc.sell_depth_1d = _sells

        _depth_crawl_dict[_item[0]] = _dc

    return _depth_crawl_dict


def extract_market_depth(_market):
    _response = requests.get("http://{}:{}/qu3ry/{}/{}".format(mongo_ip, flask_port, _market, flask_token))
    _response_dict = json.loads(json.loads(_response.text))
    _dc = DepthCrawl(_response_dict['market'], _response_dict['type'])

    _buys = __extract_market_depths(_response_dict, "buy_depth_5m")
    _sells = __extract_market_depths(_response_dict, "sell_depth_5m")
    _dc.buy_depth_5m = _buys
    _dc.sell_depth_5m = _sells

    _buys = __extract_market_depths(_response_dict, "buy_depth_15m")
    _sells = __extract_market_depths(_response_dict, "sell_depth_15m")
    _dc.buy_depth_15m = _buys
    _dc.sell_depth_15m = _sells

    _buys = __extract_market_depths(_response_dict, "buy_depth_1d")
    _sells = __extract_market_depths(_response_dict, "sell_depth_1d")
    _dc.buy_depth_1d = _buys
    _dc.sell_depth_1d = _sells
    return _dc


def __extract_market_depths(_item, _depth_type):
    _out = []
    for _md_15m in _item[_depth_type]:
        if 'buy' in _depth_type:
            _md = BuyDepth(_md_15m['bid_price'], _md_15m['p1'], _md_15m['p2'], _md_15m['p3'], _md_15m['p4'],
                       _md_15m['p5'], _md_15m['p10'], _md_15m['p15'], _md_15m['p20'], _md_15m['p25'],
                       _md_15m['p30'], _md_15m['p35'], _md_15m['p40'], _md_15m['p45'], _md_15m['p50'],
                       _md_15m['p55'], _md_15m['p60'], _md_15m['p65'], _md_15m['p70'])
        else:
            _md = SellDepth(_md_15m['ask_price'], _md_15m['p1'], _md_15m['p2'], _md_15m['p3'], _md_15m['p4'],
                           _md_15m['p5'], _md_15m['p10'], _md_15m['p15'], _md_15m['p20'], _md_15m['p25'],
                           _md_15m['p30'], _md_15m['p35'], _md_15m['p40'], _md_15m['p45'], _md_15m['p50'],
                           _md_15m['p55'], _md_15m['p60'], _md_15m['p65'], _md_15m['p70'])
        _md.set_time(_md_15m['timestamp'])
        _out.append(_md)
    return _out


depth_crawl_dict = extract_depth_crawl_dict()
md = extract_market_depth("ltcusdt")
i = 1