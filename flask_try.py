import requests
import json

from depth_crawl import DepthCrawl, BuyDepth, SellDepth


def create_depth_crawl_dict():
    _response = requests.get("http://127.0.0.1:5000/qu3ry/dfjkhn98437jnbnljudWNI89283123123sfsdfgfpJH")
    _response_dict = json.loads(json.loads(_response.text))

    _depth_crawl_dict = {}

    for _item in _response_dict.items():
        _dc = DepthCrawl(_item[1]['market'], _item[1]['type'])

        _buys = extract_market_depths(_item, "buy_depth_5m")
        _sells = extract_market_depths(_item, "sell_depth_5m")
        _dc.buy_depth_5m = _buys
        _dc.sell_depth_5m = _sells

        _buys = extract_market_depths(_item, "buy_depth_15m")
        _sells = extract_market_depths(_item, "sell_depth_15m")
        _dc.buy_depth_15m = _buys
        _dc.sell_depth_15m = _sells

        _buys = extract_market_depths(_item, "buy_depth_1d")
        _sells = extract_market_depths(_item, "sell_depth_1d")
        _dc.buy_depth_1d = _buys
        _dc.sell_depth_1d = _sells

        _depth_crawl_dict[_item[0]] = _dc

    return _depth_crawl_dict


def extract_market_depths(_item, _type):
    _out = []
    for _md_15m in _item[1][_type]:
        if 'buy' in _type:
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

depth_crawl_dict = create_depth_crawl_dict()

i = 1