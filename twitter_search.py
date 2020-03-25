import re
import time
import urllib.request
from library import authorize, send_mail, setup_logger

logger = setup_logger("binance-twitter-observer")
# authorize()

url_twitter = 'https://twitter.com/binance'
req_twitter = urllib.request.Request(url_twitter)

url_blog = 'https://www.binance.com/en/blog/'
req_blog = urllib.request.Request(url_blog)

pattern_s = re.compile('\\s+')
cnt = 0
t1 = 600
cycle = 0
phrase = 'community coin vote round'


def get_response(_req):
    with urllib.request.urlopen(_req) as response:
        _page_content = response.read()
        _content = pattern_s.sub(" ", str(_page_content).lower())
        return _content


while 1:
    _content_twitter = get_response(req_twitter)
    _content_blog = get_response(req_blog)
    if phrase in _content_twitter + _content_blog:
        cnt += 1
        send_mail("QQQ Community Coin Vote Round FOUND!!!", url_twitter)
        logger.info("Community Coin Vote Round -- found")
        time.sleep(t1)
        if cnt == 3:
            t1 *= 6
            cnt = 0
            cycle += 1
        if cycle == 3:
            cycle = 0
            cnt = 0
            t1 = 600
            time.sleep(432000)  # 5 days of sleeping, and we start from the beginning
    time.sleep(60)
