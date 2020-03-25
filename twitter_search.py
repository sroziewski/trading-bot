import re
import time
import urllib.request
from library import authorize, send_mail, setup_logger

logger = setup_logger("binance-twitter-observer")
authorize()

url = 'https://twitter.com/binance'
req = urllib.request.Request(url)

pattern_s = re.compile('\\s+')
cnt = 0
t1 = 600
cycle = 0

while 1:
    with urllib.request.urlopen(req) as response:
        _page_content = response.read()
        _content = pattern_s.sub(" ", str(_page_content).lower())
        if 'community coin vote round' in _content:
            cnt += 1
            send_mail("QQQ Community Coin Vote Round FOUND!!!", url)
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
                time.sleep(432000) # 5 days of sleeping, and we start from the beginning
        time.sleep(60)
