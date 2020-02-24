import re
import time
import urllib.request
from library import authorize, send_mail, setup_logger

logger = setup_logger("binance-twitter-observer")
authorize()

url = 'https://twitter.com/binance'
req = urllib.request.Request(url)

pattern = re.compile('Community\\s+Coin\\s+Vote\\s+Round', re.IGNORECASE)

while 1:
    with urllib.request.urlopen(req) as response:
        _page_content = response.read()
        if pattern.search(str(_page_content)):
            send_mail("QQQ Community Coin Vote Round FOUND!!!", url)
            logger.info("Community Coin Vote Round -- found")
            time.sleep(600)
        time.sleep(60)

