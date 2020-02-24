import time
import urllib.request
from library import authorize, send_mail, setup_logger

logger = setup_logger("binance-twitter-observer")
authorize()

url = 'https://twitter.com/binance'

req = urllib.request.Request(url)
while 1:
    with urllib.request.urlopen(req) as response:
        the_page = response.read()
        if 'Community Coin Vote Round'.lower() in str(the_page).lower():
            send_mail("QQQ Community Coin Vote FOUND!!!", url)
            logger.info("Community Coin Vote Round -- found")
            time.sleep(600)
        time.sleep(60)

