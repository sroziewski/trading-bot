import time
import traceback

import requests

from library import setup_logger, analyze_golden_cross, authorize, send_mail, format_found_markets, \
    get_kucoin_interval_unit

logger = setup_logger("market-setup-finder")
logger.info("Starting Market-Setup-Finder...")

authorize()

while 1:
    try:
        golden_cross_markets = analyze_golden_cross("exclude-markets-binance", "1h", "1600 hours ago", "binance")
        if len(golden_cross_markets) > 0:
            send_mail("WWW Binance Setup Found WWW", ' '.join(format_found_markets(golden_cross_markets)))
        _kucoin_ticker = "1hour"
        golden_cross_markets = analyze_golden_cross("exclude-markets-kucoin", _kucoin_ticker, get_kucoin_interval_unit(_kucoin_ticker, 1600), "kucoin")
        if len(golden_cross_markets) > 0:
            send_mail("WWW Kucoin Setup Found WWW", ' '.join(format_found_markets(golden_cross_markets)))
        time.sleep(3500)
    except Exception as err:
        if isinstance(err, requests.exceptions.ConnectionError) or isinstance(err, requests.exceptions.ReadTimeout):
            logger.error("Connection problem...")
        else:
            traceback.print_tb(err.__traceback__)
            logger.exception(err.__traceback__)