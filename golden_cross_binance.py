import time
import traceback

import requests

from library import setup_logger, analyze_golden_cross, authorize, send_mail, format_found_markets

logger = setup_logger("gc-binance")
logger.info("Starting Golden-Cross-Binance...")

authorize()

while 1:
    try:
        golden_cross_markets = analyze_golden_cross()
        if len(golden_cross_markets) > 0:
            send_mail("WWW Second Golden Cross Found WWW", ' '.join(format_found_markets(golden_cross_markets)))
        time.sleep(3500)
    except Exception as err:
        if isinstance(err, requests.exceptions.ConnectionError) or isinstance(err, requests.exceptions.ReadTimeout):
            logger.error("Connection problem...")
        else:
            traceback.print_tb(err.__traceback__)
            logger.exception(err.__traceback__)