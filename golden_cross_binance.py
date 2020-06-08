import time
import traceback

import requests

from library import setup_logger, analyze_golden_cross

logger = setup_logger("gc-binance")
logger.info("Starting Golden-Cross-Binance...")

while 1:
    try:
        analyze_golden_cross()
        time.sleep(3600)
    except Exception as err:
        if isinstance(err, requests.exceptions.ConnectionError) or isinstance(err, requests.exceptions.ReadTimeout):
            logger.error("Connection problem...")
        else:
            traceback.print_tb(err.__traceback__)
            logger.exception(err.__traceback__)