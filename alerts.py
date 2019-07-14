import time
import traceback
import requests

from library import setup_logger, start_alerts, AlertAsset, authorize

trade_assets = [
    AlertAsset('CELR'),
    AlertAsset('FTM'),
    AlertAsset('ONE'),
    AlertAsset('MATIC'),
    AlertAsset('ALGO')
]

logger = setup_logger("alerts")

authorize()

while 1:
    try:
        for trade_asset in trade_assets:  # remove asset from here
            start_alerts(trade_asset)

        time.sleep(40)
    except Exception as err:
        if isinstance(err, requests.exceptions.ConnectionError):
            logger.error("Connection problem...")
        else:
            traceback.print_tb(err.__traceback__)
            logger.exception(err.__traceback__)
