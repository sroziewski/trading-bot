import time
import traceback

import requests

from library import ObserveAsset, sat, check_price_slope, check_horizontal_price_level, setup_logger, Line, send_mail, \
    authorize, get_last_closing_price, price_to_string

logger = setup_logger("binance_pattern_watcher")
logger.info("Starting binance pattern watcher" )

authorize()

assets = [
            # ObserveAsset("binance", "CRV", 3050 * sat, line=Line(4390*sat, 3200*sat, 55, "up")),
            ObserveAsset("binance", "DIA", 7900 * sat, line=Line(9390*sat, 7850*sat, 26, "up"), horizon="up"),
            ObserveAsset("binance", "TRB", 128400 * sat, line=Line(184000*sat, 133100*sat, 103, "up"), horizon="up"),
            ObserveAsset("binance", "BZRX", 0 * sat, line=Line(1869*sat, 1550*sat, 60, "up")),
            ObserveAsset("binance", "PERL", 0 * sat, line=Line(142*sat, 125*sat, 90, "up")),
            ObserveAsset("binance", "DOCK", 0 * sat, line=Line(90*sat, 76*sat, 155, "up")),
            ObserveAsset("binance", "IRIS", 309 * sat, line=Line(369*sat, 306*sat, 88, "up"), horizon="up"),
            ObserveAsset("binance", "COTI", 289 * sat, line=Line(369*sat, 306*sat, 88, "up"), horizon="up"),
            ObserveAsset("binance", "RSR", 114 * sat, line=Line(132*sat, 114*sat, 92, "up"), horizon="up"),
            ObserveAsset("binance", "RUNE", 5126 * sat, line=Line(9475*sat, 5852*sat, 557, "up"), horizon="up"),
            ObserveAsset("binance", "OXT", 1485 * sat, line=Line(1935*sat, 1686*sat, 87, "up"), horizon="up"),
            ObserveAsset("binance", "ORN", 0* sat, line=Line(21990*sat, 18440*sat, 29, "up"), horizon="up"),
            ObserveAsset("binance", "HBAR", 198* sat, line=Line(227*sat, 211*sat, 157, "up"), horizon="up"),
            ObserveAsset("binance", "ROSE", 0* sat, line=Line(313*sat, 286*sat, 16, "up"), horizon="up"),
            ObserveAsset("binance", "CTK", 0* sat, line=Line(6588*sat, 6389*sat, 9, "up")),
            ObserveAsset("binance", "NEAR", 0* sat, line=Line(6290*sat, 5630*sat, 35, "up")),
            ObserveAsset("binance", "NEAR", 0* sat, line=Line(6290*sat, 5630*sat, 35, "up")),
            ObserveAsset("binance", "XLM", 0* sat, line=Line(1239*sat, 963*sat, 71, "up")),
            ObserveAsset("binance", "BAL", 0* sat, line=Line(94220*sat, 73220*sat, 84, "up")),
            ObserveAsset("binance", "LTC", 0* sat, line=Line(473100*sat, 441400*sat, 38, "up")),
            ObserveAsset("binance", "ANKR", 0* sat, line=Line(58*sat, 48*sat, 99, "up")),
            ObserveAsset("binance", "AKRO", 0* sat, line=Line(77*sat, 67*sat, 30, "up")),
            ObserveAsset("binance", "AUDIO", 0* sat, line=Line(1159*sat, 1012*sat, 20, "up")),
            ObserveAsset("binance", "AVA", 0* sat, line=Line(6430*sat, 5170*sat, 149, "up")),
            ObserveAsset("binance", "AXS", 0* sat, line=Line(3635*sat, 3342*sat, 17, "up")),
            ObserveAsset("binance", "BAND", 0* sat, line=Line(39832*sat, 38028*sat, 11, "up")),
            ObserveAsset("binance", "COMP", 0* sat, line=Line(928000*sat, 907600*sat, 6, "up")),
            ObserveAsset("binance", "DGB", 0* sat, line=Line(133*sat, 116*sat, 68, "up")),
            ObserveAsset("binance", "KAVA", 0* sat, line=Line(11132*sat, 8555*sat, 107, "up")),
            ObserveAsset("binance", "LRC", 0* sat, line=Line(1239*sat, 1079*sat, 86, "up")),
            ObserveAsset("binance", "LUNA", 0* sat, line=Line(1716*sat, 2096*sat, 87, "down")),
            ObserveAsset("binance", "SRM", 0* sat, line=Line(7829*sat, 6454*sat, 126, "up")),
            ObserveAsset("binance", "UMA", 0* sat, line=Line(56800*sat, 41200*sat, 185, "up")),
            ObserveAsset("binance", "XVS", 0* sat, line=Line(302400*sat, 234500*sat, 128, "up")),
            ObserveAsset("binance", "SAND", 276* sat, line=Line(301*sat, 274*sat, 16, "up")),
            ObserveAsset("binance", "WAVES", 4910* sat, line=None, horizon="up"),
            ObserveAsset("binance", "UNFI", 80662* sat, line=None, horizon="up"),
            ObserveAsset("binance", "INJ", 13121* sat, line=None, horizon="up"),
            ObserveAsset("binance", "SXP", 4951* sat, line=None, horizon="up"),
            ObserveAsset("binance", "OMG", 17950* sat, line=None, horizon="up"),
            ObserveAsset("binance", "HARD", 5891* sat, line=None, horizon="up"),
            ObserveAsset("binance", "MDT", 113* sat, line=None, horizon="up"),
            ObserveAsset("binance", "CTSI", 283* sat, line=None, horizon="up"),
            ObserveAsset("binance", "FET", 361* sat, line=None, horizon="up"),
            ObserveAsset("binance", "FLM", 1312* sat, line=None, horizon="up"),
            ObserveAsset("binance", "UTK", 617 * sat, line=None, horizon="down"),
          ]

while 1:
    mail_content = None
    found_assets = []
    for asset in assets:
        try:
            horizon = check_horizontal_price_level(asset, asset.horizon)
            slope = check_price_slope(asset)
            if horizon or slope:
                closing_price = get_last_closing_price(asset)
                found_assets.append(f"{asset.name} : {price_to_string(closing_price)} BTC")
        except Exception as err:
            if isinstance(err, requests.exceptions.ConnectionError) or isinstance(err, requests.exceptions.ReadTimeout):
                logger.error("Connection problem...")
            else:
                traceback.print_tb(err.__traceback__)
                logger.exception(err.__traceback__)
    if len(found_assets):
        mail_content = '<BR/>'.join(found_assets)
        send_mail(f"QQQ Binance Pattern Found QQQ", mail_content)
    time.sleep(3600*4-60*5)