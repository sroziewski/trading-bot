import time
import traceback

import requests

from library import check_price_slope, check_horizontal_price_level, setup_logger, send_mail, \
    authorize, get_last_closing_price, price_to_string, get_klines_asset, check_mas, \
    create_observe_assets, log_assets

logger = setup_logger("binance_pattern_watcher")
logger.info("Starting binance pattern watcher" )

authorize()

while 1:
    assets = create_observe_assets()
    log_assets(assets)
    mail_content = None
    found_assets = []
    for asset in assets:
        try:
            klines = get_klines_asset(asset)
            horizon = check_horizontal_price_level(asset, asset.horizon, klines)
            slope = check_price_slope(asset, klines)
            mas = check_mas(asset, klines)
            if mas:
                i = 1
            breakout_type = filter(lambda x: x, [("horizon", price_to_string(asset.buy_price), asset.horizon) if horizon else False,
                                                 ("slope", asset.line.market_type) if slope else False])
            if horizon or slope or mas:
                closing_price = get_last_closing_price(asset)
                types = ' '.join([i for sub in breakout_type for i in sub])
                if mas:
                    mas_type = ' '.join([str(x) for x in mas])
                    types = f"{types} {mas_type}"
                found_assets.append(f"{asset.name} : {price_to_string(closing_price)} BTC --- ticker : {asset.ticker} --- type : {types}")
        except Exception as err:
            if isinstance(err, requests.exceptions.ConnectionError) or isinstance(err, requests.exceptions.ReadTimeout):
                logger.error("Connection problem...")
            else:
                traceback.print_tb(err.__traceback__)
                logger.exception(err.__traceback__)
    if len(found_assets):
        mail_content = '<BR/>'.join(found_assets)
        send_mail(f"QQQ Binance Pattern Found QQQ", mail_content)
        logger.info('\n'.join(found_assets))
    time.sleep(3600*4-60*5)