from library import setup_logger, analyze_valuable_alts, MailContent, Markets, get_kucoin_interval_unit, send_mail, \
    process_valuable_alts, authorize

logger = setup_logger("find-valuable-alts")
logger.info("Starting Valuable Alts Finder...")

authorize()

binance_ticker = "1w"
kucoin_ticker = "1week"

binance_vol_filter = 50.0
kucoin_vol_filter = 20.0

mail_content = MailContent('')
markets_obj = Markets(binance_vol_filter, kucoin_vol_filter)

market_setups_binance_raw = analyze_valuable_alts("exclude-markets-binance", "binance", binance_ticker, "30 weeks ago", markets_obj)
market_setups_kucoin_raw = analyze_valuable_alts("exclude-markets-kucoin", "kucoin", kucoin_ticker,
                                                   get_kucoin_interval_unit(kucoin_ticker, 30), markets_obj)

market_setups_binance = sorted(market_setups_binance_raw, key=lambda x: x[1], reverse=True)
market_setups_kucoin = sorted(market_setups_kucoin_raw, key=lambda x: x[1], reverse=True)

process_valuable_alts(market_setups_binance, "binance", binance_ticker, mail_content)
process_valuable_alts(market_setups_kucoin, "kucoin", binance_ticker, mail_content)

send_mail("QQQ Valuable Alts Found QQQ", mail_content.content)



