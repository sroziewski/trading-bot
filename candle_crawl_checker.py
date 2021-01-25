import datetime
from time import sleep

from bson.codec_options import TypeRegistry, CodecOptions

from library import setup_logger, authorize, read_collections_file, flatten, \
    get_last_db_record, DecimalCodec, send_mail, extract_collection_name
from mongodb import mongo_client

logger = setup_logger("candle_crawl_checker")
logger.info("Starting candle crawl checker" )

authorize()

db = mongo_client.klines
decimal_codec = DecimalCodec()
type_registry = TypeRegistry([decimal_codec])
codec_options = CodecOptions(type_registry=type_registry)

tickers = ["15m", "30m", "1h", "4h", "8h", "12h", "1d"]

while 1:
    outdated = []

    for raw_name in flatten(read_collections_file()):
        collection_root_name, exchange = extract_collection_name(raw_name)
        for ticker in tickers:
            collection_name = f"{collection_root_name}{ticker}"
            collection = db.get_collection(collection_name, codec_options=codec_options)
            record = get_last_db_record(collection)
            _current_time = datetime.datetime.now().timestamp() * 1000
            _12h = 12 * 60 * 60 * 1000
            if ticker == "1d":
                _12h = 3 * 12 * 60 * 60 * 1000

            if exchange == "binance":
                if _current_time - record['timestamp'] > _12h:
                    outdated.append(collection_name)
            elif exchange == "kucoin":
                _12h /= 1000
                _current_time /= 1000
                if _current_time - int(record['timestamp']) > _12h:
                    outdated.append(collection_name)

    if len(outdated)>0:
        mail_content = '<BR/>'.join(outdated)
        send_mail(f"OOO CANDLE CRAWL FAILS OOO", mail_content)

    sleep(_12h/1000)




