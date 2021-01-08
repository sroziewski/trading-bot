import sys
from time import sleep

import cv2
from pytesseract import pytesseract

from config import config
from library import setup_logger, send_mail, authorize

logger = setup_logger("BTC-Dominance-Watcher")

btcd_location = config.get_parameter('btcd_location')



arguments = len(sys.argv) - 1
if arguments != 2:
    print("You have to specify type the level of BTC.D and type-of-break you want to watch...)")
    exit(0)
logger.info("Starting global market data crawling...")


def get_current_btcd():
    image = cv2.imread(btcd_location)
    y = 451
    x = 188
    h = 32
    w = 97
    crop = image[y:y + h, x:x + w]
    pytesseract.tesseract_cmd = "tesseract"

    return float(pytesseract.image_to_string(crop))


def notify_when_break_up(_level):
    _btcd = get_current_btcd()
    if _btcd > _level:
        send_mail(f"ZZZ BTC.D level {_level} BREAK UP ZZZ", f"Current BTC.D : {_btcd} > observed : {_level}")


def notify_when_break_down(_level):
    _btcd = get_current_btcd()
    if _btcd < _level:
        send_mail(f"ZZZ BTC.D level {_level} BREAK DOWN ZZZ", f"Current BTC.D : {_btcd} < observed : {_level}")


def manage_notification(_level, _type, _lines=False):
    if _type == "up":
        notify_when_break_up(_level)
    elif _type == "down":
        notify_when_break_down(_level)

    # for _line in _lines:

            
def validate_args(_args):
    _level = float(sys.argv[1])
    _type = sys.argv[2]
    assert 10 < _level < 90
    assert _type == "up" or _type == "down"
    logger.info(f"All validations done : btcd : {btcd_level} type : {_type}")


def get_line(_btcd_open1, _btcd_open2, _dt):
    _b = _btcd_open1
    _a = (_btcd_open2 - _btcd_open1)/_dt
    return _a, _b


def break_line(_line):
    _a, _b = get_line(_line.p1, _line.p2, _line.dt)
    _btcd = get_current_btcd()
    _res = False
    if _line.type == "down":
        _res = True if 0 < _a * (_line.dt + 1) + _b - _btcd else False
    else:
        _res = True if 0 > _a * (_line.dt + 1) + _b - _btcd else False

    return _res


class Line(object):
    def __init__(self, _p1, _p2, _dt, _type) -> None:
        self.p1 = _p1
        self.p2 = _p2
        self.dt = _dt
        self.type = _type


authorize()

btcd_level = float(sys.argv[1])
breakout_type = sys.argv[2]

line_breaking_up = Line(64.99, 65.11, 25, "up")

# break_line(url, 65.26, 65.41, 3, "down")


validate_args(sys.argv)

logger.info(f"BTC.D level to watch : {btcd_level}")

lines = [line_breaking_up]
while 1:
    try:
        manage_notification(btcd_level, breakout_type, lines)
    except Exception as e:
        logger.error(e)
        sleep(5)
        manage_notification(btcd_level, breakout_type, lines)
    sleep(1800)



