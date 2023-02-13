#https://www.tradeconfident.io/content/images/size/w1600/2023/02/Screen-Shot-2023-02-01-at-7.32.43-AM.png
import datetime
import threading
import urllib
from datetime import date
from time import sleep
from urllib import request

import schedule
from PIL import Image

from library import get_time

prefix_url = "https://www.tradeconfident.io/content/images/size/w1600/"
path = "/var/www/html/pics/"
# path = "D:/dev_null/trade/files/"
counter = 0


class Argument(object):
    def __init__(self, _range, _when="today"):
        self.range = _range
        self.when = _when


def save_pic(_arg : Argument):
    _date = date.today()
    if _arg.when == "yesterday":
        _date -= datetime.timedelta(1)

    _year = add_zero(_date.year)
    _month = add_zero(_date.month)
    _day = add_zero(_date.day)
    _counter = 0
    for _min in _arg.range:
        for _sec in range(60):
            _url_tmp = prefix_url + "{}/{}/Screen-Shot-{}-{}-{}-at-7.{}.{}-AM.png"\
                .format(_year, _month, _year, _month, _day, add_zero(_min), add_zero(_sec))
            try:
                _img_filename = path + "{}-{}.png".format(_arg.when, _counter)
                _img_filename_small = path + "small/{}-{}.png".format(_arg.when, _counter)
                request.urlretrieve(_url_tmp, _img_filename)
                resize_pic(_img_filename, _img_filename_small)
                _counter += 1
            except Exception as e:
                pass
    return 0


def resize_pic(_img_filename, _img_filename_small):
    image = Image.open(_img_filename)
    _width = 600
    _height = int(_width * image.height / image.width)
    new_image = image.resize((_width, _height))
    new_image.save(_img_filename_small)


def add_zero(_int):
    if _int < 10:
        return "0{}".format(_int)
    return str(_int)


def scanner(_arg):
    global counter
    _crawler_s = threading.Thread(target=save_pic, args=(_arg,),
                                  name='save_pic : {}'.format(counter))
    _crawler_s.start()
    counter += 1


def manager():
    scanner(Argument(range(0, 20)))
    scanner(Argument(range(20, 40)))
    scanner(Argument(range(40, 60)))

    scanner(Argument(range(0, 20), "yesterday"))
    scanner(Argument(range(20, 40), "yesterday"))
    scanner(Argument(range(40, 60), "yesterday"))

    with open(path + "date.txt", "w") as _f:
        # Writing data to a file
        _f.write(str(datetime.datetime.now()))



schedule.every().day.at("08:00").do(manager)

while True:
    # Checks whether a scheduled task
    # is pending to run or not
    schedule.run_pending()
    sleep(10)


