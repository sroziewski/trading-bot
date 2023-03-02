# https://www.tradeconfident.io/content/images/size/w1600/2023/02/Screen-Shot-2023-02-01-at-7.32.43-AM.png
# https://www.tradeconfident.io/content/images/2023/03/Screen-Shot-2023-03-01-at-7.36.52-AM.png
import datetime
import subprocess
import threading
from datetime import date
from time import sleep
from urllib import request
from urllib.error import HTTPError

import schedule
from PIL import Image

# prefix_url = "https://www.tradeconfident.io/content/images/size/w1600/"
prefix_url = "https://www.tradeconfident.io/content/images/"
path = "/var/www/html/pics/"
# path = "E:/dev_null/trade/files/"
counter = 0


def read_text(_file_fullname, _filename):
    im = Image.open(r""+_file_fullname)
    left = 5
    top = 5
    right = 514
    bottom = 40
    img_crop = im.crop((left, top, right, bottom))
    _tmp_file = path + "tmp/" + _filename
    img_crop.save(_tmp_file)
    tesseract_cmd = "docker run --rm -it --name myapp -v \"/var/www/html/pics/tmp\":/app  -w /app \"tesseract-ocr\" tesseract {} stdout --oem 1".format(_filename)
    p = subprocess.Popen(tesseract_cmd, stdout=subprocess.PIPE, shell=True)
    return p.communicate()


class Argument(object):
    def __init__(self, _range, _when="today"):
        self.range = _range
        self.when = _when


_list = []

def save_pic(_arg: Argument):
    global _list
    _date = date.today()
    if _arg.when == "yesterday":
        _date -= datetime.timedelta(1)

    _year = add_zero(_date.year)
    _month = add_zero(_date.month)
    _day = add_zero(_date.day)
    _counter = 0
    for _min in _arg.range:
        for _sec in range(60):
            _url_tmp = prefix_url + "{}/{}/Screen-Shot-{}-{}-{}-at-7.{}.{}-AM.png" \
                .format(_year, _month, _year, _month, _day, add_zero(_min), add_zero(_sec))
            _list.append(_url_tmp)
            try:
                _filename_suffix = "{}-{}.png".format(_arg.when, _counter)
                _img_filename = path + _filename_suffix
                _img_filename_small = path + "small/{}-{}.png".format(_arg.when, _counter)
                request.urlretrieve(_url_tmp, _img_filename)
                resize_pic(_img_filename, _img_filename_small)

                _txt = read_text(_img_filename, _filename_suffix)
                print(_txt)
                _counter += 1
            except HTTPError as e:
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


schedule.every().day.at("16:49").do(manager)
schedule.every().day.at("20:30").do(manager)
#
while True:
    # Checks whether a scheduled task
    # is pending to run or not
    schedule.run_pending()
    sleep(10)
