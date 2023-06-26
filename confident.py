# https://www.tradeconfident.io/content/images/size/w1600/2023/02/Screen-Shot-2023-02-01-at-7.32.43-AM.png
# https://www.tradeconfident.io/content/images/2023/03/Screen-Shot-2023-03-01-at-7.36.52-AM.png
import copy
import datetime
import subprocess
import threading
from datetime import date
from time import sleep
from urllib import request
from urllib.error import HTTPError

import schedule
from PIL import Image

from config import config
from library import setup_logger

# prefix_url = "https://www.tradeconfident.io/content/images/size/w1600/"
prefix_url = "https://www.tradeconfident.io/content/images/"
path = "/var/www/html/pics/"
# path = "E:/dev_null/trade/files/"
map_path = path + "map/"
small_path = path + "small/"
counter = 0

logger = setup_logger("Trade-Confident")

coin_map_0 = {
    "atom": [],
    "dot": [],
    "avax": [],
    "eos": [],
    "doge": [],
    "link": [],
    "ltc": [],
    "mana": [],
    "matic": [],
    "sand": [],
    "shiba": [],
    "sol": [],
    "uni": [],
    "btc": [],
    "vet": [],
    "xrp": [],
    "eth": [],
    "ada": [],
    "algo": [],
    "ape": [],
    "others": []
}


def read_text(_file_fullname, _filename):
    global locked
    im = Image.open(r""+_file_fullname)
    left = 5
    top = 5
    right = 514
    bottom = 48
    img_crop = im.crop((left, top, right, bottom))
    _tmp_file = path + "tmp/" + _filename
    img_crop.save(_tmp_file)
    tesseract_cmd = "tesseract {} stdout --oem 1 -l eng".format(_tmp_file)
    while locked:
        sleep(1)
    locked = True
    p = subprocess.Popen(tesseract_cmd, stdout=subprocess.PIPE, shell=True)
    _r = p.communicate()
    locked = False
    return _r


class Argument(object):
    def __init__(self, _range, _map, _when="today"):
        self.range = _range
        self.when = _when
        self.coin_map = _map


locked = False


def save_pic(_arg: Argument):
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
            try:
                _filename_suffix = "{}-{}-{}.png".format(_arg.when, max(_arg.range), _counter)
                _img_filename = path + _filename_suffix
                _img_filename_small = small_path + "{}-{}-{}.png".format(_arg.when, max(_arg.range), _counter)
                request.urlretrieve(_url_tmp, _img_filename)
                resize_pic(_img_filename, _img_filename_small)
                _txt = read_text(_img_filename, _filename_suffix)
                _coin = extract_coin(_txt[0])
                if _coin == "others":
                    logger.info("file: {} -- others: {}".format(_filename_suffix, _txt[0]))
                _arg.coin_map[_coin].append(_filename_suffix)
                _counter += 1
            except HTTPError:
                pass
    return 0


def extract_coin(_txt: str):
    _txt = _txt.lower().decode('utf-8').split("/")[0]

    _atom = ['cosmos', 'cocsmo', 'coemos', 'cocmoese']
    _dot = ['polkadot', 'idot']
    _avax = ['avalanche', 'aveleme', 'avelemae', 'avellemee']
    _eos = ['eos', 'eqs', 'fos', 'fqs']
    _doge = ['doge', 'doaecoin', 'doagecoin']
    _link = ['link']
    _ltc = ['jtecoin', 'litecoin', 'itecoin']
    _mana = ['decentraland', 'pecentraland', 'dBreeertelead']
    _matic = ['poalvaon', 'polygon', 'polvaon', 'palvaon']
    _sand = ['sand']
    _shiba = ['hiba', 'shib']
    _sol = ['solana', 'gplana', 'cnolana', 'colana', 'qnalana']
    _uni = ['uni', 'ynjswap', 'jniewan', 'iniewan', 'jniswan', 'inicwan', '1nicwan']
    _btc = ['bitcoin', 'ritcoin']
    _vet = ['echain', '\\/echain', 'ochain']
    _xrp = ['xrp', 'wyrp', 'xyrp', 'rp']
    _eth = ['eth', 'fthe', 'ciheretim', 'eiheretim']
    _ada = ['cardano']
    _algo = ['algo', 'alaorand']
    _ape = ['ape', 'apne', 'anecoin', 'avecoin']

    if any(item in _txt for item in _atom):
        return "atom"
    if any(item in _txt for item in _dot):
        return "dot"
    if any(item in _txt for item in _avax):
        return "avax"
    if any(item in _txt for item in _eos):
        return "eos"
    if any(item in _txt for item in _doge):
        return "doge"
    if any(item in _txt for item in _link):
        return "link"
    if any(item in _txt for item in _ltc):
        return "ltc"
    if any(item in _txt for item in _mana):
        return "mana"
    if any(item in _txt for item in _matic):
        return "matic"
    if any(item in _txt for item in _sand):
        return "sand"
    if any(item in _txt for item in _shiba):
        return "shiba"
    if any(item in _txt for item in _sol):
        return "sol"
    if any(item in _txt for item in _uni):
        return "uni"
    if any(item in _txt for item in _btc):
        return "btc"
    if any(item in _txt for item in _vet):
        return "vet"
    if any(item in _txt for item in _xrp):
        return "xrp"
    if any(item in _txt for item in _eth):
        return "eth"
    if any(item in _txt for item in _ada):
        return "ada"
    if any(item in _txt for item in _algo):
        return "algo"
    if any(item in _txt for item in _ape):
        return "ape"
    return "others"


def write_map(_coin_map):
    for _key, _list in _coin_map.items():
        with open(map_path + "{}.txt".format(_key), "w") as _f:
            for _row in _list:
                _f.write(_row +"\n")


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
    return _crawler_s


def manager():
    logger.info("Starting manager...")

    coin_map = copy.deepcopy(coin_map_0)

    scanners = []

    scanners.append(scanner(Argument(range(0, 20), coin_map)))
    scanners.append(scanner(Argument(range(20, 40), coin_map)))
    scanners.append(scanner(Argument(range(40, 60), coin_map)))

    scanners.append(scanner(Argument(range(0, 20), coin_map, "yesterday")))
    scanners.append(scanner(Argument(range(20, 40), coin_map, "yesterday")))
    scanners.append(scanner(Argument(range(40, 60), coin_map, "yesterday")))

    [x.join() for x in scanners]

    logger.info("Writing map...")
    write_map(coin_map)

    with open(path + "date.txt", "w") as _f:
        # Writing data to a file
        _f.write(str(datetime.datetime.now()))


def clear_chart_dir():
    working_dir = config.get_parameter('working_dir')
    _p = subprocess.Popen("python {}/rename.py && cd {} && rm -f *.png && rm -f small/*.png && rm -f tmp/*.png && rm -f map/*.txt".format(working_dir, path), stdout=subprocess.PIPE, shell=True)
    _p.communicate()


schedule.every().day.at("07:58").do(clear_chart_dir)
schedule.every().day.at("08:00").do(manager)
schedule.every().day.at("20:30").do(manager)
#
while True:
    # Checks whether a scheduled task
    # is pending to run or not
    schedule.run_pending()
    sleep(10)
