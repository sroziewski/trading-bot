import shutil
import uuid

from library import get_files

path = "/var/www/html/pics/small"
destiny = "{}/store/".format(path)

for file in get_files(path):
    shutil.move(path+file, destiny+uuid.uuid4().__str__()+".png")
