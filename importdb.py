import os
import subprocess
import more_itertools as mit

files = os.listdir('/mongo-backup/cont')
coins = list(map(lambda y: y.split(".")[0], filter(lambda x: 'bson' in x, files)))

data_in = list(mit.chunked(coins, 4))

for chunk in data_in:
    for el in chunk:
        cmd = "mv  cont/{}.bson cont/{}.metadata.json klines".format(el, el)
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        process.wait()
        process = subprocess.Popen("mongorestore -d klines klines/", shell=True, stdout=subprocess.PIPE)
        process.wait()
        process = subprocess.Popen("rm klines/*", shell=True, stdout=subprocess.PIPE)
        process.wait()