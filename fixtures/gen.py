
from random import randint, choice
import csv

txs = {}

def get_random_tx_type():
    types = ["deposit", "withdrawal", "resolve", "chargeback", "dispute"]
    return choice(types)

def gen_client():
    return randint(0,65535)

def gen_uniq_txid():
    global txs
    while True:
        id = randint(0, 4294967295)
        if id not in txs:
            txs[id] = 1
            return id
def gen_existent_txid():
    global txs
    if len(txs.keys()) > 0:
        return choice(list(txs.keys()))
    else:
        return gen_uniq_txid()
        

    #
records = [["type", "client", "tx "   ," amount"]]
for i in range(0,5_000_000):

    cid = gen_client()
    tx_type = get_random_tx_type()
    if randint(0,10) < 8 and tx_type not in ("deposit", "withdrawal"):
        txid = gen_existent_txid()
    else:
        txid = gen_uniq_txid()

    amount = randint(0, 100)

    records.append([tx_type, cid, txid, amount])

with open('myfile.csv', 'w', newline='') as file:
    mywriter = csv.writer(file, delimiter=',')
    mywriter.writerows(records)


