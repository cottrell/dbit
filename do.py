import datetime
import json
import time
import random
import gzip
import lib
import os
from lib import client
_mydir = os.path.dirname(os.path.realpath(__file__))
_data_dir = os.path.join(_mydir, 'data')

safe_commands = [
    # meta
    'getcurrencies',
    'getinstruments',
    # my state (private)
    'account',
    'orderhistory', # orderhistory(self, count=None)
    'positions',
    'tradehistory', # tradehistory(self, countNum=None, instrument="all", startTradeId=None)
    'getopenorders', # getopenorders(self, instrument=None, orderId=None)
    # market state (public)
    'getlasttrades', # getlasttrades(self, instrument, count=None, since=None)
    'getorderbook', # getorderbook(self, instrument)
    'getsummary', # getsummary(self, instrument)
    'index',
    'stats'
    ]

def snapshot():
    curr = lib.getcurrencies()
    instr = lib.getinstruments()
    instrumentNames = [x['instrumentName'] for x in instr]
    orderbook = dict()
    lasttrades = dict()
    summary = dict()
    for i, k in enumerate(instrumentNames):
        print('request {}: getting orderbook, summary and lasttrades for {}'.format(i, k))
        orderbook[k] = client.getorderbook(k)
        lasttrades[k] = client.getlasttrades(k)
        summary[k] = client.getsummary(k)
    return dict(curr=curr, instr=instr, orderbook=orderbook, lasttrades=lasttrades, summary=summary)

def run(stop_condition, mean_period=60):
    if not os.path.exists(_data_dir):
        os.makedirs(_data_dir)
    i = 0
    while not stop_condition(i):
        print('i={}'.format(i))
        i = i + 1
        d = snapshot()
        filename = os.path.join(_data_dir, 'snapshot_{}.json'.format(datetime.datetime.today().isoformat()))
        print('writing {}'.format(filename))
        json.dump(d, open(filename, 'w'))
        wait = random.random() * mean_period * 2
        print('sleeping {} seconds'.format(wait))
        time.sleep(wait)

run(lambda i: i > 1000)

# globals().update(snapshot())

# actions/danger
#  'buy',
#  'cancel',
#  'cancelall',
#  'edit',
#  'sell',
