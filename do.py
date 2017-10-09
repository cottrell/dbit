import datetime
import numpy as np
import pandas as pd
import glob
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

def take_snapshot_write_file():
    d = snapshot()
    filename = os.path.join(_data_dir, 'snapshot_{}.json'.format(datetime.datetime.today().isoformat()))
    print('writing {}'.format(filename))
    json.dump(d, open(filename, 'w'))
    cmd = 'gzip {}'.format(filename)
    print(cmd)
    os.system(cmd)

def run(stop_condition, mean_period=60):
    if not os.path.exists(_data_dir):
        os.makedirs(_data_dir)
    i = 0
    while not stop_condition(i):
        print('i={}'.format(i))
        i = i + 1
        take_snapshot_write_file()
        wait = random.random() * mean_period * 2
        print('sleeping {} seconds'.format(wait))
        time.sleep(wait)

def _to_date(x):
    return datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S GMT')

# do something simple for now
def transform_summary(d):
    imap = pd.DataFrame(d['instr']).set_index('instrumentName')
    imap = imap.drop('created', axis=1)
    summary = pd.DataFrame(d['summary']).T
    # summary.index.names = ['instrumentName'] # do not need, is in data already
    df = summary.join(imap)
    df['maturity_days'] = df.expiration.map(_to_date)
    # - datetime.datetime.today()
    df['created'] = df.created.map(_to_date)
    for k in f_cols:
        df[k] = df[k].replace('', np.nan).astype(float)
    df['strike'] = df.strike.astype(float)
    return df

f_cols = [
 'askPrice',
 'bidPrice',
 'estDelPrice',
 'high',
 'last',
 'low',
 'markPrice',
 'midPrice',
 'openInterest',
 'volume',
 'volumeBtc', # redundant with volume?
 # 'created', # ignore diff in created for now
 # 'maturity_days']
 ]

c_cols = [
 # 'instrumentName',
 'kind',
 'optionType',
 'strike'
 'settlement',
 ]

class Data():
    """
    Processing the json snapshots. There are the following records in snapshot: ['curr', 'instr', 'orderbook', 'lasttrades', 'summary']

        Each of these are per-instrument records.
        summary: can be flattened, only need to lookup strike and maturity from instr. This should be starting point.
        orderbook:
        lasttrades:
    """
    def __init__(self):
        filename = os.path.join(_data_dir, 'snapshot_*.json')
        self.filenames = glob.glob(filename) + glob.glob(filename + '.gz')
        self.data = None
    def load(self, n_files=None):
        d = list()
        filenames = self.filenames
        if n_files is not None:
            filenames = filenames[-n_files:]
        for f in filenames:
            print('reading {}'.format(f))
            opener = open
            if f.endswith('.gz'):
                opener = gzip.open
            d.append(json.load(opener(f)))
        self.data = d
    def __repr__(self):
        out = ['{} entries:'.format(len(self.filenames))]
        if self.data is not None:
            for k in self.data[0]:
                out += ['{} {} entries '.format(k, len(self.data[0][k]))]
        return '\n'.join(out)


data = Data()

# run(lambda i: i > 1000)

# globals().update(snapshot())

# actions/danger
#  'buy',
#  'cancel',
#  'cancelall',
#  'edit',
#  'sell',
