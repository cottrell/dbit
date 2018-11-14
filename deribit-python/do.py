#!/usr/bin/env python
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
if not os.path.exists(_data_dir):
    os.makedirs(_data_dir)

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
        orderbook[k] = client.getorderbook(k) # depth optional
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
def transform_summary(snapshot):
    d = snapshot
    imap = pd.DataFrame(d['instr']).set_index('instrumentName')
    imap = imap.drop('created', axis=1)
    summary = pd.DataFrame(d['summary']).T
    # summary.index.names = ['instrumentName'] # do not need, is in data already
    df = summary.join(imap)
    df['expiration'] = df.expiration.map(_to_date)
    df['maturity_days'] = df.expiration.dt.day
    # - datetime.datetime.today()
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
     # 'volumeBtc', # redundant with volume?
     'strike',
     # 'created', # ignore diff in created for now
     'maturity_days'
     ]
    c_cols = [
     # 'instrumentName',
     'kind',
     'optionType',
     'settlement',
     ]
    df['created'] = df.created.map(_to_date)
    for k in f_cols:
        df[k] = df[k].replace('', np.nan).astype(float)
    df['strike'] = df.strike.astype(float)
    return df

def transform_orderbook(snapshot, method='simple', gridsize=100):
    d = snapshot
    # only deal with bid and asks
    orderbook = d['orderbook']
    if method == 'simple':
        out = list()
        for k, v in orderbook.items():
            tstamp = v['tstamp']
            for x in v['asks'][::-1]:
                out.append([k, tstamp, False, x['cm'], x['price']])
            for x in v['bids']:
                out.append([k, tstamp, True, x['cm'], x['price']])
        out = pd.DataFrame(out, columns=['instrumentName', 'tstamp', 'bid', 'cm', 'price']).set_index('instrumentName')
        return out
    elif method == 'piecewise_mean':
        imap = {x['instrumentName']: x for x in d['instr']}
        max_qty = dict(option=0, future=0)
        for k, v in orderbook.items():
            orderbook[k]['f_ask'] = None
            kind = imap[k]['kind']
            if v['asks']:
                orderbook[k]['f_ask'] = _get_piecewise_mean_price_vs_size_from_orderbook_entry(v['asks'])
                max_qty[kind] = max(max_qty[kind], max([x['quantity'] for x in v['asks']]))
            orderbook[k]['f_bid'] = None
            if v['bids']:
                orderbook[k]['f_bid'] = _get_piecewise_mean_price_vs_size_from_orderbook_entry(v['bids']) if v['bids'] else None
                max_qty[kind] = max(max_qty[kind], max([x['quantity'] for x in v['bids']]))
            orderbook[k].update(imap[k]) # jam everything in
        return orderbook, max_qty
    else:
        raise Exception('not implemented ... ')
        imap = pd.DataFrame(d['instr']).set_index('instrumentName')
        pricePrecision = imap['pricePrecision']
        # typically precisions is 2 for futures and 4 for options. price range for asks or bids is < 30 for futures and < 1 for options.
        # do mean price vs qty

def plot_snapshot_orders(snapshot):
    d, max_qty = transform_orderbook(snapshot, method='piecewise_mean')
    from matplotlib.pyplot import plot, show, clf, figure, ion
    ion()
    fa = figure(1)
    fa.clf()
    ga = fa.gca()
    fb = figure(2)
    gb = fb.gca()
    from pylab import linspace
    for k, v in d.items():
        if v['kind'] == 'option':
            g = ga
            # TODO calc max ranges for option and futures dynamically in transform orderbook
            x = linspace(0, max_qty['option'])
        else:
            g = gb
            x = linspace(0, max_qty['future'])
        f = v['f_ask']
        if f is not None:
            g.plot(x, f(x), 'r-', alpha=0.5)
        f = v['f_bid']
        if f is not None:
            g.plot(-x, f(x), 'b-', alpha=0.5)


from scipy.interpolate import PPoly
def _get_piecewise_mean_price_vs_size_from_orderbook_entry(orders):
    """ orders is just asks or just orders """
    cm = [0] + [x['cm'] for x in orders]
    # integral (price times qty) d_qty / qty
    # represent this as integral of piecewise polynomial with coeff [0, price]
    price = np.zeros((2, len(cm)-1))
    price[1,:] = [x['price'] for x in orders]
    f = PPoly(price, cm, extrapolate=False)
    F = f.antiderivative()
    return lambda x: F(x) / x

def _check_ranges(df):
    g = df[df.cm > 0].groupby(level='instrumentName').price
    a = g.max() - g.min()
    a = a.sort_values().tail()
    g = df[df.cm < 0].groupby(level='instrumentName').price
    b = g.max() - g.min()
    b = b.sort_values().tail()
    return {'bids': a, 'asks': b}

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

if __name__ == '__main__':
    take_snapshot_write_file()
