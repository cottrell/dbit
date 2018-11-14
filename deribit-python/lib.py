"""
TODO kafka commit
0. start kafka
1. websocket_to_kafka
2. kafka_consume(1) # to see a bit
"""
import numpy as np
from functools import lru_cache
from collections import defaultdict
import time
import pandas as pd
import datetime
import json as _json
import rapidjson as json
import os
import websocket # pip install websocket-client not websocket
import deribit_api as da
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from scipy.interpolate import PPoly

cred = _json.load(open(os.path.expanduser('~/.cred/deribit/deribit.json')))
test_url = 'https://test.deribit.com'
url = 'https://www.deribit.com'
client = da.RestClient(cred['access_key'], cred['access_secret'], url=url)

# almost don't need these for the subscribe
getinstruments = lru_cache()(client.getinstruments)
getcurrencies = lru_cache()(client.getcurrencies)

def get_websocket_producer():
    producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    def on_message(ws, message):
        print('sending {}'.format(len(message)))
        producer.send(topic, message)
    def on_error(ws, error):
        print(error)
    def on_close(ws):
        print("### closed ###")
    def on_open(ws):
        data = {
            "id": 5533,
            "action": "/api/v1/private/subscribe",
            "arguments": {
                "instrument": ["all"],
                "event": ["order_book", "trade"]
            }
        }
        data['sig'] = client.generate_signature(data['action'], data['arguments'])
        ws.send(json.dumps(data))
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://www.deribit.com/ws/api/v1/",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()
    return ws

def get_consumer():
    # https://jeqo.github.io/post/2017-01-31-kafka-rewind-consumers-offset/
    topic = 'dbitsub'
    # http://kafka-python.readthedocs.io/en/master/usage.html
    # doesn't work when group_id is specified dunno yet
    # consumer = KafkaConsumer(topic, group_id='my-group', auto_offset_reset='earliest', value_deserializer=lambda v: json.loads(v.decode('utf-8')))
    return KafkaConsumer(topic, group_id=None, auto_offset_reset='earliest', value_deserializer=lambda v: json.loads(v))
    # return KafkaConsumer(topic, group_id=None, auto_offset_reset='earliest', value_deserializer=lambda v: json.loads(v.decode('utf-8')))

class AttrDict(defaultdict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self

def state_reset():
    global state, trade_history
    state = AttrDict(lambda x: AttrDict())
    trade_history = AttrDict(list)

try:
    state
    trade_history
except NameError as e:
    state = None
    trade_history = None
    state_reset()

def state_summary():
    print('len(state)={}'.format(len(state)))
    for k in trade_history:
        # for now compute each time
        t = [x['timeStamp'] for x in trade_history[k]]
        mt, Mt = None, None
        if t:
            mt = datetime.datetime.fromtimestamp(min(t) / 1000)
            Mt = datetime.datetime.fromtimestamp(max(t) / 1000)
        print('len(trade_history[{}])={} over [{}, {}]'.format(k, len(trade_history[k]), mt, Mt))

_trade_event_keys = [
        # 'label',
        # 'me',
        # 'tradeId',
        # 'tradeSeq',
        'direction',
        'indexPrice',
        'instrument',
        'matchingId',
        'orderId',
        'price',
        'quantity',
        'selfTrade'
        'state',
        'tickDirection',
        'timeStamp',
        ]

# more complicated as more structure within the values
_order_book_event_keys = [
        'askIv', # implied vol
        'asks', # list of dicst of the ask side of order book
        'bidIv', # implied vol
        'bids', # list of dicst of the bid side of order book
        'high',
        'iR',
        'instrument',
        'last',
        'low',
        'mark',
        'markIv',
        'settlementPrice',
        'state',
        'tstamp',
        'uIx', # underlying name
        'uPx' # underlying ref price
        ]

def example_trade_event():
    return [{'message': 'trade_event',
  'result': [{'direction': 'sell',
    'indexPrice': 17385.92,
    'instrument': 'BTC-29DEC17',
    'label': '',
    'matchingId': 1815100178,
    'me': '',
    'orderId': 1815100036,
    'price': 17585.3,
    'quantity': 5,
    'selfTrade': False,
    'state': 'closed',
    'tickDirection': 1,
    'timeStamp': 1513722307530,
    'tradeId': 2974411,
    'tradeSeq': 1304657},
   {'direction': 'sell',
    'indexPrice': 17385.92,
    'instrument': 'BTC-29DEC17',
    'label': '',
    'matchingId': 1815100178,
    'me': '',
    'orderId': 1815100040,
    'price': 17585.3,
    'quantity': 4,
    'selfTrade': False,
    'state': 'closed',
    'tickDirection': 0,
    'timeStamp': 1513722307528,
    'tradeId': 2974410,
    'tradeSeq': 1304656}],
  'success': True,
  'testnet': False}]

def example_order_book_event():
    return [{'message': 'order_book_event',
  'result': {'askIv': 149.61,
   'asks': [{'cm': 1.1, 'price': 0.1434, 'quantity': 1.1},
    {'cm': 1.8, 'price': 0.1437, 'quantity': 0.7000000000000001},
    {'cm': 2.9, 'price': 0.1459, 'quantity': 1.1},
    {'cm': 3.9, 'price': 0.1531, 'quantity': 1.0},
    {'cm': 4.9, 'price': 0.27, 'quantity': 1.0},
    {'cm': 5.9, 'price': 0.45, 'quantity': 1.0}],
   'bidIv': 141.19,
   'bids': [{'cm': 1.0, 'price': 0.1387, 'quantity': 1.0},
    {'cm': 2.1, 'price': 0.1385, 'quantity': 1.1},
    {'cm': 3.2, 'price': 0.1368, 'quantity': 1.1},
    {'cm': 4.2, 'price': 0.1089, 'quantity': 1.0},
    {'cm': 4.3, 'price': 0.0484, 'quantity': 0.1}],
   'high': 0.1426,
   'iR': 0.0,
   'instrument': 'BTC-29DEC17-16000-C',
   'last': 0.1426,
   'low': 0.1413,
   'mark': 0.13803730589391372,
   'markIv': 140.0,
   'settlementPrice': 0.20169035420902914,
   'state': 'open',
   'tstamp': 1513722307500,
   'uIx': 'BTC-29DEC17',
   'uPx': 17590.0},
  'success': True,
  'testnet': False}]

def get_piecewise_price_vs_size_from_orderbook_entry(orders, mean=True):
    """ orders is just asks or just orders. takes maybe 300 micros though timeit reports much less """
    if not orders:
        return None
    cm = [0] + [x['cm'] for x in orders]
    # integral (price times qty) d_qty / qty
    # represent this as integral of piecewise polynomial with coeff [0, price]
    price = np.zeros((2, len(cm)-1))
    price[1,:] = [x['price'] for x in orders]
    f = PPoly(price, cm, extrapolate=False)
    F = f.antiderivative()
    if mean:
        # generally you want mean price if you took out the stack up to some size
        return lambda x: F(x) / x
    else:
        return F

def plot_order_book_event(result, fig=None):
    from pylab import plot, fig, linspace
    ion()
    # HERE HERE and then do backend + webviz
    asks = result['asks']
    # empty list means order book is empty or something else is wrong
    fasks = None
    if asks:
        fasks = get_piecewise_price_vs_size_from_orderbook_entry(asks)
    bids = result['bids']
    fbids = None
    if bids:
        fbids = get_piecewise_price_vs_size_from_orderbook_entry(bids)
    fig = figure(1)
    fig.clf()
    ax = fig.gca()
    n = 100
    max_size = 100
    x = linspace(1. / n, max_size, n) # size
    plot(x, fbids(x), '.-')
    plot(x, fasks(x), '.-')

def handle_trade_event(results):
    # print('handle trade event')
    # result is a list of dicts
    # for now just append the series of trades for no good reason
    for result in results:
        key = result['instrument']
        trade_history[key].append(result)

def handle_order_book_event(result):
    # print('handle order book event')
    # result is a dict
    global state
    key = result['instrument']
    state[key] = result
    asks = result['asks']
    result['fasks'] = get_piecewise_price_vs_size_from_orderbook_entry(asks)
    bids = result['bids']
    result['fbids'] = get_piecewise_price_vs_size_from_orderbook_entry(bids)

def handle_notification(notification):
    assert not notification['testnet']
    assert notification['success']
    message = notification['message']
    if message == 'trade_event':
        results = notification['result'] # this is a list of things?
        assert type(results) is list
        handle_trade_event(results)
    elif message == 'order_book_event':
        result = notification['result'] # this is not a list of things?
        assert type(result) is dict
        handle_order_book_event(result)
    else:
        raise Exception('not sure')

def handle_value(value):
    # nothing to do here
    if value.get('message', 'nope') == 'subscribed':
        print("got {}".format(value))
        return
    notifications = value['notifications']
    for x in notifications:
        handle_notification(x)

class ConsumerHelper():
    """
    This is a holder for the consumer not the state.
    State is module level global.
    Mostly for debug/dev.
    initial one looks like this: '{"id":5533,"success":true,"testnet":false,"message":"subscribed"}'
    others like this: '{"notifications":[{"success":true,"testnet":false,"message":"trade_event","result":[{"tradeId":2974411,"instrument":"BTC-29DEC17","timeStamp":1513722307530,"quantity":5,"price":17585.3,"direction":"sell","orderId":1815100036,"matchingId":1815100178,"tradeSeq":1304657,"tickDirection":1,"indexPrice":17385.92,"state":"closed","label":"","me":"","selfTrade":false},{"tradeId":2974410,"instrument":"BTC-29DEC17","timeStamp":1513722307528,"quantity":4,"price":17585.3,"direction":"sell","orderId":1815100040,"matchingId":1815100178,"tradeSeq":1304656,"tickDirection":0,"indexPrice":17385.92,"state":"closed","label":"","me":"","selfTrade":false}]}],"msOut":1513722307996136}'
    """
    def __init__(self, minutes_back=None):
        self._consumer = get_consumer()
        self.msg = None
        self.value = None
        self.notifications = None
        if minutes_back is not None:
            seekbackminutes(minutes_back, consumer=self._consumer)
    def notifications_generator(self, reduced=True, n=None):
        # indep of class
        for i, msg in enumerate(self._consumer):
            value = json.loads(msg.value)
            if value.get('message', 'nope') == 'subscribed':
                continue
            notifications = value['notifications']
            for notification in notifications:
                message = notification['message']
                results = notification['result']
                if message == 'order_book_event':
                    results = [results]
                elif message == 'trade_event':
                    pass
                else:
                    raise Exception("not sure what notify {}".format(message))
                for x in results:
                    if reduced:
                        # yield {'message': message, 'ts': x['timeStamp'] if 'timeStamp' in x else x['tstamp'], 'instrument': x['instrument']}
                        yield [message, x['timeStamp'] if 'timeStamp' in x else x['tstamp'], x['instrument']]
                    else:
                        yield message, x
            if n is not None and i > n:
                raise StopIteration
    def consume_n_with_value_callback(self, n, callback=None):
        if callback is None:
            callback = handle_value
        for i in range(n):
            self.consume_one()
            callback(self.value)
        state_summary()
    def consume_one(self):
        msg = self._consumer.__next__()
        # print('got one message:', msg.topic, datetime.datetime.fromtimestamp(msg.timestamp / 1000), msg.offset, msg.partition) # msg.value
        self.msg = msg
        self.value = json.loads(msg.value)
        self.notifications = self.value.get('notifications', None)

try:
    consumer
except Exception as e:
    try:
        consumer = ConsumerHelper()
    except Exception as e:
        print('possibly ok')
        print(e)

def seekbackminutes(offset_minutes, consumer):
    t = datetime.datetime.today() - datetime.timedelta(minutes=offset_minutes)
    part = consumer.partitions_for_topic('dbitsub')
    assert len(part) == 1
    part = part.pop() # take first expect only one
    tp = TopicPartition('dbitsub', part)
    consumer.unsubscribe() # you need this first
    consumer.assign([tp])
    offset = consumer.offsets_for_times({tp: t.timestamp() * 1000})
    print('seeking {} {}'.format(tp, offset[tp]))
    if offset[tp] is None:
        print('probably no data there')
    consumer.seek(tp, offset[tp].offset)

def count_eventtypes(consumer=None, offset_minutes=5):
    if consumer is None:
        consumer = get_consumer()
    d = defaultdict(lambda : 0)
    # first message should be subscribed message if start from beginning? TODO
    msg = consumer.__next__()
    t0 = msg.timestamp
    for i, msg in enumerate(consumer):
        x = json.loads(msg.value)
        if 'message' in x and x['message'] == 'subscribed':
            t0 = msg.timestamp
            continue
        notifications = x['notifications']
        assert len(notifications) == 1, 'wrong number of notifications'
        xx = notifications[0]
        key = xx['message']
        d[key] += 1
        if i % 10000 == 0:
            T = msg.timestamp - t0
            print(i, T)
            print(pd.Series(d) / T * 1000)

def count_events(consumer=None, offset_minutes=5):
    if consumer is None:
        consumer = get_consumer()
    d = defaultdict(lambda : 0)
    # first message should be subscribed message if start from beginning? TODO
    msg = consumer.__next__()
    t0 = msg.timestamp
    for i, msg in enumerate(consumer):
        x = json.loads(msg.value)
        if 'message' in x and x['message'] == 'subscribed':
            t0 = msg.timestamp
            continue
        notifications = x['notifications']
        assert len(notifications) == 1, 'wrong number of notifications'
        xx = notifications[0]
        key = (xx['message'], xx['result']['instrument'])
        d[key] += 1
        if i % 10000 == 0:
            T = msg.timestamp - t0
            print(i, T)
            print(pd.Series(d) / T * 1000)
            # print((pd.Series(d) / T * 1000).describe()) # timestamp is in ms?


a = None

# subscribe just gives the full order stack anyway ... it is not incremental. convenient but chatty
def test_websocket(start=False, instrument="all", event=["order_book", "trade"]):
    # example: ws = lib.test_websocket(instrument='BTC-29DEC17-17000-P')
    if type(event) is str:
        event = [event]
    def on_message(ws, message):
        global a
        a = message
        print(message)

    def on_error(ws, error):
        print(error)

    def on_close(ws):
        print("### closed ###")

    def on_open(ws):
        data = {
            "id": 5533,
            "action": "/api/v1/private/subscribe",
            "arguments": {
                "instrument": [instrument],
                "event": event
                # "event": ["order_book", "trade", "user_order"] // events to be reported, possible events:
                #                                       // "order_book" -- order book change
                #                                       // "trade" -- trade notification
                #                                       // "announcements" -- announcements (list of new announcements titles is send)
                #                                       // "user_order" -- change of user orders (openning, cancelling, filling)
                #                                       // "my_trade" -- filtered trade notification, only trades of the
            }
        }
        data['sig'] = client.generate_signature(data['action'], data['arguments'])

        ws.send(json.dumps(data))

    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://www.deribit.com/ws/api/v1/",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    if start:
        ws.run_forever()
    return ws
