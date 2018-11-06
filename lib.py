from functools import lru_cache
import time
import pandas as pd
import datetime
import json
import deribit_api as da
import os
cred = json.load(open(os.path.expanduser('~/.cred/deribit/deribit.json')))
# url = 'https://test.deribit.com'
url = 'https://www.deribit.com'
client = da.RestClient(cred['access_key'], cred['access_secret'], url=url)

getinstruments = lru_cache()(client.getinstruments)
getcurrencies = lru_cache()(client.getcurrencies)

# https://jeqo.github.io/post/2017-01-31-kafka-rewind-consumers-offset/

import websocket
# pip install websocket-client # not websocket
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
# producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))
try:
    producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    topic = 'dbitsub'
    # http://kafka-python.readthedocs.io/en/master/usage.html
    # doesn't work when group_id is specified dunno yet
    # consumer = KafkaConsumer(topic, group_id='my-group', auto_offset_reset='earliest', value_deserializer=lambda v: json.loads(v.decode('utf-8')))
    consumer = KafkaConsumer(topic, group_id=None, auto_offset_reset='earliest', value_deserializer=lambda v: json.loads(v.decode('utf-8')))
except Exception as e:
    consumer = None
    print('possibly ok')
    print(e)

from collections import defaultdict

# TODO kafka commit

# 0. start kafka
# 1. websocket_to_kafka
# 2. kafka_consume(1) # to see a bit

def kafka_consume(minutes_back=None, consumer=consumer):
    if minutes_back is not None:
        seekbackminutes(minutes_back, consumer=consumer)
    for i in range(10):
        # msg in consumer:
        msg = consumer.__next__()
        print(msg.topic, datetime.datetime.fromtimestamp(msg.timestamp / 1000), msg.offset, msg.partition) # msg.value
    last_msg = msg
    return last_msg

def howtogetoffsetsmaybe(minutes, consumer=consumer):
    t = datetime.datetime.today() - datetime.timedelta(minutes=minutes)
    consumer.partitions_for_topic('dbitsub')
    tp = TopicPartition('dbitsub', 0)
    return consumer.offsets_for_times({tp: t.timestamp() * 1000})

def seekbackminutes(offset_minutes, consumer=consumer):
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

def count_eventtypes(consumer=consumer, offset_minutes=5):
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

def count_events(consumer=consumer, offset_minutes=5):
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


def websocket_to_kafka():
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

def test_kafka():
    producer = KafkaProducer(bootstrap_servers='localhost:1234')

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

"""
websocket docs

WebSocket API to subscribe to notifications.

action, URI Path: /api/v1/private/subscribe
Parameters:
{
    "id": 5533, // id, developer identifies the response by the sent id
    "action": "/api/v1/private/subscribe",  
    "arguments": {
        "instrument": ["BTC-19DEC14"], // instrument filter if applicable, i.e., list of instrument names,
                                       // see getinstruments REST API
                                   // also filters ["all"], ["futures"], ["options"] are allowed
                                       // ["all"] = no filter, all instruments, except "index"
                                       // ["futures"] = notification for futures    
                                       // ["options"] = notification for options
                                       // ["index"] = DRB price index notification
       "event": ["order_book", "trade", "user_order"] // events to be reported, possible events:
                                                      // "order_book" -- order book change 
                                                      // "trade" -- trade notification
                                                      // "announcements" -- announcements (list of new announcements titles is send)
                                                      // "user_order" -- change of user orders (openning, cancelling, filling)
                                                      // "my_trade" -- filtered trade notification, only trades of the
                                                      // subscribed user are reported with trade direction "buy"/"sell" from the 
                                                      // subscribed user point of view ("I sell ...", "I buy ..."), see below.
                                                      // Note, for "index" - events are ignored and can be [],
    },     
   "sig": "...." // required !
}
Response message is JSON object:
{
    "id": 5533,       // equal to the request id
    "success": true,  // true or false 
    "message": "subscribed", // subscribed or not
    "result": null
}
WebSocket notification message:
{
 "notifications": [ 
            // list of notifications (notification objects)
        .....
       ]
}
Following notification objects are possible:
trade_event
with list of the trades. JSON object:
{
     "success": true,  // false of true
      "message": "trade_event", // event type
      "result": [ // list of trades (for trade event)
            {
             "tradeId": 3,                    // trade id
             "timeStamp": 1418152290669, // Unix timestamp 
             "instrument": "BTC-22JAN16", // name of instrument
             "quantity": 61,          // quantity, in contracts ($10 per contract for futures, ฿1 — for options)   
             "price": 415,       // float, USD for futures, BTC for options
             "state": "closed",     // order state
             "direction": "buy",    // direction of the taker's order
             "orderId": 10,         // order id (the taker's order)
             "matchingId": 6,       // id of matching order (the maker's order)
             "makerComm": 0.0001,   
             "takerComm": 0.0005,
             "indexPrice": 420.69,   // index price
             "label":"",              // user defined label for the order from side
                                     // of the subscribed user (up to 4 chars)
             "me": "t",               // "t" - if the subscriber is taker, "m" - the subscriber is maker, "" - empty string
                                     // if the trade is between other users (trade without subsriber's participation)
                                     // can be used to quickly detect subscriber's trades between other user trades
             "tickDirection": 0     // Direction of the "tick". 
                                     // Valid values: 0 = Plus Tick; 
                                     //1 = Zero-Plus Tick; 2 = Minus Tick; 3 = Zero-Minus Tick
           }
        ]
     }

my_trade_event
with list of the filtered trades – only subscriber’s trades are reported. JSON object:
{
     "success": true,  // false of true
      "message": "my_trade_event", // event type
      "result": [ // list of trades (for trade event)
            {
             "tradeId": 3,                // trade id
             "timeStamp": 1418152290669,  // Unix timestamp 
         "instrument": "BTC-22JAN16", // name of instrument
             "quantity": 61,              // quantity, in contracts ($10 per contract for futures, ฿1 — for options)   
             "price": 415,                // float, USD for futures, BTC for options
             "state": "closed",           // order state
             "direction": "buy",          // direction of the subscriber's order (it describes what the subscriber does - sells or buys)
             "orderId": 10,               // order id (the taker's order)
             "matchingId": 6,       // id of matching order (the maker's order)
             "makerComm": 0.0001,   // maker fee
             "takerComm": 0.0005,   // taker fee
             "indexPrice": 420.69,  // index price
             "label":"",             // user defined label for the order from side
                                    // of the subscribed user (up to 4 chars)
             "tickDirection": 0,     // Direction of the "tick". 
                                     // Valid values: 0 = Plus Tick; 
                                     //1 = Zero-Plus Tick; 2 = Minus Tick;
                                     // 3 = Zero-Minus Tick
   ] 
}
order_book_event
It notifies about a change of order book for certain instrument. JSON object:
{
            "success": true,
            "message": "order_book_event",
            "result": {
                "instrument": "BTC-9OCT15",
                "bids": [
                    {
                        "quantity": 10, // quantity, in contracts ($10 per contract for futures, ฿1 — for options)
                        "price": 418.19, // float, USD for futures, BTC for options
                        "cm": 10  // cumulative quantity, in contracts ($10 per contract for futures, ฿1 — for options)
                    }
                    ...// next best bids
                ],
                "asks": [
                    {
                        "quantity": 10,
                        "price": 422.21,
                        "cm": 20
                    }
                    ...// next best asks  
                ],
                "last": 418.19,
                "low": 415.18,
                "high": 420.26
            }
        }
user_order_event
It notifies about a change of user’s orders. This event is triggered for all changes of the user orders, it doesn’t depend on “instrument” parameter at subscription. JSON object:
{   "success": true,
    "message": "user_orders_event",
    "result": [
                {
                    "id": 1031,                   // order identifier (for compatibility with older api version)
                    "orderId": 1031,              // order identifier 
                    "instrument": "BTC-22JAN16",  // instrument name
                    "direction": "sell",          // direction of the order "buy" or "sell"
                    "price": 426,                 // price, units depend on the asset type 
                    "quantity": 10,               // quantity, in contracts ($10 per contract for futures, ฿1 — for options)
                    "filledQuantity": 0,          // filled quantity, in contracts ($10 per contract for futures, ฿1 — for options)
                    "state": "open",              // order state "open", "cancelled", "filled"
                    "avgPrice": 0,                // average price 
                    "label": "",                  // order label if present 
                    "created": 1453454229858,     // creation Unix timestamp
                    "modified": 1453454229858     // Unix timestamp of the last change
                  }
              ]
}
announcements
It notifies with titles of new announcements. JSON object:
{
    "notifications": [
        {
            "success": true,
            "testnet": false,
            "message": "announcements",
            "result": [ // list of new announcements titles 
                "Welcome to Deribit!"
            ]
        }
    ]
}
default event for “index”
It notifies about price index value.  JSON object:
{
    "success": true,
     "message": "index",
     "result": {
         "btc": 1134.73, // current index price
         "edp": 1135.75  // estimated delivery price,
                         // the estimation of the price used for settlement 
                         // and delivery of contracts, calculated as the time
                         // weighted average of the Deribit index
                         // over the last half hour before expiration. It is equal
                         // to current price index most of time except last 30 min
                         // before expiration.
     }
}
unsubscribe

Unsubscribe from all notifications without closing the websocket connection. Currently there is no selectors.
action, URI Path: /api/v1/private/unsubscribe
Paramerts: none
{
    "id": 1798, // id, developer identifies the response by the sent id
    "action": "/api/v1/private/unsubscribe",
    "sig": "...."  // required
}
Response message is JSON object:
{
    "id": 1798,      // equal to the request id
    "success": true,
    "message": "unsubscribed",
    "result": null
}
"""
