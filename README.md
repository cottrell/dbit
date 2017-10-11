airflow + dbit playground

see also https://github.com/cottrell/notebooks/tree/master/airflow


# example
    curl https://www.deribit.com/api/v1/public/getorderbook?instrument=BTC-29DEC17-1500-P
    {"success":true,"testnet":false,"message":"","result":{"instrument":"BTC-29DEC17-1500-P","bids":[{"quantity":10.0,"price":0.0005,"cm":10.0}],"asks":[{"quantity":3.0,"price":0.006,"cm":3.0}],"tstamp":1507752217072,"last":0.0069,"low":"","high":"","mark":0.00015522947071283107},"msIn":1507752236315,"msOut":1507752236315}
