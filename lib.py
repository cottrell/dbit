from functools import lru_cache
import json
import deribit_api as da
import os
cred = json.load(open(os.path.abspath('~/.cred/deribit/deribit.json')))
# url = 'https://test.deribit.com'
url = 'https://www.deribit.com'
client = da.RestClient(cred['access_key'], cred['access_secret'], url=url)

getinstruments = lru_cache()(client.getinstruments)
getcurrencies = lru_cache()(client.getcurrencies)
