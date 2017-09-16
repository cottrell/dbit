from functools import lru_cache
import json
import deribit_api as da
cred = json.load(open('/Users/davidcottrell/.cred/deribit/deribit.json'))
# url = 'https://test.deribit.com'
url = 'https://www.deribit.com'
client = da.RestClient(cred['access_key'], cred['access_secret'], url=url)

getinstruments = lru_cache()(client.getinstruments)
getcurrencies = lru_cache()(client.getcurrencies)
