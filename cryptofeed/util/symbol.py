from typing import List
import requests


def unify_exchange_name(exchange: str) -> str:
    if exchange.endswith("_FUTURES"):
        return exchange[:-8]
    elif exchange.endswith("_SWAP"):
        return exchange[:-5]
    else:
        return exchange

def cmc_hot_symbol_regex(limit: int) -> str:
    assets = cmc_hot_assets(limit)
    return f"({'|'.join(assets)})-.*"

def cmc_hot_assets(limit: int) -> List[str]:
    hot_assets = []
    url = "https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listing"
    response = requests.get(url, params={'start': 1, 'limit': limit, 'sortBy': 'market_cap', 'sortType': 'desc'})
    for item in response.json()['data']['cryptoCurrencyList']:
        hot_assets.append(item['symbol'])
    return hot_assets
