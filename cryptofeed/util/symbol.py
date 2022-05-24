def unify_exchange_name(exchange: str) -> str:
    if exchange.endswith("_FUTURES"):
        return exchange[:-8]
    elif exchange.endswith("_SWAP"):
        return exchange[:-5]
    else:
        return exchange
