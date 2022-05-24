import asyncio
import time
from decimal import Decimal
from yapic import json
from collections import defaultdict
from cryptofeed.symbols import Symbol
from cryptofeed.feed import Feed
from cryptofeed.defines import MEXC_FUTURES, L2_BOOK, TRADES, PERPETUAL, ASK, BID
from cryptofeed.connection import AsyncConnection, HTTPPoll, HTTPConcurrentPoll, RestEndpoint, Routes, WebsocketEndpoint
from cryptofeed.types import Trade, Ticker, Candle, Liquidation, Funding, OrderBook, OrderInfo, Balance
from typing import Tuple, Dict
import logging
import random

LOG = logging.getLogger('feedhandler')

class MexcFutures(Feed):
    id = MEXC_FUTURES
    websocket_endpoints = [WebsocketEndpoint('wss://contract.mxc.com/ws')]
    rest_endpoints = [RestEndpoint('https://contract.mxc.com', routes=Routes('/api/v1/contract/detail', l2book='/api/v1/contract/depth/{}?limit={}'))]
    websocket_channels = {
        L2_BOOK: 'sub.depth',
        TRADES: 'sub.deal',
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @classmethod
    def timestamp_normalize(cls, ts: float) -> float:
        return ts / 1000.0

    @classmethod
    def _parse_symbol_data(cls, data: dict) -> Tuple[Dict, Dict]:
        ret = {}
        info = defaultdict(dict)

        for entry in data["data"]:
            if entry["state"] != 0:
                continue
            s = Symbol(
                entry['baseCoin'].upper(),
                entry['quoteCoin'].upper(),
                type=PERPETUAL
            )
            ret[s.normalized] = entry['symbol']
            info['tick_size'][s.normalized] = entry['priceUnit']
            info['instrument_type'][s.normalized] = s.type
        return ret, info

    def __reset(self):
        self._l2_book = {}
        self.last_update_id = {}


    async def _snapshot(self, pair: str) -> None:
        max_depth = self.max_depth if self.max_depth else 1000

        resp = await self.http_conn.read(self.rest_endpoints[0].route('l2book', self.sandbox).format(pair, max_depth), retry_count=10, retry_delay=random.randint(5, 30))
        resp = json.loads(resp)

        std_pair = self.exchange_symbol_to_std_symbol(pair)
        self.last_update_id[std_pair] = resp['data']['timestamp']
        self._l2_book[std_pair] = OrderBook(self.id, std_pair, max_depth=self.max_depth, bids={Decimal(u[0]): Decimal(u[1]) for u in resp['data']['bids']}, asks={Decimal(u[0]): Decimal(u[1]) for u in resp['data']['asks']})
        await self.book_callback(L2_BOOK, self._l2_book[std_pair], time.time(), timestamp=self.timestamp_normalize(resp['data']['timestamp']), raw=resp, sequence_number=self.last_update_id[std_pair])

    async def _book(self, msg: dict, timestamp: float):
        symbol = msg['symbol']
        pair = self.exchange_symbol_to_std_symbol(symbol)

        if pair not in self._l2_book:
            await self._snapshot(symbol)

        if msg["ts"] < self.last_update_id[pair]:
            return

        delta = {BID: [], ASK: []}

        for s, side in (('bids', BID), ('asks', ASK)):
            for update in msg["data"][s]:
                price = Decimal(update[0])
                amount = Decimal(update[1])
                if amount == 0:
                    if price in self._l2_book[pair].book[side]:
                        del self._l2_book[pair].book[side][price]
                        delta[side].append((price, amount))
                else:
                    self._l2_book[pair].book[side][price] = amount
                    delta[side].append((price, amount))

        await self.book_callback(L2_BOOK, self._l2_book[pair], timestamp, timestamp=self.timestamp_normalize(msg['ts']), delta=delta, raw=msg, sequence_number=self.last_update_id[pair])


    async def message_handler(self, msg: str, conn: AsyncConnection, timestamp: float):
        msg = json.loads(msg)

        if msg['channel'] == 'push.depth':
            await self._book(msg, timestamp)
        elif msg['channel'] == 'rs.error':
            LOG.error("%s: Websocket subscribe failed %s", self.id, msg['data'])
        elif msg['channel'].startswith('rs'):
            return
        elif msg['channel'] in ("pong", "clientId"):
            return
        else:
            LOG.warning("%s: unexpected channel type received: %s", self.id, msg)

    async def subscribe(self, conn: AsyncConnection):
        self.__reset()

        asyncio.get_event_loop().create_task(self.pager(conn))

        for chan, symbols in self.subscription.items():
            for symbol in symbols:
                msg = {
                    "method": chan,
                    "param": {
                        "symbol": symbol,
                    },
                }
                await conn.write(json.dumps(msg))

    async def pager(self, conn: AsyncConnection):
        while True:
            await asyncio.sleep(10)
            await conn.write(json.dumps({"method": "ping"}))
