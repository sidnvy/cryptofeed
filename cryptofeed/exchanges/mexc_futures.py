import asyncio
import time
from decimal import Decimal
from yapic import json
from collections import defaultdict
from cryptofeed.symbols import Symbol
from cryptofeed.feed import Feed
from cryptofeed.defines import BUY, MEXC_FUTURES, L2_BOOK, SELL, TICKER, TRADES, PERPETUAL, ASK, BID
from cryptofeed.connection import AsyncConnection, HTTPPoll, HTTPConcurrentPoll, RestEndpoint, Routes, WebsocketEndpoint
from cryptofeed.types import Trade, Ticker, Candle, Liquidation, Funding, OrderBook, OrderInfo, Balance
from typing import Tuple, Dict
import logging
import random

LOG = logging.getLogger('feedhandler')

class MexcFutures(Feed):
    id = MEXC_FUTURES
    websocket_endpoints = [WebsocketEndpoint('wss://contract.mexc.com/ws')]
    rest_endpoints = [RestEndpoint('https://contract.mexc.com', routes=Routes('/api/v1/contract/detail', l2book='/api/v1/contract/depth/{}?limit={}'))]
    websocket_channels = {
        L2_BOOK: 'sub.depth.full',
        # L2_BOOK: 'sub.depth',
        TRADES: 'sub.deal',
        # TICKER: 'sub.ticker',  # quote ticker limit one symbol per connection
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


    async def _snapshot(self, symbol: str, timestamp: float) -> None:
        # await asyncio.sleep(random.randint(1,3))
        max_depth = self.max_depth if self.max_depth > 0 else 50

        resp = await self.http_conn.read(self.rest_endpoints[0].route('l2book', self.sandbox).format(symbol, max_depth), retry_count=10, retry_delay=random.randint(5, 30))
        resp = json.loads(resp)

        pair = self.exchange_symbol_to_std_symbol(symbol)
        server_time = int(resp['data']['timestamp'])
        version = resp['data']['version']
        self.last_update_id[pair] = version

        bids = {Decimal(u[0]): Decimal(u[1]) for u in resp['data']['bids']}
        asks = {Decimal(u[0]): Decimal(u[1]) for u in resp['data']['asks']}

        self._l2_book[pair] = OrderBook(self.id, pair, max_depth=self.max_depth, bids=bids, asks=asks)
        await self.book_callback(L2_BOOK, self._l2_book[pair], timestamp, timestamp=self.timestamp_normalize(server_time), raw=resp, sequence_number=version)

    async def _book_snapshot(self, msg: dict, timestamp: float):
        """
        {
            "channel": "push.depth.full",
            "data": {
                "asks": [
                    [
                        1900.95,
                        4578,
                        2
                    ],
                    [
                        1901,
                        1581,
                        1
                    ],
                    [
                        1901.05,
                        20952,
                        2
                    ],
                    [
                        1901.1,
                        4290,
                        2
                    ],
                    [
                        1901.15,
                        1586,
                        2
                    ]
                ],
                "bids": [
                    [
                        1900.8,
                        19074,
                        1
                    ],
                    [
                        1900.75,
                        1561,
                        2
                    ],
                    [
                        1900.7,
                        1326,
                        1
                    ],
                    [
                        1900.65,
                        4035,
                        2
                    ],
                    [
                        1900.6,
                        2261,
                        1
                    ]
                ],
                "version": 3925474132
            },
            "symbol": "ETH_USDT",
            "ts": 1653901834782
        }
        """
        symbol = msg['symbol']
        version = msg['data']['version']
        pair = self.exchange_symbol_to_std_symbol(symbol)
        bids = {Decimal(price): Decimal(amount) for price, amount, *_ in msg['data']['bids']}
        asks = {Decimal(price): Decimal(amount) for price, amount, *_ in msg['data']['asks']}
        self._l2_book[pair] = OrderBook(self.id, pair, max_depth=self.max_depth, bids=bids, asks=asks)
        await self.book_callback(L2_BOOK, self._l2_book[pair], timestamp, timestamp=self.timestamp_normalize(msg['ts']), raw=msg, sequence_number=version)

    async def _book(self, msg: dict, timestamp: float):
        """
        {
            "channel": "push.depth",
            "data": {
                "asks": [
                    [
                        29070,
                        11165,
                        5
                    ]
                ],
                "bids": [],
                "version": 5176219201
            },
            "symbol": "BTC_USDT",
            "ts": 1653649736598
        }
        """
        symbol = msg['symbol']
        pair = self.exchange_symbol_to_std_symbol(symbol)

        if pair not in self._l2_book:
            await self._snapshot(symbol, timestamp)

        version = msg['data']['version']
        if version <= self.last_update_id[pair]:
            return
        elif version > self.last_update_id[pair] + 1:
            del self._l2_book[pair]
            del self.last_update_id[pair]
            LOG.warning("%s: Missing %s book update detected, resetting book", self.id, pair)
            return

        self.last_update_id[pair] = version

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

        await self.book_callback(L2_BOOK, self._l2_book[pair], timestamp, timestamp=self.timestamp_normalize(msg['ts']), delta=delta, raw=msg, sequence_number=version)

    async def _trade(self, msg: dict, timestamp: float):
        """
        {
            "channel": "push.deal",
            "data": {
                "M": 1,
                "O": 3,
                "T": 1,
                "p": 29039,
                "t": 1653651010569,
                "v": 1181
            },
            "symbol": "BTC_USDT",
            "ts": 1653651010569
        }

        """
        data = msg['data']
        t = Trade(
            self.id,
            self.exchange_symbol_to_std_symbol(msg['symbol']),
            SELL if data['T'] == 2 else BUY,
            Decimal(data['v']),
            Decimal(data['p']),
            self.timestamp_normalize(data['t']),
            raw=msg
        )
        await self.callback(TRADES, t, timestamp)

    async def _ticker(self, msg: dict, timestamp: float):
        """
        {
            "channel": "push.ticker",
            "data": {
                "amount24": 1105314649.44675,
                "ask1": 29019.5,
                "bid1": 29019,
                "contractId": 10,
                "fairPrice": 29010.8,
                "fundingRate": 0.00015,
                "high24Price": 29791.5,
                "holdVol": 15202615,
                "indexPrice": 29006.2,
                "lastPrice": 29015,
                "lower24Price": 28000,
                "maxBidPrice": 31906.5,
                "minAskPrice": 26105.5,
                "riseFallRate": -0.0128,
                "riseFallValue": -378.5,
                "symbol": "BTC_USDT",
                "timestamp": 1653651419009,
                "volume24": 380388668
            },
            "symbol": "BTC_USDT",
            "ts": 1653651419009
        }
        """
        data = msg['data']
        t = Ticker(
            self.id, 
            self.exchange_symbol_to_std_symbol(data['symbol']),
            Decimal(data['bid1']),
            Decimal(data['ask1']),
            self.timestamp_normalize(data['timestamp']),
            raw=msg
        )
        await self.callback(TICKER, t, timestamp)

    async def message_handler(self, msg: str, conn: AsyncConnection, timestamp: float):
        msg = json.loads(msg)

        if msg['channel'] == 'push.depth':
            await self._book(msg, timestamp)
        elif msg['channel'] == 'push.depth.full':
            await self._book_snapshot(msg, timestamp)
        elif msg['channel'] == 'push.deal':
            await self._trade(msg, timestamp)
        elif msg['channel'] == 'push.ticker':
            await self._ticker(msg, timestamp)
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
                if chan == 'sub.depth.full':
                    msg['param']['limit'] = 5
                await conn.write(json.dumps(msg))

    async def pager(self, conn: AsyncConnection):
        while conn.is_open:
            await asyncio.sleep(10)
            await conn.write(json.dumps({"method": "ping"}))
