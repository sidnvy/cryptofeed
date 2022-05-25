
'''
Copyright (C) 2017-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from collections import defaultdict
from cryptofeed.symbols import Symbol
import logging
from decimal import Decimal
from typing import Dict, Tuple

from yapic import json

from cryptofeed.connection import AsyncConnection, RestEndpoint, Routes, WebsocketEndpoint
from cryptofeed.defines import BID, ASK, BUY, TICKER, ZB_FUTURES, L2_BOOK, SELL, TRADES, PERPETUAL
from cryptofeed.feed import Feed
from cryptofeed.types import OrderBook, Trade

LOG = logging.getLogger('feedhandler')


class ZbFutures(Feed):
    id = ZB_FUTURES
    websocket_endpoints = [
        WebsocketEndpoint('wss://fapi.zb.com/usdt/ws/public/v1', instrument_filter=('QUOTE', ('USDT',))),
        WebsocketEndpoint('wss://fapi.zb.com/qc/ws/public/v1', instrument_filter=('QUOTE', ('QC',)))
    ]
    rest_endpoints = [RestEndpoint('https://fapi.zb.com', routes=Routes(['/usdt/Server/api/v2/config/marketList', '/qc/Server/api/v2/config/marketList']))]
    websocket_channels = {
        L2_BOOK: 'DepthWhole',  # Depth increment update fails all the time (bid ask overlaps), use snapshot instead
        TRADES: 'Trade',
    }
    request_limit = 60

    @classmethod
    def timestamp_normalize(cls, ts: float) -> float:
        return ts / 1000.0

    @classmethod
    def _parse_symbol_data(cls, data: list) -> Tuple[Dict, Dict]:
        ret = {}
        info = defaultdict(dict)

        for d in data:
            for entry in d['data']:
                if entry['canTrade'] != True:
                    continue
                s = Symbol(
                    entry['sellerCurrencyName'].upper(),
                    entry['buyerCurrencyName'].upper(),
                    type=PERPETUAL
                )
                ret[s.normalized] = entry['symbol']
                info['tick_size'][s.normalized] = entry['minAmount']  # zb does not provide tick size
                info['instrument_type'][s.normalized] = s.type
        return ret, info

    def __reset(self):
        self._l2_book = {}
        # self._last_server_time = defaultdict(int)

    async def _book(self, msg: dict, timestamp: float):
        """
        {
            "channel": "BTC_USDT.Depth",
            "data": {
                "bids": [
                    [
                        29346.52,
                        0.34
                    ],
                    [
                        29351.29,
                        0.19
                    ],
                    [
                        29354.08,
                        0.24
                    ]
                ],
                "asks": [
                    [
                        29375.44,
                        0.15
                    ],
                    [
                        29375.45,
                        0
                    ],
                    [
                        29385.68,
                        0.209
                    ]
                ],
                "time": "1653376831866"
            }
        }

        """
        symbol = msg['channel'].partition('.')[0]
        pair = self.exchange_symbol_to_std_symbol(symbol)
        server_time = int(msg['data']['time'])

        # if server_time < self._last_server_time[pair]:
        #     LOG.warning(f"ZB_FUTURES {symbol} OrderBook update out of order")
        #     return
        # self._last_server_time[pair] = server_time

        if "type" in msg and msg['type'] == 'Whole':
            await self._book_snapshot(msg, timestamp)
        else:
            delta = {BID: [], ASK: []}
            for s, side in (('bids', BID), ('asks', ASK)):
                if s not in msg["data"]:
                    continue

                for update in msg["data"][s]:
                    price = Decimal(update[0])
                    amount = Decimal(update[1])

                    if amount == 0:
                        if price in self._l2_book[pair].book[side]:
                            delta[side].append((price, 0))
                            del self._l2_book[pair].book[side][price]
                    else:
                        delta[side].append((price, amount))
                        self._l2_book[pair].book[side][price] = amount

            await self.book_callback(L2_BOOK, self._l2_book[pair], timestamp, timestamp=self.timestamp_normalize(server_time), delta=delta, raw=msg, sequence_number=server_time)

    async def _book_snapshot(self, msg: dict, timestamp: float):
        symbol = msg['channel'].partition('.')[0]
        pair = self.exchange_symbol_to_std_symbol(symbol)

        # snapshot
        server_time = int(msg['data']['time'])
        # self._last_server_time[pair] = server_time

        bids = {Decimal(price): Decimal(amount) for price, amount in msg['data']['bids']}
        asks = {Decimal(price): Decimal(amount) for price, amount in msg['data']['asks']}
        self._l2_book[pair] = OrderBook(self.id, pair, max_depth=self.max_depth, bids=bids, asks=asks)
        await self.book_callback(L2_BOOK, self._l2_book[pair], timestamp, timestamp=self.timestamp_normalize(server_time), raw=msg, sequence_number=server_time)

    async def _trade(self, msg: dict, timestamp: float):
        """
        {
            "channel": "BTC_USDT.Trade",
            "data": [
                [
                    29355.03,   # price
                    0.143,      # quantity
                    1,          # side ( -1 sell, 1 buy)
                    1653377916  # timestamp
                ],
                [
                    29356.56,
                    0.291,
                    -1,
                    1653377916
                ]
            ]
        }
        """
        symbol = msg['channel'].partition('.')[0]
        pair = self.exchange_symbol_to_std_symbol(symbol)
        for trade in msg['data']:
            t = Trade(
                self.id,
                pair,
                BUY if trade[2] > 0 else SELL,
                Decimal(trade[1]),
                Decimal(trade[0]),
                trade[3],
                raw=trade
            )
            await self.callback(TRADES, t, timestamp)

    async def message_handler(self, msg: str, conn: AsyncConnection, timestamp: float):
        msg = json.loads(msg, parse_float=Decimal)

        if 'errorCode' not in msg:
            if msg['channel'].endswith('Depth'):
                await self._book(msg, timestamp)
            elif msg["channel"].endswith('DepthWhole'):
                await self._book_snapshot(msg, timestamp)
            elif msg['channel'].endswith('Trade'):
                await self._trade(msg, timestamp)
            else:
                LOG.warning("%s: unexpected channel type received: %s", self.id, msg)
        else:
            LOG.error("%s: Websocket subscribe failed %s", self.id, msg['errorMsg'])

    async def subscribe(self, conn: AsyncConnection):
        self.__reset()

        for chan, symbols in self.subscription.items():
            for symbol in symbols:
                msg = {"action": "subscribe", "channel": f"{symbol}.{chan}"}
                if chan == "Depth" and self.max_depth > 0:
                    msg["size"] = self.max_depth
                await conn.write(json.dumps(msg))

