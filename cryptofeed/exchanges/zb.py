from collections import defaultdict
from cryptofeed.symbols import Symbol
import logging
from decimal import Decimal
from typing import Dict, Tuple

from yapic import json

from cryptofeed.connection import AsyncConnection, RestEndpoint, Routes, WebsocketEndpoint
from cryptofeed.feed import Feed
from cryptofeed.defines import BID, ASK, BUY, TICKER, ZB as ZB_str, L2_BOOK, SELL, TRADES, SPOT
from cryptofeed.types import OrderBook, Trade, Ticker

LOG = logging.getLogger("feedhandler")

class Zb(Feed):
    id = ZB_str
    websocket_endpoints = [WebsocketEndpoint('wss://api.zb.com/websocket')]
    rest_endpoints = [RestEndpoint('https://api.zb.com', routes=Routes('/data/v1/markets'))]
    websocket_channels = {
        L2_BOOK: 'depth',
        TRADES: 'trades',
        TICKER: 'ticker',
    }
    request_limit = 60

    @classmethod
    def timestamp_normalize(cls, ts: float) -> float:
        return ts / 1000.0

    @classmethod
    def _parse_symbol_data(cls, data: dict) -> Tuple[Dict, Dict]:
        ret = {}
        info = defaultdict(dict)

        for symbol, entry in data.items():
            base, _, quote = symbol.partition('_')
            stype = SPOT
            s = Symbol(
                base.upper(),
                quote.upper(),
                type=stype
            )
            ret[s.normalized] = base+quote
            info['tick_size'][s.normalized] = entry['minAmount']  # zb does not provide tick size
            info['instrument_type'][s.normalized] = stype
        return ret, info

    def __reset(self):
        self._l2_book = {}
        self._last_server_time = defaultdict(int)

    async def _book(self, msg: dict, timestamp: float):
        """
        {
            "asks": [
                [
                    0.002463,
                    73.186
                ],
                [
                    0.002462,
                    3.876
                ],
                [
                    0.002448,
                    30.000
                ],
                [
                    0.002442,
                    6.000
                ],
                [
                    0.002435,
                    22.571
                ]
            ],
            "dataType": "depth",
            "bids": [
                [
                    0.002351,
                    22.576
                ],
                [
                    0.002349,
                    85.951
                ],
                [
                    0.002331,
                    0.375
                ],
                [
                    0.00233,
                    86.802
                ],
                [
                    0.002316,
                    105.524
                ]
            ],
            "channel": "ltcbtc_depth",
            "timestamp": 1653374164
        }

        """
        # Only snapshot
        symbol = msg['channel'].partition('_')[0]
        pair = self.exchange_symbol_to_std_symbol(symbol)

        server_time = int(msg['timestamp'])
        if server_time < self._last_server_time[pair]:
            LOG.warning(f"ZB {symbol} OrderBook update out of order")
            return
        self._last_server_time[pair] = server_time

        bids = {Decimal(price): Decimal(amount) for price, amount in msg['bids']}
        asks = {Decimal(price): Decimal(amount) for price, amount in msg['asks']}

        self._l2_book[pair] = OrderBook(self.id, pair, max_depth=self.max_depth, bids=bids, asks=asks)

        await self.book_callback(L2_BOOK, self._l2_book[pair], timestamp, timestamp=server_time, raw=msg)

    async def _trade(self, msg: dict, timestamp: float):
        """
        {
            "data": [
                {
                    "date": 1653374616,
                    "amount": "0.0027",
                    "price": "29282.7",
                    "trade_type": "bid",
                    "type": "buy",
                    "tid": 2786991818
                },
                {
                    "date": 1653374616,
                    "amount": "0.0346",
                    "price": "29282.7",
                    "trade_type": "bid",
                    "type": "buy",
                    "tid": 2786991819
                },
                {
                    "date": 1653374616,
                    "amount": "0.0034",
                    "price": "29282.7",
                    "trade_type": "bid",
                    "type": "buy",
                    "tid": 2786991820
                },
                {
                    "date": 1653374616,
                    "amount": "0.0707",
                    "price": "29284.13",
                    "trade_type": "ask",
                    "type": "sell",
                    "tid": 2786991821
                }
            ],
            "dataType": "trades",
            "channel": "btcusdt_trades"
        }

        """
        symbol = msg['channel'].partition('_')[0]
        pair = self.exchange_symbol_to_std_symbol(symbol)
        for trade in msg['data']:
            t = Trade(
                self.id,
                pair,
                BUY if trade['type'] == 'buy' else SELL,
                Decimal(trade['amount']),
                Decimal(trade['price']),
                trade['date'],
                id=str(trade["tid"]),
                raw=trade
            )
            await self.callback(TRADES, t, timestamp)

    async def _ticker(self, msg: dict, timestamp: float):
        """
        {
            "date": "1653460062279",
            "ticker": {
                "high": "0.002439",
                "vol": "24.942",
                "last": "0.002419",
                "low": "0.002419",
                "buy": "0.002333",
                "sell": "0.002431",
                "turnover": "0.060",
                "open": "0.002439",
                "riseRate": "-0.82"
            },
            "dataType": "ticker",
            "channel": "ltcbtc_ticker"
        }

        """
        symbol = msg['channel'].partition('_')[0]
        pair = self.exchange_symbol_to_std_symbol(symbol)
        t = Ticker(
            self.id,
            pair,
            Decimal(msg['ticker']['buy']),
            Decimal(msg['ticker']['sell']),
            self.timestamp_normalize(int(msg['date'])),
            raw=msg
        )
        await self.callback(TICKER, t, timestamp)


    async def message_handler(self, msg: str, conn: AsyncConnection, timestamp: float):
        msg = json.loads(msg, parse_float=Decimal)

        if 'errorCode' not in msg:
            if msg['dataType'] == 'depth':
                await self._book(msg, timestamp)
            elif msg['dataType'] == 'trades':
                await self._trade(msg, timestamp)
            elif msg['dataType'] == 'ticker':
                await self._ticker(msg, timestamp)
            else:
                LOG.warning("%s: unexpected channel type received: %s", self.id, msg)
        else:
            LOG.error("%s: Websocket subscribe failed %s", self.id, msg['errorMsg'])

    async def subscribe(self, conn: AsyncConnection):
        self.__reset()

        for chan, symbols in self.subscription.items():
            for symbol in symbols:
                msg = {"event": "addChannel", "channel": f"{symbol}_{chan}"}
                await conn.write(json.dumps(msg))

