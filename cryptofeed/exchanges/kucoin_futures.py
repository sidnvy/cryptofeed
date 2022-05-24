
'''
Copyright (C) 2017-2022 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from decimal import Decimal
import logging
import time
from typing import Dict, Tuple
import hmac
import base64
import hashlib

from yapic import json

from cryptofeed.defines import ASK, BID, BUY, CANDLES, FUTURES, KUCOIN_FUTURES, L2_BOOK, PERPETUAL, SELL, TICKER, TRADES
from cryptofeed.feed import Feed
from cryptofeed.util.time import timedelta_str_to_sec
from cryptofeed.symbols import Symbol
from cryptofeed.connection import AsyncConnection, RestEndpoint, Routes, WebsocketEndpoint
from cryptofeed.types import OrderBook, Trade, Ticker, Candle


LOG = logging.getLogger('feedhandler')


class KuCoinFutures(Feed):
    id = KUCOIN_FUTURES
    websocket_endpoints = None
    rest_endpoints = [RestEndpoint('https://api-futures.kucoin.com', routes=Routes('/api/v1/contracts/active', l2book='/api/v1/level2/snapshot?symbol={}'))]
    valid_candle_intervals = {'1m', '3m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '1w'}
    candle_interval_map = {'1m': '1min', '3m': '3min', '15m': '15min', '30m': '30min', '1h': '1hour', '2h': '2hour', '4h': '4hour', '6h': '6hour', '8h': '8hour', '12h': '12hour', '1d': '1day', '1w': '1week'}
    websocket_channels = {
        L2_BOOK: '/contractMarket/level2',
        TRADES: '/contractMarket/execution',
        TICKER: '/contractMarket/tickerV2',
    }

    @classmethod
    def _parse_symbol_data(cls, data: dict) -> Tuple[Dict, Dict]:
        ret = {}
        info = {'tick_size': {}, 'instrument_type': {}}
        for symbol in data['data']:
            s = Symbol(
                'BTC' if symbol['baseCurrency'] == 'XBT' else symbol['baseCurrency'],  # Convert XBT to BTC
                symbol['quoteCurrency'],
                expiry_date=None if symbol['type'] == "FFWCSX" else symbol['expireDate'] / 1000,
                type=PERPETUAL if symbol['type'] == "FFWCSX" else FUTURES,
            )
            info['tick_size'][s.normalized] = symbol['tickSize']
            ret[s.normalized] = symbol['symbol']
            info['instrument_type'][s.normalized] = s.type
        return ret, info

    def __init__(self, **kwargs):
        address_info = self.http_sync.write('https://api-futures.kucoin.com/api/v1/bullet-public', json=True)
        token = address_info['data']['token']
        address = address_info['data']['instanceServers'][0]['endpoint']
        address = f"{address}?token={token}"
        self.websocket_endpoints = [WebsocketEndpoint(address, options={'ping_interval': address_info['data']['instanceServers'][0]['pingInterval'] / 2000})]
        super().__init__(**kwargs)
        if any([len(self.subscription[chan]) > 100 for chan in self.subscription]):
            raise ValueError("Kucoin futures has a limit of 100 symbols per connection")
        self.__reset()

    def __reset(self):
        self._l2_book = {}
        self.seq_no = {}

    async def _ticker(self, msg: dict, symbol: str, timestamp: float):
        """
        {
            "type": "message",
            "topic": "/contractMarket/tickerV2:XBTUSDTM",
            "subject": "tickerV2",
            "data": {
                "symbol": "XBTUSDTM",
                "sequence": 1643179263771,
                "bestBidSize": 23489,
                "bestBidPrice": "29281.0",
                "bestAskPrice": "29282.0",
                "ts": 1653393615604191200,
                "bestAskSize": 35647
            }
        }
        """
        t = Ticker(
            self.id,
            self.exchange_symbol_to_std_symbol(symbol),
            Decimal(msg['data']['bestBidPrice']),
            Decimal(msg['data']['bestAskPrice']), 
            float(msg['data']['ts']) / 1000000000,
            raw=msg
        )

        await self.callback(TICKER, t, timestamp)

    async def _trades(self, msg: dict, symbol: str, timestamp: float):
        """
        {
            "type": "message",
            "topic": "/contractMarket/execution:XBTUSDTM",
            "subject": "match",
            "data": {
                "makerUserId": "626fbf71da3ba3000108b654",
                "symbol": "XBTUSDTM",
                "sequence": 73438434,
                "side": "sell",
                "size": 3,
                "price": 29238,
                "takerOrderId": "628cc819c6793c000129ba81",
                "makerOrderId": "628cc800ddfea800012d6fe6",
                "takerUserId": "625d5ddd127f6b000193af49",
                "tradeId": "628cc8193c7feb352310767c",
                "ts": 1653393433803646838
            }
        }
        """
        t = Trade(
            self.id,
            self.exchange_symbol_to_std_symbol(symbol),
            BUY if msg['data']['side'] == 'buy' else SELL,
            Decimal(msg['data']['size']),
            Decimal(msg['data']['price']),
            float(msg['data']['ts']) / 1000000000,
            id=msg['data']['tradeId'],
            raw=msg
        )
        await self.callback(TRADES, t, timestamp)

    async def _snapshot(self, symbol: str):
        data = await self.http_conn.read(self.rest_endpoints[0].route('l2book', self.sandbox).format(symbol))
        pair = self.exchange_symbol_to_std_symbol(symbol)
        timestamp = time.time()
        data = json.loads(data, parse_float=Decimal)
        data = data['data']
        self.seq_no[pair] = int(data['sequence'])
        bids = {Decimal(price): Decimal(amount) for price, amount in data['bids']}
        asks = {Decimal(price): Decimal(amount) for price, amount in data['asks']}
        self._l2_book[pair] = OrderBook(self.id, pair, max_depth=self.max_depth, bids=bids, asks=asks)

        await self.book_callback(L2_BOOK, self._l2_book[pair], timestamp, raw=data, sequence_number=int(data['sequence']))

    async def _process_l2_book(self, msg: dict, symbol: str, timestamp: float):
        """
        {
            "type": "message",
            "topic": "/contractMarket/level2:XBTUSDTM",
            "subject": "level2",
            "data": {
                "sequence": 1644892621579,
                "change": "29361.0,sell,2172",
                "timestamp": 1653390554867
            }
        }
        """
        data = msg['data']
        sequence = data['sequence']
        pair = self.exchange_symbol_to_std_symbol(symbol)
        if pair not in self._l2_book or sequence > self.seq_no[pair] + 1:
            if pair in self.seq_no and sequence > self.seq_no[pair] + 1:
                LOG.warning("%s: Missing book update detected, resetting book", self.id)
            await self._snapshot(symbol)

        data = msg['data']
        if sequence < self.seq_no[pair]:
            return

        self.seq_no[pair] = data['sequence']

        delta = {BID: [], ASK: []}
        update = data['change'].split(',')
        
        price = Decimal(update[0])
        side = BID if update[1] == 'buy' else ASK
        amount = Decimal(update[2])

        if amount == 0:
            if price in self._l2_book[pair].book[side]:
                del self._l2_book[pair].book[side][price]
                delta[side].append((price, amount))
        else:
            self._l2_book[pair].book[side][price] = amount
            delta[side].append((price, amount))

        await self.book_callback(L2_BOOK, self._l2_book[pair], timestamp, delta=delta, raw=msg, sequence_number=data['sequence'])

    async def message_handler(self, msg: str, conn, timestamp: float):
        msg = json.loads(msg, parse_float=Decimal)

        if 'topic' not in msg:
            if msg['type'] == 'error':
                LOG.warning("%s: error from exchange %s", self.id, msg)
                return
            elif msg['type'] in {'welcome', 'ack'}:
                return
            else:
                LOG.warning("%s: Unhandled message type %s", self.id, msg)
                return

        topic, symbol = msg['topic'].split(":", 1)
        topic = self.exchange_channel_to_std(topic)

        if topic == TICKER:
            await self._ticker(msg, symbol, timestamp)
        elif topic == TRADES:
            await self._trades(msg, symbol, timestamp)
        elif topic == CANDLES:
            await self._candles(msg, symbol, timestamp)
        elif topic == L2_BOOK:
            await self._process_l2_book(msg, symbol, timestamp)
        else:
            LOG.warning("%s: Unhandled message type %s", self.id, msg)

    async def subscribe(self, conn: AsyncConnection):
        self.__reset()
        for chan in self.subscription:
            for symbol in self.subscription[chan]:
                await conn.write(json.dumps({
                    'id': 1,
                    'type': 'subscribe',
                    'topic': f"{chan}:{symbol}",
                    # 'privateChannel': False,
                    'response': True
                }))
