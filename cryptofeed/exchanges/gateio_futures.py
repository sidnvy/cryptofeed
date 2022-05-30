
'''
Copyright (C) 2017-2021  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from collections import defaultdict
import logging
from decimal import Decimal
import time
from typing import Dict, Tuple

from yapic import json

from cryptofeed.connection import AsyncConnection, RestEndpoint, Routes, WebsocketEndpoint
from cryptofeed.defines import BID, ASK, CANDLES, GATEIO_FUTURES, L2_BOOK, TICKER, TRADES, BUY, SELL, PERPETUAL
from cryptofeed.feed import Feed
from cryptofeed.symbols import Symbol
from cryptofeed.types import OrderBook, Trade, Ticker, Candle
from cryptofeed.util.time import timedelta_str_to_sec


LOG = logging.getLogger('feedhandler')


class GateioFutures(Feed):
    id = GATEIO_FUTURES
    websocket_endpoints = [WebsocketEndpoint('wss://fx-ws.gateio.ws/v4/ws/usdt', options={'compression': None})]
    rest_endpoints = [RestEndpoint('https://api.gateio.ws', routes=Routes('/api/v4/futures/usdt/contracts', l2book='/api/v4/futures/usdt/order_book?contract={}&with_id=true'))]

    valid_candle_intervals = {'10s', '1m', '5m', '15m', '30m', '1h', '4h', '8h', '1d', '3d'}
    websocket_channels = {
        L2_BOOK: 'futures.order_book_update',  # NOT CORRECT!
        TRADES: 'futures.trades',
        # CANDLES: 'spot.candlesticks'
    }

    @classmethod
    def _parse_symbol_data(cls, data: dict) -> Tuple[Dict, Dict]:
        ret = {}
        info = {'instrument_type': {}}

        for entry in data:
            if entry["in_delisting"]:
                continue
            base, _, quote = entry["name"].partition("_")
            s = Symbol(base, quote, type=PERPETUAL)
            ret[s.normalized] = entry['name']
            info['instrument_type'][s.normalized] = s.type
        return ret, info

    def _reset(self):
        self._l2_book = {}
        self.last_update_id = {}
        self.forced = defaultdict(bool)

    async def _ticker(self, msg: dict, timestamp: float):
        """
        """
        t = Ticker(
            self.id,
            self.exchange_symbol_to_std_symbol(msg['result']['currency_pair']),
            Decimal(msg['result']['highest_bid']),
            Decimal(msg['result']['lowest_ask']),
            float(msg['time']),
            raw=msg
        )
        await self.callback(TICKER, t, timestamp)

    async def _trades(self, msg: dict, timestamp: float):
        """
        {
          "channel": "futures.trades",
          "event": "update",
          "time": 1541503698,
          "result": [
            {
              "size": -108,
              "id": 27753479,
              "create_time": 1545136464,
              "create_time_ms": 1545136464123,
              "price": "96.4",
              "contract": "BTC_USD"
            }
          ]
        }
        """
        for result in msg['result']:
            t = Trade(
                self.id,
                self.exchange_symbol_to_std_symbol(result['contract']),
                SELL if result['size'] < 0 else BUY,
                Decimal(result['size']),
                Decimal(result['price']),
                float(result['create_time_ms']) / 1000,
                id=str(result['id']),
                raw=msg
            )
            await self.callback(TRADES, t, timestamp)

    async def _snapshot(self, symbol: str):
        """
        {
            "id": 2679059670,
            "asks": [[price, amount], [...], ...],
            "bids": [[price, amount], [...], ...]
        }
        """
        ret = await self.http_conn.read(self.rest_endpoints[0].route('l2book', self.sandbox).format(symbol))
        data = json.loads(ret, parse_float=Decimal)

        pair = self.exchange_symbol_to_std_symbol(symbol)
        self._l2_book[pair] = OrderBook(self.id, pair, max_depth=self.max_depth)
        self.last_update_id[pair] = data['id']
        self._l2_book[pair].book.bids = {Decimal(entry["p"]): Decimal(entry["s"]) for entry in data['bids']}
        self._l2_book[pair].book.asks = {Decimal(entry["p"]): Decimal(entry["s"]) for entry in data['asks']}
        await self.book_callback(L2_BOOK, self._l2_book[pair], time.time(), raw=data, sequence_number=data['id'])

    def _check_update_id(self, pair: str, msg: dict) -> Tuple[bool, bool]:
        skip_update = False
        forced = not self.forced[pair]

        if forced and msg['u'] <= self.last_update_id[pair]:
            skip_update = True
        elif forced and msg['U'] <= self.last_update_id[pair] + 1 <= msg['u']:
            self.last_update_id[pair] = msg['u']
            self.forced[pair] = True
        elif not forced and self.last_update_id[pair] + 1 == msg['U']:
            self.last_update_id[pair] = msg['u']
        else:
            # self._reset()
            del self.last_update_id[pair]
            del self._l2_book[pair]
            self.forced[pair] = False
            LOG.warning("%s: Missing book %s update detected, resetting book", self.id, pair)
            skip_update = True

        return skip_update

    async def _process_l2_book(self, msg: dict, timestamp: float):
        """
        {
          "time": 1615366381,
          "channel": "futures.order_book_update",
          "event": "update",
          "error": null,
          "result": {
            "t": 1615366381417,
            "s": "BTC_USD",
            "U": 2517661101,
            "u": 2517661113,
            "b": [
              {
                "p": "54672.1",
                "s": 0
              },
              {
                "p": "54664.5",
                "s": 58794
              }
            ],
            "a": [
              {
                "p": "54743.6",
                "s": 0
              },
              {
                "p": "54742",
                "s": 95
              }
            ]
          }
        }
        """
        pair = self.exchange_symbol_to_std_symbol(msg['result']['s'])
        if pair not in self._l2_book:
            await self._snapshot(msg['result']['s'])

        skip_update = self._check_update_id(pair, msg['result'])
        if skip_update:
            return

        ts = msg['result']['t'] / 1000
        delta = {BID: [], ASK: []}

        for s, side in (('b', BID), ('a', ASK)):
            for update in msg['result'][s]:
                price = Decimal(update["p"])
                amount = Decimal(update["s"])

                if amount == 0:
                    if price in self._l2_book[pair].book[side]:
                        del self._l2_book[pair].book[side][price]
                        delta[side].append((price, amount))
                else:
                    self._l2_book[pair].book[side][price] = amount
                    delta[side].append((price, amount))

        await self.book_callback(L2_BOOK, self._l2_book[pair], timestamp, delta=delta, timestamp=ts, raw=msg)

    async def _candles(self, msg: dict, timestamp: float):
        """
        {
            'time': 1619092863,
            'channel': 'spot.candlesticks',
            'event': 'update',
            'result': {
                't': '1619092860',
                'v': '1154.64627',
                'c': '54992.64',
                'h': '54992.64',
                'l': '54976.29',
                'o': '54976.29',
                'n': '1m_BTC_USDT'
            }
        }
        """
        interval, symbol = msg['result']['n'].split('_', 1)
        if interval == '7d':
            interval = '1w'
        c = Candle(
            self.id,
            self.exchange_symbol_to_std_symbol(symbol),
            float(msg['result']['t']),
            float(msg['result']['t']) + timedelta_str_to_sec(interval) - 0.1,
            interval,
            None,
            Decimal(msg['result']['o']),
            Decimal(msg['result']['c']),
            Decimal(msg['result']['h']),
            Decimal(msg['result']['l']),
            Decimal(msg['result']['v']),
            None,
            float(msg['time']),
            raw=msg
        )
        await self.callback(CANDLES, c, timestamp)

    async def message_handler(self, msg: str, conn, timestamp: float):
        msg = json.loads(msg, parse_float=Decimal)

        if "error" in msg:
            if msg['error'] is None:
                pass
            else:
                LOG.warning("%s: Error received from exchange - %s", self.id, msg)
        if msg['event'] == 'subscribe':
            return
        elif 'channel' in msg:
            market, channel = msg['channel'].split('.')
            if channel == 'book_ticker':
                await self._ticker(msg, timestamp)
            elif channel == 'trades':
                await self._trades(msg, timestamp)
            elif channel == 'order_book_update':
                await self._process_l2_book(msg, timestamp)
            elif channel == 'candlesticks':
                await self._candles(msg, timestamp)
            else:
                LOG.warning("%s: Unhandled message type %s", self.id, msg)
        else:
            LOG.warning("%s: Invalid message type %s", self.id, msg)

    async def subscribe(self, conn: AsyncConnection):
        self._reset()
        for chan in self.subscription:
            symbols = self.subscription[chan]
            nchan = self.exchange_channel_to_std(chan)
            if nchan in {L2_BOOK, CANDLES}:
                for symbol in symbols:
                    await conn.write(json.dumps(
                        {
                            "time": int(time.time()),
                            "channel": chan,
                            "event": 'subscribe',
                            "payload": [symbol, '100ms', "5"] if nchan == L2_BOOK else [self.candle_interval, symbol],
                        }
                    ))
            else:
                await conn.write(json.dumps(
                    {
                        "time": int(time.time()),
                        "channel": chan,
                        "event": 'subscribe',
                        "payload": symbols,
                    }
                ))
