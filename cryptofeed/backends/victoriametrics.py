
'''
Copyright (C) 2017-2022 Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.
'''
from collections import defaultdict
import logging

from cryptofeed.backends.backend import BackendBookCallback, BackendCallback
from cryptofeed.backends.socket import SocketCallback
from cryptofeed.defines import BID, ASK
from typing import Optional

from cryptofeed.util.symbol import unify_exchange_name


LOG = logging.getLogger('feedhandler')


class VictoriaMetricsCallback(SocketCallback):
    def __init__(self, addr: str, port: int, key: Optional[str] = None, **kwargs):
        """
        Parent class for VictoriaMetrics callbacks

        VictoriaMetrics support multiple protocol for data ingestion.
        In the following implementation we present data in InfluxDB Line Protocol format.

        InfluxDB schema
        ---------------
        MEASUREMENT | TAGS | FIELDS

        Measurement: Data Feed-Exchange (configurable)
        TAGS: symbol
        FIELDS: timestamp, amount, price, other funding specific fields

        InfluxDB Line Protocol to VictoriaMetrics storage
        -------------------------------------------------
        Please note that Field names are mapped to time series names prefixed with
        {measurement}{separator} value, where {separator} equals to _ by default.
        It can be changed with -influxMeasurementFieldSeparator command-line flag.

        For example, the following InfluxDB line:
            foo,tag1=value1,tag2=value2 field1=12,field2=40

        is converted into the following Prometheus data points:
            foo_field1{tag1="value1", tag2="value2"} 12
            foo_field2{tag1="value1", tag2="value2"} 40

        Ref: Ref: https://github.com/VictoriaMetrics/VictoriaMetrics#how-to-send-data-from-influxdb-compatible-agents-such-as-telegraf

        Parameters
        ----------
        addr: str
          Address for connection. Should be in the format:
          <protocol>://<address>
          Example:
          tcp://127.0.0.1
          udp://127.0.0.1
         port: int
           port for connection.
         key: str
           key to use when writing data
        """
        super().__init__(addr, port=port, key=key, numeric_type=float, **kwargs)

    async def write(self, data: dict):
        # Overwrite
        data['exchange'] = unify_exchange_name(data['exchange'])

        # Combine lines
        measurement = data['key'] if 'key' in data else self.key
        tags = self._tag_set(data)
        fields = self._field_set(data)
        timestamp = self._convert_sec_to_millisec(data['timestamp'])
        receipt_timestamp = self._convert_sec_to_millisec(data['receipt_timestamp'])

        line = f"{measurement},{tags} {fields} {timestamp}\n"
        line_by_receipt = f"{measurement}_origin,{tags} {fields} {receipt_timestamp}\n"

        print(line)
        print(line_by_receipt)
        await super().write(line+line_by_receipt)

    def _tag_set(self, data: dict) -> str:
        return f'exchange={data["exchange"]},symbol={data["symbol"]}'

    def _field_set(self, data: dict) -> str:
        return f'timestamp={data["timestamp"]},receipt_timestamp={data["receipt_timestamp"]}'

    def _convert_sec_to_millisec(self, sec: float) -> int:
        return int(sec * 1000)


class VictoriaMetricsBookCallback(VictoriaMetricsCallback):
    default_key = 'book'

    async def _write_rows(self, start, data):
        msg = []
        timestamp = data['timestamp']
        receipt_timestamp = data['receipt_timestamp']
        ts = int(timestamp * 1000000000) if timestamp else int(receipt_timestamp * 1000000000)
        for side in (BID, ASK):
            for price in data.book[side]:
                val = data.book[side][price]
                if isinstance(val, dict):
                    for order_id, amount in val.items():
                        msg.append(f'{start},side={side} id={order_id},receipt_timestamp={receipt_timestamp},timestamp={timestamp},price={price},amount={amount} {ts}')
                        ts += 1
                else:
                    msg.append(f'{start},side={side} receipt_timestamp={receipt_timestamp},timestamp={timestamp},price={price},amount={val} {ts}')
                    ts += 1
        await self.queue.put('\n'.join(msg) + '\n')


class TradeVictoriaMetrics(VictoriaMetricsCallback, BackendCallback):
    default_key = 'trades'

    def _field_set(self, data: dict) -> str:
        return f'price={data["price"]},amount={data["amount"]},timestamp={data["timestamp"]},receipt_timestamp={data["receipt_timestamp"]}'

class FundingVictoriaMetrics(VictoriaMetricsCallback, BackendCallback):
    default_key = 'funding'

    def _field_set(self, data: dict) -> str:
        return f'rate={data["rate"]},next_funding_time={data["next_funding_time"]},timestamp={data["timestamp"]},receipt_timestamp={data["receipt_timestamp"]}'


class BookVictoriaMetrics(VictoriaMetricsBookCallback, BackendBookCallback):
    def __init__(self, *args, snapshots_only=False, snapshot_interval=1000, extract_ticker=False, **kwargs):
        self.snapshots_only = snapshots_only
        self.snapshot_interval = snapshot_interval
        self.snapshot_count = defaultdict(int)
        self.extract_ticker = extract_ticker
        super().__init__(*args, **kwargs)

    # async def write(self, data):
    #     start = f"{self.key},exchange={data['exchange']},symbol={data['symbol']},delta={str('delta' in data)}"
    #     await self._write_rows(start, data)

    def _field_set(self, data: dict) -> str:
        return f'bid={data["bid"]},ask={data["ask"]},timestamp={data["timestamp"]},receipt_timestamp={data["receipt_timestamp"]}'


class TickerVictoriaMetrics(VictoriaMetricsCallback, BackendCallback):
    default_key = 'ticker'

    def _field_set(self, data: dict) -> str:
        return f'bid={data["bid"]},ask={data["ask"]},timestamp={data["timestamp"]},receipt_timestamp={data["receipt_timestamp"]}'


class OpenInterestVictoriaMetrics(VictoriaMetricsCallback, BackendCallback):
    default_key = 'open_interest'

    def _field_set(self, data: dict) -> str:
        return f'open_interest={data["open_interest"]},timestamp={data["timestamp"]},receipt_timestamp={data["receipt_timestamp"]}'


class LiquidationsVictoriaMetrics(VictoriaMetricsCallback, BackendCallback):
    default_key = 'liquidations'

    def _field_set(self, data: dict) -> str:
        return f'quantity={data["quantity"]},price={data["price"]},timestamp={data["timestamp"]},receipt_timestamp={data["receipt_timestamp"]}'


class CandlesVictoriaMetrics(VictoriaMetricsCallback, BackendCallback):
    default_key = 'candles'
