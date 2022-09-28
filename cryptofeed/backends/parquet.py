import atexit
from typing import Optional
from collections import defaultdict
import urllib.parse
import s3fs
import os
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

from cryptofeed.backends.backend import BackendBookCallback, BackendCallback
from cryptofeed.defines import CANDLES, FUNDING, OPEN_INTEREST, TICKER, TRADES, LIQUIDATIONS, INDEX, L2_BOOK

class ParquetCallback:
    DATA_TYPES = [TRADES, L2_BOOK, OPEN_INTEREST, CANDLES, FUNDING, LIQUIDATIONS]
    PARTITION_COLS = {
        TRADES: ['exchange', 'symbol', 'date'],
        TICKER: ['exchange', 'symbol', 'date'],
        L2_BOOK: ['exchange', 'symbol', 'date'],
        OPEN_INTEREST: ['exchange', 'symbol', 'date'],
        CANDLES: ['exchange', 'symbol', 'date'],
        FUNDING: ['exchange', 'symbol', 'date'],
        LIQUIDATIONS: ['exchange', 'symbol', 'date']
    }
    TIMESTAMP_FORMAT = {
        TRADES: '%Y%m%d',
        L2_BOOK: '%Y%m%d',
        OPEN_INTEREST: '%Y%m',
        LIQUIDATIONS: '%Y%m',
        CANDLES: '%Y%m',
        FUNDING: '%Y%m',
    }
    DTYPES = {
        TRADES: {
            'exchange': 'string',
            'symbol': 'string',
            'date': 'int64',
            'side': 'string',
            'timestamp': 'timestamptime64[ns, UTC]',
            'amount': 'float64',
            'price': 'float64',
        },
        L2_BOOK: {
            'exchange': 'string',
            'symbol': 'string',
            'date': 'int64',
            'timestamp': 'timestamptime64[ns, UTC]',
            'book': 'float64',
        },
        OPEN_INTEREST: {
            'exchange': 'string',
            'symbol': 'string',
            'date': 'int64',
            'timestamp': 'timestamptime64[ns, UTC]',
            'open_interest': 'float64',
        },
        LIQUIDATIONS: {
            'exchange': 'string',
            'symbol': 'string',
            'date': 'int64',
            'timestamp': 'timestamptime64[ns, UTC]',
            'side': 'string',
            'quantity': 'float64',
            'price': 'float64',
        },
        CANDLES: {
            'exchange': 'string',
            'symbol': 'string',
            'date': 'int64',
            'timestamp': 'timestamptime64[ns, UTC]',
            'close_time': 'timestamptime64[ns, UTC]',
            'open': 'float64',
            'high': 'float64',
            'low': 'float64',
            'close': 'float64',
            'volume': 'float64',
            'quote_volume': 'float64',
            'taker_buy_volume': 'float64',
            'taker_buy_quote_volume': 'float64',
            'trades': 'int64',
        },
        FUNDING: {
            'exchange': 'string',
            'symbol': 'string',
            'date': 'int64',
            'timestamp': 'timestamptime64[ns, UTC]',
            'next_funding_time': 'timestamptime64[ns, UTC]',
            'mark_price': 'float64',
            'rate': 'float64',
            'predicted_rate': 'float64',
        }
    }
    RETURNING_INDEX = {
        'orderbook': ['timestamp', 'side', 'level'],
        'open_interest': 'timestamp',
        'long_short_ratio': 'timestamp',
        'candle': 'timestamp',
        'funding_rate': 'timestamp',
    }
   
    def __init__(self, url: str, numeric_type = float, none_to = None, data_type: Optional[str] = None, **kwargs):
        parsed_url = urllib.parse.urlparse(url)
        path = parsed_url.path
        if path[0] == '/':
            path = path[1:]
        if not path.endswith('/'):
            path = path + '/'
        if parsed_url.scheme == 'file' or (parsed_url.scheme == '' and parsed_url.netloc == ''):
            if parsed_url.scheme == '' and parsed_url.netloc == '':
                path = parsed_url.path
            path = os.path.expanduser(path)
            self.fs = None
            self.fs_makedirs = os.makedirs
            self.fs_listdir = os.listdir
        elif parsed_url.scheme == 's3':
            args = {}
            if parsed_url.hostname is not None:
                if parsed_url.port is not None:
                    endpoint_url = f'https://{parsed_url.hostname}:{parsed_url.port}'
                else:
                    endpoint_url = f'https://{parsed_url.hostname}'
                args['client_kwargs'] = {
                    'endpoint_url': endpoint_url,
                }
                args['config_kwargs'] = {
                    's3': {
                        'addressing_style': 'virtual',  # compatible with aliyun oss
                    },
                }
            if parsed_url.username is None or parsed_url.password is None:
                args['anon'] = True
            else:
                args['key'] = parsed_url.username
                args['secret'] = parsed_url.password
            
            self.fs = s3fs.S3FileSystem(**args)
            self.fs_makedirs = self.fs.makedirs
            self.fs_listdir = self.fs.ls
        else:
            raise ValueError(f'unrecognized url {url}')
            
        self.dtype = data_type if data_type else self.default_dtype
        self.numeric_type = numeric_type
        self.none_to = none_to
        self.root_path = path + 'parquet/'
        self.dtype_path = f'{self.root_path}{self.dtype}'
        self.buffer = []
        self.fs_makedirs(self.dtype_path, exist_ok=True)

        atexit.register(self.close)

        
    @staticmethod
    def _need_flush(buffer, new_data, partition_cols):
        return (
                len(buffer) > 0
                and
                (new_data.loc[new_data.index[0], partition_cols] != buffer[-1].loc[
                    buffer[-1].index[-1], partition_cols]).any()
        )
    
    @staticmethod
    def _need_immediate_flush(buffer, new_data, partition_cols):
        return (new_data.loc[new_data.index[0], partition_cols] != new_data.loc[new_data.index[-1], partition_cols]).any()
    
    def _flush(self, buffer, path, partition_cols):
        if len(buffer) == 0:
            return

        pq.write_to_dataset(
            pa.Table.from_pandas(pd.concat(buffer, ignore_index=True), preserve_index=False),
            root_path=path,
            partition_cols=partition_cols,
            filesystem=self.fs,
            coerce_timestamps="us",
            allow_truncated_timestamps=True
        )
        
        buffer.clear()
        
    def _append_data(self, buffer, path, new_data, partition_cols):
        if self._need_immediate_flush(buffer, new_data, partition_cols):
            buffer.append(new_data)
            self._flush(buffer, path, partition_cols)
        elif self._need_flush(buffer, new_data, partition_cols):
            self._flush(buffer, path, partition_cols)
            buffer.append(new_data)
        else:
            buffer.append(new_data)
                
    async def write(self, data: dict):
        # data to pandas df
        df = pd.DataFrame({key: [value] for key, value in data.items()})
        df['timestamp'] = pd.to_datetime(df.timestamp, unit='s')
        df['receipt_timestamp'] = pd.to_datetime(df.receipt_timestamp, unit='s')
        df.set_index(['timestamp'])

        for col in self.PARTITION_COLS[self.dtype]:
            if col == 'date':
                df['date'] = df.timestamp.dt.strftime(self.TIMESTAMP_FORMAT[self.dtype])
        # df = df.astype(self.DTYPES[self.dtype])
        self._append_data(self.buffer, self.dtype_path, df, self.PARTITION_COLS[self.dtype])
        
    def close(self):
        atexit.unregister(self.close)
        self.flush()
    
    def flush(self):
        self._flush(self.buffer, self.dtype_path, self.PARTITION_COLS[self.dtype])


class TradeParquet(ParquetCallback, BackendCallback):
    default_dtype = TRADES

class FundingParquet(ParquetCallback, BackendCallback):
    default_dtype = FUNDING

class BookParquet(ParquetCallback, BackendBookCallback):
    default_dtype = L2_BOOK

    def __init__(self, *args, snapshots_only=False, snapshot_interval=1000, extract_ticker=False, **kwargs):
        self.snapshots_only = snapshots_only
        self.snapshot_interval = snapshot_interval
        self.snapshot_count = defaultdict(int)
        self.extract_ticker = extract_ticker
        super().__init__(*args, **kwargs)


class TickerParquet(ParquetCallback, BackendCallback):
    default_dtype = TICKER

class CandlesParquet(ParquetCallback, BackendCallback):
    default_dtype = CANDLES

class OpenInterestParquet(ParquetCallback, BackendCallback):
    default_dtype = OPEN_INTEREST

class LiquidationsParquet(ParquetCallback, BackendCallback):
    default_dtype = LIQUIDATIONS
