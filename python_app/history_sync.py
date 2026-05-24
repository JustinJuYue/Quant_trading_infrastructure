import os
import time
import logging
import pandas as pd
from datetime import datetime, timezone
from data_pipeline import CryptoDataFetcher

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("HistorySync")


def _timeframe_to_ms(timeframe: str) -> int:
    """
    Convert a timeframe string to its equivalent duration in milliseconds.
    Raises ValueError if the timeframe is not supported.
    """
    mapping = {
        "1m":  60_000,
        "3m":  180_000,
        "5m":  300_000,
        "15m": 900_000,
        "30m": 1_800_000,
        "1h":  3_600_000,
        "2h":  7_200_000,
        "4h":  14_400_000,
        "6h":  21_600_000,
        "8h":  28_800_000,
        "12h": 43_200_000,
        "1d":  86_400_000,
    }
    if timeframe not in mapping:
        raise ValueError(f"Unsupported timeframe: {timeframe}")
    return mapping[timeframe]


def _ensure_utc_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure the DataFrame carries a UTC-aware DatetimeIndex named 'timestamp'.
    Handles both a 'timestamp' column and a pre-existing numeric or datetime index.
    """
    df = df.copy()
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
        df = df.dropna(subset=['timestamp']).set_index('timestamp')
    else:
        # Attempt millisecond-epoch parse first; fall back to generic parse if needed.
        idx_ms = pd.to_datetime(df.index, unit='ms', utc=True, errors='coerce')
        if idx_ms.isna().mean() > 0.5:
            idx_ms = pd.to_datetime(df.index, utc=True, errors='coerce')
        df.index = idx_ms
        df = df[~df.index.isna()]
    df.index.name = 'timestamp'
    return df


def _load_existing(file_path: str) -> pd.DataFrame | None:
    """
    Load an existing parquet file and return a DataFrame with a UTC DatetimeIndex.
    Returns None if the file does not exist.
    Only the core OHLCV columns are retained to prevent schema conflicts on merge.
    """
    if not os.path.exists(file_path):
        return None

    df = pd.read_parquet(file_path)
    df = _ensure_utc_datetime_index(df)

    expected = ['open', 'high', 'low', 'close', 'volume']

    # Retain only base OHLCV columns; derived columns (e.g. volume_spike)
    # are excluded to avoid NaN propagation during concatenation.
    available_cols = [c for c in expected if c in df.columns]
    df = df[available_cols]

    missing = [c for c in expected if c not in df.columns]
    if missing:
        logger.warning(f"Existing file is missing columns {missing}; available: {df.columns.tolist()}")

    for c in expected:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')

    return df


def _flush_buffer_to_disk(
    new_data_buffer: list,
    file_path: str,
    fetcher: CryptoDataFetcher,
    timeframe_freq: str = "1min",
) -> None:
    """
    Merge the in-memory candle buffer with any existing on-disk data,
    deduplicate, clean, validate, and persist to parquet.

    The timestamp column is always stored as a regular column (not the index)
    to maintain a stable parquet schema across incremental writes.
    """
    if not new_data_buffer:
        return

    logger.info(f"Flushing {len(new_data_buffer)} candles to disk...")

    # Build a DataFrame from the raw OHLCV buffer.
    new_df = pd.DataFrame(
        new_data_buffer,
        columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
    )
    new_df['timestamp'] = pd.to_datetime(new_df['timestamp'], unit='ms', utc=True, errors='coerce')
    new_df = new_df.dropna(subset=['timestamp']).set_index('timestamp')
    new_df.index.name = 'timestamp'

    for c in ['open', 'high', 'low', 'close', 'volume']:
        new_df[c] = pd.to_numeric(new_df[c], errors='coerce')

    # Merge with existing on-disk data if present.
    existing_df = _load_existing(file_path)
    if existing_df is not None:
        full_df = pd.concat([existing_df, new_df], axis=0)
    else:
        full_df = new_df

    # Enforce a consistent UTC DatetimeIndex after concatenation.
    full_df = _ensure_utc_datetime_index(full_df)

    # Remove duplicates, keeping the most recent entry for each timestamp.
    full_df = full_df[~full_df.index.duplicated(keep='last')]
    full_df.sort_index(inplace=True)

    # Apply exchange-level cleaning and validation.
    clean_df = fetcher.clean_and_validate(full_df, timeframe_freq=timeframe_freq)

    # Normalize the output schema: ensure 'timestamp' is a plain column before saving.
    if isinstance(clean_df.index, pd.DatetimeIndex):
        clean_df.index = pd.to_datetime(clean_df.index, utc=True, errors='coerce')
        clean_df.index.name = 'timestamp'
        clean_df = clean_df.reset_index()
    else:
        if 'timestamp' not in clean_df.columns and 'index' in clean_df.columns:
            clean_df = clean_df.rename(columns={'index': 'timestamp'})
        if 'timestamp' not in clean_df.columns:
            raise ValueError(
                f"Cleaned DataFrame has no 'timestamp' field. "
                f"Columns present: {clean_df.columns.tolist()}"
            )
        clean_df['timestamp'] = pd.to_datetime(clean_df['timestamp'], utc=True, errors='coerce')

    clean_df = clean_df.dropna(subset=['timestamp'])

    # Persist with a stable schema: timestamp stored as a column, not the index.
    clean_df.to_parquet(file_path, engine='pyarrow', compression='snappy', index=False)

    logger.info(f"Chunk saved. Database currently holds {len(clean_df):,} candles.")


def sync_historical_data(
    symbol: str = "BTC/USDT",
    timeframe: str = "1m",
    start_date: datetime = datetime(2017, 8, 17, 0, 0, 0, tzinfo=timezone.utc),
) -> None:
    """
    Synchronise historical OHLCV data for a given symbol and timeframe.

    Behaviour:
    - If no local data exists, a full fetch is initiated from start_date.
    - If local data exists but does not reach back to start_date, a
      backfill is performed from start_date to fill the historical gap.
    - If local data already covers start_date, only forward (incremental)
      updates are fetched from the latest stored candle onward.

    Data is buffered in memory and flushed to disk in chunks to limit
    peak memory consumption. Progress is preserved on KeyboardInterrupt,
    allowing safe resumption on the next run.
    """
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_script_dir)
    data_dir = os.path.join(project_root, "data")
    os.makedirs(data_dir, exist_ok=True)

    file_name = f"{symbol.replace('/', '_')}_{timeframe}.parquet"
    file_path = os.path.join(data_dir, file_name)

    fetcher = CryptoDataFetcher(exchange_id='binance')
    tf_ms = _timeframe_to_ms(timeframe)
    target_since_ms = int(start_date.timestamp() * 1000)

    # Determine the fetch start position using bidirectional coverage detection.
    existing_df = _load_existing(file_path)
    if existing_df is not None and not existing_df.empty:
        min_ts    = existing_df.index.min()
        max_ts    = existing_df.index.max()
        min_ts_ms = int(min_ts.timestamp() * 1000)

        if min_ts_ms > target_since_ms:
            # Local data does not reach back to the target start date; backfill required.
            since = target_since_ms
            logger.info(
                f"Local data starts at {min_ts}. "
                f"Target start date is {start_date.date()}. "
                f"Initiating historical backfill from {start_date.date()}."
            )
        else:
            # Local history is sufficient; resume forward from the latest candle.
            since = int(max_ts.timestamp() * 1000) + tf_ms
            logger.info(
                f"Local data covers {min_ts.date()} to {max_ts.date()}. "
                f"Resuming forward sync from {max_ts} (next candle)."
            )
        del existing_df
    else:
        since = target_since_ms
        logger.info(f"No local data found. Initiating full fetch from {start_date.date()}.")

    memory_buffer = []
    CHUNK_SIZE    = 40_000

    while True:
        human_time = datetime.fromtimestamp(since / 1000, tz=timezone.utc)
        logger.info(f"Fetching chunk starting at: {human_time}")

        try:
            chunk = fetcher.exchange.fetch_ohlcv(
                symbol=symbol,
                timeframe=timeframe,
                since=since,
                limit=1000
            )

            if not chunk:
                logger.info("Exchange returned no data. Sync complete.")
                break

            # Discard any candles that fall before the current cursor position.
            chunk = [c for c in chunk if int(c[0]) >= since]
            if not chunk:
                logger.info("All returned candles were already covered. Sync complete.")
                break

            memory_buffer.extend(chunk)

            # Advance the cursor to the candle immediately following the last received.
            last_ts    = int(chunk[-1][0])
            next_since = last_ts + tf_ms

            # Safety guard: ensure the cursor always moves forward.
            if next_since <= since:
                logger.warning(
                    "Cursor did not advance. Forcing increment by one timeframe interval."
                )
                next_since = since + tf_ms

            since = next_since

            # Flush to disk once the buffer reaches the configured chunk size.
            if len(memory_buffer) >= CHUNK_SIZE:
                _flush_buffer_to_disk(
                    new_data_buffer=memory_buffer,
                    file_path=file_path,
                    fetcher=fetcher,
                    timeframe_freq='1min' if timeframe == '1m' else timeframe
                )
                memory_buffer = []

            sleep_time = (fetcher.exchange.rateLimit / 1000.0) * 1.5
            time.sleep(max(sleep_time, 0.5))

        except KeyboardInterrupt:
            logger.warning("Interrupted by user. Flushing remaining buffer before exit...")
            if memory_buffer:
                _flush_buffer_to_disk(
                    new_data_buffer=memory_buffer,
                    file_path=file_path,
                    fetcher=fetcher,
                    timeframe_freq='1min' if timeframe == '1m' else timeframe
                )
            logger.info("Progress saved. The sync can be safely resumed on the next run.")
            return

        except Exception as e:
            logger.exception(f"Unexpected error in sync loop: {e}. Retrying in 5 seconds...")
            time.sleep(5)

    # Final flush for any candles remaining in the buffer after the loop exits.
    if memory_buffer:
        _flush_buffer_to_disk(
            new_data_buffer=memory_buffer,
            file_path=file_path,
            fetcher=fetcher,
            timeframe_freq='1min' if timeframe == '1m' else timeframe
        )

    logger.info(f"Historical data for {symbol} ({timeframe}) is fully synchronised.")


if __name__ == "__main__":
    # Each task specifies a symbol, timeframe, and the earliest desired start date.
    # The 1-minute timeframes begin from 2020-01-01 to limit data volume.
    # The 1-hour timeframes begin from the Binance launch date (2017-08-17).
    sync_tasks = [
        ("BTC/USDT", "1h", datetime(2017, 8, 17, tzinfo=timezone.utc)),
        ("BTC/USDT", "1m", datetime(2020, 1,  1,  tzinfo=timezone.utc)),
        ("ETH/USDT", "1h", datetime(2017, 8, 17, tzinfo=timezone.utc)),
        ("ETH/USDT", "1m", datetime(2020, 1,  1,  tzinfo=timezone.utc)),
    ]

    for symbol, timeframe, start_date in sync_tasks:
        logger.info("=" * 50)
        logger.info(f"Starting task: {symbol} {timeframe} from {start_date.date()}")
        logger.info("=" * 50)
        try:
            sync_historical_data(symbol, timeframe, start_date=start_date)
        except Exception as e:
            logger.error(f"Task failed [{symbol} {timeframe}]: {e}")

    logger.info("All requested data sets have been successfully synchronised.")