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
    mapping = {
        "1m": 60_000,
        "3m": 180_000,
        "5m": 300_000,
        "15m": 900_000,
        "30m": 1_800_000,
        "1h": 3_600_000,
        "2h": 7_200_000,
        "4h": 14_400_000,
        "6h": 21_600_000,
        "8h": 28_800_000,
        "12h": 43_200_000,
        "1d": 86_400_000,
    }
    if timeframe not in mapping:
        raise ValueError(f"Unsupported timeframe: {timeframe}")
    return mapping[timeframe]


def _ensure_utc_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Guarantee DataFrame has UTC DatetimeIndex named 'timestamp'.
    Handles both timestamp column and pre-existing index forms.
    """
    df = df.copy()

    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
        df = df.dropna(subset=['timestamp']).set_index('timestamp')
    else:
        # Try ms epoch first; fallback generic parse if needed
        idx_ms = pd.to_datetime(df.index, unit='ms', utc=True, errors='coerce')
        if idx_ms.isna().mean() > 0.5:
            idx_ms = pd.to_datetime(df.index, utc=True, errors='coerce')
        df.index = idx_ms
        df = df[~df.index.isna()]

    df.index.name = 'timestamp'
    return df


def _load_existing(file_path: str) -> pd.DataFrame | None:
    """
    Safely load parquet and return DataFrame with UTC DatetimeIndex.
    """
    if not os.path.exists(file_path):
        return None

    df = pd.read_parquet(file_path)
    df = _ensure_utc_datetime_index(df)

    expected = ['open', 'high', 'low', 'close', 'volume']
    
    # 🔴 关键修复：只保留基础数据列，剔除掉 volume_spike 等计算列，防止 Concat 时产生 NaN
    available_cols = [c for c in expected if c in df.columns]
    df = df[available_cols]

    missing = [c for c in expected if c not in df.columns]
    if missing:
        logger.warning(f"Existing file missing columns {missing}; available={df.columns.tolist()}")

    # Coerce numeric columns if available
    for c in expected:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce')

    return df


def _flush_buffer_to_disk(
    new_data_buffer: list,
    file_path: str,
    fetcher: CryptoDataFetcher,
    timeframe_freq: str = "1min",
):
    if not new_data_buffer:
        return

    logger.info(f"💾 Flushing {len(new_data_buffer)} candles to disk to free memory...")

    # Build new chunk DataFrame
    new_df = pd.DataFrame(
        new_data_buffer,
        columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
    )
    new_df['timestamp'] = pd.to_datetime(new_df['timestamp'], unit='ms', utc=True, errors='coerce')
    new_df = new_df.dropna(subset=['timestamp']).set_index('timestamp')
    new_df.index.name = 'timestamp'

    # Coerce numeric
    for c in ['open', 'high', 'low', 'close', 'volume']:
        new_df[c] = pd.to_numeric(new_df[c], errors='coerce')

    # Merge existing + new
    existing_df = _load_existing(file_path)
    if existing_df is not None:
        full_df = pd.concat([existing_df, new_df], axis=0)
    else:
        full_df = new_df

    # Enforce index consistency post-concat
    full_df = _ensure_utc_datetime_index(full_df)

    # Deduplicate + sort
    full_df = full_df[~full_df.index.duplicated(keep='last')]
    full_df.sort_index(inplace=True)

    # Clean/validate
    clean_df = fetcher.clean_and_validate(full_df, timeframe_freq=timeframe_freq)

    # Normalize output schema robustly before save
    if isinstance(clean_df.index, pd.DatetimeIndex):
        clean_df.index = pd.to_datetime(clean_df.index, utc=True, errors='coerce')
        clean_df.index.name = 'timestamp'
        clean_df = clean_df.reset_index()
    else:
        if 'timestamp' not in clean_df.columns and 'index' in clean_df.columns:
            clean_df = clean_df.rename(columns={'index': 'timestamp'})
        if 'timestamp' not in clean_df.columns:
            raise ValueError(
                f"clean_df has no 'timestamp' field. columns={clean_df.columns.tolist()}"
            )
        clean_df['timestamp'] = pd.to_datetime(clean_df['timestamp'], utc=True, errors='coerce')

    clean_df = clean_df.dropna(subset=['timestamp'])

    # Save stable parquet schema (timestamp column persisted)
    clean_df.to_parquet(
        file_path,
        engine='pyarrow',
        compression='snappy',
        index=False
    )

    logger.info(f"✅ Chunk saved! Database currently holds {len(clean_df)} candles.")


def sync_historical_data(symbol: str = "BTC/USDT", timeframe: str = "1m"):
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_script_dir)
    data_dir = os.path.join(project_root, "data")
    os.makedirs(data_dir, exist_ok=True)

    file_name = f"{symbol.replace('/', '_')}_{timeframe}.parquet"
    file_path = os.path.join(data_dir, file_name)

    fetcher = CryptoDataFetcher(exchange_id='binance')
    tf_ms = _timeframe_to_ms(timeframe)

    # Start position
    existing_df = _load_existing(file_path)
    if existing_df is not None and not existing_df.empty:
        max_ts = existing_df.index.max()
        since = int(max_ts.timestamp() * 1000) + tf_ms  # start at next candle
        logger.info(f"Local database found. Resuming fetch from {max_ts} (next candle).")
        del existing_df
    else:
        dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        since = int(dt.timestamp() * 1000)
        logger.info("No local data. Initiating full fetch from 2024-01-01")

    memory_buffer = []
    CHUNK_SIZE = 40_000

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
                logger.info("No more data returned. Sync complete.")
                break

            # Keep strictly newer candles
            chunk = [c for c in chunk if int(c[0]) >= since]
            if not chunk:
                logger.info("Chunk contained no new candles. Sync complete.")
                break

            memory_buffer.extend(chunk)

            # Move cursor to next candle after last fetched
            last_ts = int(chunk[-1][0])
            next_since = last_ts + tf_ms

            # Safety: ensure forward progress
            if next_since <= since:
                logger.warning("Non-progressing cursor detected; forcing increment by timeframe.")
                next_since = since + tf_ms

            since = next_since

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
            logger.warning("⚠️ Interrupted by user. Flushing remaining buffer...")
            if memory_buffer:
                _flush_buffer_to_disk(
                    new_data_buffer=memory_buffer,
                    file_path=file_path,
                    fetcher=fetcher,
                    timeframe_freq='1min' if timeframe == '1m' else timeframe
                )
            logger.info("💾 Progress saved. You can safely resume later.")
            return

        except Exception as e:
            logger.exception(f"Sync loop error: {e}. Retrying in 5 seconds...")
            time.sleep(5)

    # Final flush
    if memory_buffer:
        _flush_buffer_to_disk(
            new_data_buffer=memory_buffer,
            file_path=file_path,
            fetcher=fetcher,
            timeframe_freq='1min' if timeframe == '1m' else timeframe
        )

    logger.info(f"🎉 All historical data for {symbol} is fully synced!")


if __name__ == "__main__":
    # 定义需要下载的任务列表 (交易对, 时间级别)
    sync_tasks = [
        ("BTC/USDT", "1h"),  # BTC 1小时线 (用于宏观状态与低频统计建模)
        ("ETH/USDT", "1h"),  # ETH 1小时线 (用于和 BTC 进行低频协整性分析)
        ("ETH/USDT", "1m")   # ETH 1分钟线 (用于配对高频微观结构测试)
    ]

    for symbol, timeframe in sync_tasks:
        logger.info(f"\n" + "="*50)
        logger.info(f"🚀 STARTING TASK: Syncing {symbol} at {timeframe}")
        logger.info("="*50)
        
        try:
            sync_historical_data(symbol, timeframe)
        except Exception as e:
            logger.error(f"❌ Task {symbol} {timeframe} failed: {e}")
            
    logger.info("\n🎉🎉 All requested data sets have been successfully synced! 🎉🎉")