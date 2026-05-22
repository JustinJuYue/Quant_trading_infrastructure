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
    """Guarantee DataFrame has UTC DatetimeIndex named 'timestamp'."""
    df = df.copy()
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
        df = df.dropna(subset=['timestamp']).set_index('timestamp')
    else:
        idx_ms = pd.to_datetime(df.index, unit='ms', utc=True, errors='coerce')
        if idx_ms.isna().mean() > 0.5:
            idx_ms = pd.to_datetime(df.index, utc=True, errors='coerce')
        df.index = idx_ms
        df = df[~df.index.isna()]
    df.index.name = 'timestamp'
    return df


def _load_existing(file_path: str) -> pd.DataFrame | None:
    """Safely load parquet and return DataFrame with UTC DatetimeIndex."""
    if not os.path.exists(file_path):
        return None

    df = pd.read_parquet(file_path)
    df = _ensure_utc_datetime_index(df)

    expected = ['open', 'high', 'low', 'close', 'volume']
    available_cols = [c for c in expected if c in df.columns]
    df = df[available_cols]

    missing = [c for c in expected if c not in df.columns]
    if missing:
        logger.warning(f"Existing file missing columns {missing}; available={df.columns.tolist()}")

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

    logger.info(f"💾 Flushing {len(new_data_buffer)} candles to disk...")

    new_df = pd.DataFrame(
        new_data_buffer,
        columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
    )
    new_df['timestamp'] = pd.to_datetime(new_df['timestamp'], unit='ms', utc=True, errors='coerce')
    new_df = new_df.dropna(subset=['timestamp']).set_index('timestamp')
    new_df.index.name = 'timestamp'

    for c in ['open', 'high', 'low', 'close', 'volume']:
        new_df[c] = pd.to_numeric(new_df[c], errors='coerce')

    existing_df = _load_existing(file_path)
    if existing_df is not None:
        full_df = pd.concat([existing_df, new_df], axis=0)
    else:
        full_df = new_df

    full_df = _ensure_utc_datetime_index(full_df)
    full_df = full_df[~full_df.index.duplicated(keep='last')]
    full_df.sort_index(inplace=True)

    clean_df = fetcher.clean_and_validate(full_df, timeframe_freq=timeframe_freq)

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
    clean_df.to_parquet(file_path, engine='pyarrow', compression='snappy', index=False)

    logger.info(f"✅ Chunk saved! Database currently holds {len(clean_df)} candles.")


# ============================================================
# ✅ 核心修复：支持 start_date 参数 + 双向检测（补历史 & 续新）
# ============================================================
def sync_historical_data(
    symbol: str = "BTC/USDT",
    timeframe: str = "1m",
    start_date: datetime = datetime(2017, 8, 17, 0, 0, 0, tzinfo=timezone.utc),  # ✅ 新增
):
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_script_dir)
    data_dir = os.path.join(project_root, "data")
    os.makedirs(data_dir, exist_ok=True)

    file_name = f"{symbol.replace('/', '_')}_{timeframe}.parquet"
    file_path = os.path.join(data_dir, file_name)

    fetcher = CryptoDataFetcher(exchange_id='binance')
    tf_ms = _timeframe_to_ms(timeframe)

    # ✅ 双向检测：判断本地数据最早时间是否早于目标 start_date
    target_since_ms = int(start_date.timestamp() * 1000)

    existing_df = _load_existing(file_path)
    if existing_df is not None and not existing_df.empty:
        min_ts = existing_df.index.min()  # ✅ 检查最早时间
        max_ts = existing_df.index.max()
        min_ts_ms = int(min_ts.timestamp() * 1000)

        if min_ts_ms > target_since_ms:
            # ✅ 本地数据不够早，需要往前补历史
            since = target_since_ms
            logger.info(
                f"📂 Local data starts at {min_ts} — earlier than target {start_date.date()}. "
                f"⏪ Backfilling history from {start_date.date()}."
            )
        else:
            # ✅ 本地历史已足够，只需往后续传
            since = int(max_ts.timestamp() * 1000) + tf_ms
            logger.info(
                f"📂 Local data covers {min_ts.date()} → {max_ts.date()}. "
                f"▶️  Resuming forward from {max_ts} (next candle)."
            )
        del existing_df
    else:
        since = target_since_ms
        logger.info(f"🆕 No local data. Initiating full fetch from {start_date.date()}")

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

            chunk = [c for c in chunk if int(c[0]) >= since]
            if not chunk:
                logger.info("Chunk contained no new candles. Sync complete.")
                break

            memory_buffer.extend(chunk)

            last_ts = int(chunk[-1][0])
            next_since = last_ts + tf_ms

            if next_since <= since:
                logger.warning("Non-progressing cursor detected; forcing increment.")
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

    logger.info(f"🎉 All historical data for {symbol} ({timeframe}) is fully synced!")


if __name__ == "__main__":
    # ✅ 每个任务独立配置起始时间
    sync_tasks = [
        ("BTC/USDT", "1h", datetime(2017, 8, 17, tzinfo=timezone.utc)),  # Binance 上线日
        ("BTC/USDT", "1m", datetime(2020, 1,  1,  tzinfo=timezone.utc)),  # 1m 数据量大，从2020起
        ("ETH/USDT", "1h", datetime(2017, 8, 17, tzinfo=timezone.utc)),  # Binance 上线日
        ("ETH/USDT", "1m", datetime(2020, 1,  1,  tzinfo=timezone.utc)),  # 1m 数据量大，从2020起
    ]

    for symbol, timeframe, start_date in sync_tasks:
        logger.info("\n" + "=" * 50)
        logger.info(f"🚀 STARTING TASK: {symbol} {timeframe} from {start_date.date()}")
        logger.info("=" * 50)
        try:
            sync_historical_data(symbol, timeframe, start_date=start_date)
        except Exception as e:
            logger.error(f"❌ Task {symbol} {timeframe} failed: {e}")

    logger.info("\n🎉🎉 All requested data sets have been successfully synced! 🎉🎉")