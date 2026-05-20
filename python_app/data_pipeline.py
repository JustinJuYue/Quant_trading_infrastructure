import os
import time
import logging
import ccxt
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

# Configure industrial-grade logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("DataPipeline")

class CryptoDataFetcher:
    def __init__(self, exchange_id: str = 'binance'):
        """
        Initialize the exchange interface via CCXT with automatic rate limiting.
        """
        try:
            exchange_class = getattr(ccxt, exchange_id)
            self.exchange = exchange_class({
                'enableRateLimit': True,  # CRITICAL: Prevents IP bans from the exchange
                'timeout': 15000,
            })
            logger.info(f"Initialized DataFetcher for exchange: {exchange_id.upper()}")
        except AttributeError:
            logger.error(f"Exchange {exchange_id} is not supported by CCXT.")
            raise

    def fetch_ohlcv(self, symbol: str, timeframe: str = '1m', limit: int = 1000) -> pd.DataFrame:
        """
        Fetch OHLCV data with retry logic.
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                logger.debug(f"Fetching {symbol} {timeframe} data, attempt {attempt + 1}")
                data = self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
                
                df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                
                # Enforce UTC timezone and strict float64 typing
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
                df.set_index('timestamp', inplace=True)
                df = df.astype(np.float64)
                
                return df
                
            except (ccxt.NetworkError, ccxt.ExchangeError) as e:
                logger.warning(f"Network/Exchange error on attempt {attempt + 1}: {e}")
                time.sleep(2 ** attempt)  # Exponential backoff
        
        logger.error(f"Failed to fetch data for {symbol} after {max_retries} attempts.")
        raise ConnectionError(f"Cannot fetch data from {self.exchange.id}")

    def clean_and_validate(self, df: pd.DataFrame, timeframe_freq: str = '1min') -> pd.DataFrame:
        """
        Data Quality Assurance (QA) Engine.
        1. Checks for missing values (NaNs).
        2. Validates timestamp continuity.
        3. Detects and tags volume spikes.
        """
        df_clean = df.copy()

        # --- 1. Timestamp Continuity Check ---
        # Create an expected contiguous index
        full_idx = pd.date_range(start=df_clean.index.min(), end=df_clean.index.max(), freq=timeframe_freq)
        missing_candles = len(full_idx) - len(df_clean)
        
        if missing_candles > 0:
            logger.warning(f"Detected {missing_candles} missing candles. Reindexing and forward-filling.")
            # Reindex and forward fill prices; fill missing volumes with 0
            df_clean = df_clean.reindex(full_idx)
            df_clean[['open', 'high', 'low', 'close']] = df_clean[['open', 'high', 'low', 'close']].ffill()
            df_clean['volume'] = df_clean['volume'].fillna(0.0)

        # --- 2. NaN Check (Safeguard) ---
        if df_clean.isnull().values.any():
            logger.warning("NaNs detected after continuity fix. Dropping corrupted rows.")
            df_clean.dropna(inplace=True)

        # --- 3. Volume Spike Detection (Robust Median Approach) ---
        # We use a rolling median instead of mean to avoid skew from massive single spikes
        window_size = 20
        rolling_median_vol = df_clean['volume'].rolling(window=window_size, min_periods=1).median()
        
        # Define a spike as volume being 5x greater than the rolling median
        spike_condition = df_clean['volume'] > (rolling_median_vol * 5)
        df_clean['volume_spike'] = np.where(spike_condition, 1, 0)
        
        num_spikes = df_clean['volume_spike'].sum()
        if num_spikes > 0:
            logger.info(f"Tagged {num_spikes} volume anomalies.")

        return df_clean

    def save_to_parquet(self, df: pd.DataFrame, symbol: str, base_dir: str = "./data"):
        """
        Store dataframe efficiently using Parquet, partitioned by ticker and date.
        """
        os.makedirs(base_dir, exist_ok=True)
        
        # Prepare partitioning columns
        df['ticker'] = symbol.replace("/", "_")
        df['date'] = df.index.date.astype(str)
        
        # Reset index to save timestamp as a column
        df_out = df.reset_index()
        
        # Save partitioned parquet
        # Format will be: base_dir/ticker=BTC_USDT/date=2023-10-01/...parquet
        df_out.to_parquet(
            base_dir,
            engine='pyarrow',
            compression='snappy',
            partition_cols=['ticker', 'date'],
            existing_data_behavior='delete_matching' # Overwrites existing data for the same partition
        )
        logger.info(f"Successfully saved {len(df)} rows to Parquet at {base_dir}")