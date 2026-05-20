import time
import logging
from data_pipeline import CryptoDataFetcher
from client import MarketDataClient  # Imported from our previous module
import numpy as np

logger = logging.getLogger("LiveIntegration")

def serialize_row_to_dict(row, symbol: str) -> dict:
    """
    CRITICAL: MsgPack cannot serialize Pandas/Numpy data types directly.
    We must explicitly cast np.float64 and pd.Timestamp to native Python types.
    """
    return {
        "timestamp": int(row.name.timestamp() * 1000), # Unix ms
        "ticker": symbol,
        "open": float(row['open']),
        "high": float(row['high']),
        "low": float(row['low']),
        "close": float(row['close']),
        "volume": float(row['volume']),
        "volume_spike": int(row['volume_spike'])
    }

def run_live_feed():
    symbol = "BTC/USDT"
    fetcher = CryptoDataFetcher(exchange_id='binance')
    
    logger.info("Initializing Live Feed. Connecting to Julia Engine...")
    
    with MarketDataClient(endpoint="tcp://127.0.0.1:5555") as zmq_client:
        try:
            while True:
                # 1. Fetch latest data (e.g., last 50 candles for rolling metrics)
                raw_df = fetcher.fetch_ohlcv(symbol, timeframe='1m', limit=50)
                
                # 2. Clean and validate
                clean_df = fetcher.clean_and_validate(raw_df, timeframe_freq='1min')
                
                # Optional: Periodically save the batch to Parquet
                # fetcher.save_to_parquet(clean_df, symbol)
                
                # 3. Extract the most recent closed/current candle
                latest_row = clean_df.iloc[-1]
                
                # 4. Cast to native types for IPC safety
                payload = serialize_row_to_dict(latest_row, symbol)
                
                # 5. Dispatch over ZMQ
                logger.info(f"Dispatching latest data -> Price: {payload['close']}")
                signal = zmq_client.send_market_slice(payload)
                
                if signal:
                    logger.info(f"Received Execution Signal: Action={signal.get('action')}, Weight={signal.get('weight')}")
                
                # Wait before pulling the next candle (polling architecture)
                time.sleep(10)
                
        except KeyboardInterrupt:
            logger.info("Live feed stopped by user.")
        except Exception as e:
            logger.error(f"Live feed encountered a critical error: {e}", exc_info=True)

if __name__ == "__main__":
    run_live_feed()