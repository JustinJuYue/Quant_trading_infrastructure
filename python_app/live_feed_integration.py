import os
import time
import logging
import ccxt
import pandas as pd
from dotenv import load_dotenv

from data_pipeline import CryptoDataFetcher
from client import MarketDataClient
from order_manager import ExchangeExecution

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("LiveIntegration")


def serialize_row_to_dict(row, symbol: str) -> dict:
    """
    Convert a pandas/numpy DataFrame row to native Python types.
    Required for safe serialisation via msgpack before ZMQ transmission.
    """
    return {
        "timestamp":    int(row.name.timestamp() * 1000),
        "ticker":       symbol,
        "open":         float(row["open"]),
        "high":         float(row["high"]),
        "low":          float(row["low"]),
        "close":        float(row["close"]),
        "volume":       float(row["volume"]),
        "volume_spike": int(row.get("volume_spike", 0)),
    }


def hydrate_julia_engine(zmq_client: MarketDataClient) -> bool:
    """
    Pre-warm Julia strategy states by sending historical candles
    from local Parquet files via ZMQ before live trading starts.
    
    Args:
        zmq_client (MarketDataClient): The active ZMQ client connection.
        
    Returns:
        bool: True if hydration succeeded, False if it failed or
              was skipped (system will run in Cold Start mode).
    """
    # Config: which assets and timeframes to hydrate
    HYDRATION_CONFIG = [
        {"alpha_id": "Alpha005_MacroReflexivity", "asset": "BTC_USDT", "timeframe": "1h", "rows": 50},
        {"alpha_id": "Alpha005_MacroReflexivity", "asset": "ETH_USDT", "timeframe": "1h", "rows": 50},
        {"alpha_id": "Alpha003_PriceKinematics", "asset": "BTC_USDT", "timeframe": "1m", "rows": 50},
    ]
    
    DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
    
    for config in HYDRATION_CONFIG:
        asset = config["asset"]
        tf = config["timeframe"]
        rows = config["rows"]
        
        file_path = os.path.join(DATA_DIR, f"{asset}_{tf}.parquet")
        
        if not os.path.exists(file_path):
            logger.warning(
                f"[HYDRATE] Missing {asset}_{tf}.parquet. "
                f"Skipping. Alpha will Cold Start."
            )
            continue
            
        try:
            # Read parquet with pandas — fast, no JIT compilation overhead
            df = pd.read_parquet(file_path, columns=["timestamp", "close", "volume_spike"])
            df = df.tail(rows)  # Last N rows, already sorted chronologically by history_sync
            
            logger.info(
                f"[HYDRATE] Sending {len(df)} bars of "
                f"{asset} {tf} to Julia..."
            )
            
            for _, row in df.iterrows():
                # Build the exact same payload format Julia expects
                ts = row["timestamp"]
                if hasattr(ts, "timestamp"):
                    ts_ms = ts.timestamp() * 1000
                else:
                    ts_ms = float(ts) * 1000
                    
                hydration_payload = {
                    "type": "HYDRATE",            # Special metadata message type
                    "ticker": asset,
                    "resolution": tf,
                    "timestamp": ts_ms,
                    "assets": {
                        asset: {
                            "close": float(row["close"]),
                            "volume_spike": int(row["volume_spike"])
                        }
                    }
                }
                
                # Send to Julia and wait for ACK
                response = zmq_client.send_market_slice(hydration_payload)
                
                # Julia usually replies with {"action": "HOLD"} for hydration bars
                if response and response.get("action") != "HOLD":
                    logger.debug(f"[HYDRATE] Signal triggered during warmup: {response}")
            
            logger.info(f"[HYDRATE] {asset} {tf} hydration complete.")
            
        except Exception as e:
            logger.warning(f"[HYDRATE] Failed to hydrate {asset} {tf}: {e}. Continuing to live loop.")
            continue
            
    logger.info("[HYDRATE] All requested assets hydrated. Starting live feed.")
    return True


def run_live_feed() -> None:
    """
    Main entry point for the live market data feed.

    Monitors multiple symbol/timeframe combinations concurrently in a
    polling loop. For each task, the most recently closed candle is
    extracted, serialised, and dispatched to the Julia engine via ZMQ.
    Any non-HOLD signal returned by the engine is routed through the
    order management system.
    """
    load_dotenv()

    # Define all symbol/timeframe pairs to monitor simultaneously.
    feed_tasks = [
        {"symbol": "ETH/USD", "julia_ticker": "ETH_USDT", "timeframe": "1h"},
        {"symbol": "BTC/USD", "julia_ticker": "BTC_USDT", "timeframe": "1m"},
    ]

    # Initialise per-task timestamp locks to prevent duplicate candle dispatch.
    last_sent_timestamps = {
        f"{t['julia_ticker']}_{t['timeframe']}": 0 for t in feed_tasks
    }

    fetcher = CryptoDataFetcher(exchange_id="kraken")

    api_key    = os.getenv("KRAKEN_API_KEY")
    secret_key = os.getenv("KRAKEN_SECRET_KEY")
    if not api_key or not secret_key:
        logger.error("API keys are missing from the .env file. Halting system.")
        return

    auth_exchange = ccxt.kraken({
        "apiKey":          api_key,
        "secret":          secret_key,
        "enableRateLimit": True,
        "options":         {"defaultType": "spot"},
    })
    
    # Initialize the global Order Execution engine (handles DRY_RUN internally)
    executor = ExchangeExecution(auth_exchange)

    logger.info("Live feed initialised. Connecting to Julia engine...")

    with MarketDataClient(endpoint="tcp://127.0.0.1:5555") as zmq_client:
        try:
            # 🚀 PRE-WARMUP: Hydrate the Julia State Machine via ZMQ
            hydrate_julia_engine(zmq_client)
            
            while True:
                # Iterate over every configured symbol/timeframe task in sequence.
                for task in feed_tasks:
                    symbol        = task["symbol"]
                    julia_ticker  = task["julia_ticker"]
                    tf            = task["timeframe"]
                    task_id       = f"{julia_ticker}_{tf}"

                    try:
                        # Fetch the latest candles and apply cleaning/validation.
                        raw_df   = fetcher.fetch_ohlcv(symbol, timeframe=tf, limit=5)
                        freq_str = "1min" if tf == "1m" else "1h"
                        clean_df = fetcher.clean_and_validate(raw_df, timeframe_freq=freq_str)

                        # Use the second-to-last candle, which is guaranteed to be fully closed.
                        latest_closed_row  = clean_df.iloc[-2]
                        current_candle_ts  = int(latest_closed_row.name.timestamp() * 1000)

                        # Only dispatch if this candle has not already been sent.
                        if current_candle_ts > last_sent_timestamps[task_id]:

                            payload = {
                                "ticker":           julia_ticker,
                                "resolution":       tf,
                                "timestamp":        current_candle_ts,
                                "price_at_signal":  float(latest_closed_row["close"]),
                                "assets": {
                                    julia_ticker: serialize_row_to_dict(
                                        latest_closed_row, julia_ticker
                                    )
                                },
                            }

                            logger.info(
                                f"[{tf}] Candle closed. Dispatching {julia_ticker} "
                                f"| Close: {payload['price_at_signal']}"
                            )

                            # Transmit the market slice to the Julia engine.
                            signal = zmq_client.send_market_slice(payload)

                            # Route any actionable signal through the OMS.
                            if signal and signal.get("action") != "HOLD":
                                logger.warning(
                                    f"Signal received: alpha={signal.get('alpha_id')} "
                                    f"action={signal.get('action')} ticker={julia_ticker}"
                                )
                                signal["price_at_signal"] = payload["price_at_signal"]

                                # The ExchangeExecution (executor) intrinsically respects the 
                                # DRY_RUN environment variable. Route all actionable signals through it.
                                executor.execute_signal(signal)

                            # Advance the timestamp lock for this task.
                            last_sent_timestamps[task_id] = current_candle_ts

                            # --- Portfolio Summary Logging Hook ---
                            if tf == "1h":
                                if hasattr(executor, 'log_portfolio_summary'):
                                    executor.log_portfolio_summary()

                    except Exception as loop_e:
                        logger.error(f"Error processing task [{task_id}]: {loop_e}")

                    # Throttle requests to respect exchange API rate limits.
                    time.sleep(1)

                # Pause between full scan cycles before beginning the next poll.
                time.sleep(10)

        except KeyboardInterrupt:
            logger.info("Live feed stopped by user.")
        except Exception as e:
            logger.error(f"Critical error in live feed: {e}", exc_info=True)


if __name__ == "__main__":
    run_live_feed()