import os
import time
import logging
import ccxt
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


def run_live_feed() -> None:
    """
    Main entry point for the live market data feed.

    Monitors multiple symbol/timeframe combinations concurrently in a
    polling loop. For each task, the most recently closed candle is
    extracted, serialised, and dispatched to the Julia engine via ZMQ.
    Any non-HOLD signal returned by the engine is routed through the
    order management system.

    Set DRY_RUN = True to intercept all orders and log them without
    submitting to the exchange. Set DRY_RUN = False only when ready
    for live trading with real capital.
    """
    load_dotenv()

    # Define all symbol/timeframe pairs to monitor simultaneously.
    feed_tasks = [
        {"symbol": "ETH/USD", "julia_ticker": "ETH_USDT", "timeframe": "1h"},
        {"symbol": "BTC/USD", "julia_ticker": "BTC_USDT", "timeframe": "1m"},
    ]

    # Initialise per-task timestamp locks to prevent duplicate candle dispatch.
    # Each entry tracks the millisecond timestamp of the last candle sent for
    # a given symbol/timeframe combination.
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
    executor = ExchangeExecution(auth_exchange)

    logger.info("Live feed initialised. Connecting to Julia engine...")

    with MarketDataClient(endpoint="tcp://127.0.0.1:5555") as zmq_client:
        try:
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

                        # Use the second-to-last candle, which is guaranteed to be
                        # fully closed and will not be revised by the exchange.
                        latest_closed_row  = clean_df.iloc[-2]
                        current_candle_ts  = int(latest_closed_row.name.timestamp() * 1000)

                        # Only dispatch if this candle has not already been sent.
                        if current_candle_ts > last_sent_timestamps[task_id]:

                            # Construct the payload in the nested-assets format
                            # expected by the Julia backtester routing layer.
                            # The 'resolution' field is used by Julia to route
                            # the data slice to the correct alpha handler.
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

                                # --------------------------------------------------
                                # DRY RUN SWITCH
                                # Set to True  : orders are logged but not submitted.
                                # Set to False : orders are sent to the live exchange.
                                # Review carefully before disabling dry-run mode.
                                # --------------------------------------------------
                                DRY_RUN = True

                                if DRY_RUN:
                                    logger.info(
                                        f"[DRY RUN] Order intercepted: "
                                        f"action={signal.get('action')} "
                                        f"ticker={julia_ticker} "
                                        f"price={signal['price_at_signal']}. "
                                        f"No order submitted to exchange."
                                    )
                                else:
                                    # Submit the order to Kraken using real capital.
                                    executor.execute_signal(signal)

                            # Advance the timestamp lock for this task.
                            last_sent_timestamps[task_id] = current_candle_ts

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