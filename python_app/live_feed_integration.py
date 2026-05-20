import os
import time
import logging
import ccxt
from dotenv import load_dotenv

from data_pipeline import CryptoDataFetcher
from client import MarketDataClient
from order_manager import ExchangeExecution  # 确保 order_manager.py 里有这个类

# logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("LiveIntegration")

def serialize_row_to_dict(row, symbol: str) -> dict:
    """
    Convert pandas/numpy row to native Python types for msgpack safety.
    """
    return {
        "timestamp": int(row.name.timestamp() * 1000),
        "ticker": symbol,
        "open": float(row["open"]),
        "high": float(row["high"]),
        "low": float(row["low"]),
        "close": float(row["close"]),
        "volume": float(row["volume"]),
        "volume_spike": int(row.get("volume_spike", 0)),
    }

def run_live_feed():
    load_dotenv()  # 读取 .env

    symbol = "BTC/USDT"

    # 1) Public data fetcher
    fetcher = CryptoDataFetcher(exchange_id="binance")

    # 2) Authenticated exchange for execution
    api_key = os.getenv("KRAKEN_API_KEY")
    secret_key = os.getenv("KRAKEN_SECRET_KEY")

    if not api_key or not secret_key:
        logger.error("API Keys missing in .env! Halting system.")
        return

    auth_exchange = ccxt.kraken({
        "apiKey": api_key,
        "secret": secret_key,
        "enableRateLimit": True,
        "options": {"defaultType": "spot"},
    })

    # 3) OMS
    executor = ExchangeExecution(auth_exchange)

    logger.info("Live Feed + OMS initialized. Connecting to Julia Engine...")

    with MarketDataClient(endpoint="tcp://127.0.0.1:5555") as zmq_client:
        try:
            while True:
                raw_df = fetcher.fetch_ohlcv(symbol, timeframe="1m", limit=50)
                clean_df = fetcher.clean_and_validate(raw_df, timeframe_freq="1min")
                latest_row = clean_df.iloc[-1]

                payload = serialize_row_to_dict(latest_row, symbol)
                payload["price_at_signal"] = payload["close"]

                logger.info(f"Dispatching latest data -> Price: {payload['close']}")
                signal = zmq_client.send_market_slice(payload)

                if signal:
                    logger.info(
                        f"Received Signal: Action={signal.get('action')}, Weight={signal.get('weight')}"
                    )

                    # 把定价字段补进 signal，给 OMS sizing 用
                    signal["price_at_signal"] = payload["close"]

                    if signal.get("action") != "HOLD":
                        logger.warning(f"ALPHA TRIGGERED: Routing {signal.get('action')} to OMS")
                        executor.execute_signal(signal)

                time.sleep(10)

        except KeyboardInterrupt:
            logger.info("Live feed stopped by user.")
        except Exception as e:
            logger.error(f"Critical error in live feed: {e}", exc_info=True)

if __name__ == "__main__":
    run_live_feed()