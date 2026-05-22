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
    load_dotenv()

    # 1. 定义你需要同时监控的所有任务（多标的、多频段）
    feed_tasks = [
        {"symbol": "ETH/USD", "julia_ticker": "ETH_USDT", "timeframe": "1h"},
        {"symbol": "BTC/USD", "julia_ticker": "BTC_USDT", "timeframe": "1m"}
    ]

    # 初始化状态锁：记录每个任务最后发送的 K 线时间戳，防重放污染
    last_sent_timestamps = {f"{t['julia_ticker']}_{t['timeframe']}": 0 for t in feed_tasks}

    fetcher = CryptoDataFetcher(exchange_id="kraken")
    
    api_key = os.getenv("KRAKEN_API_KEY")
    secret_key = os.getenv("KRAKEN_SECRET_KEY")
    if not api_key or not secret_key:
        logger.error("API Keys missing in .env! Halting system.")
        return

    auth_exchange = ccxt.kraken({
        "apiKey": api_key, 
        "secret": secret_key, 
        "enableRateLimit": True, 
        "options": {"defaultType": "spot"}
    })
    executor = ExchangeExecution(auth_exchange)

    logger.info("Live Feed Multi-Tasker initialized. Connecting to Julia Engine...")

    with MarketDataClient(endpoint="tcp://127.0.0.1:5555") as zmq_client:
        try:
            while True:
                # 遍历所有配置的频段任务
                for task in feed_tasks:
                    symbol = task["symbol"]
                    julia_ticker = task["julia_ticker"]
                    tf = task["timeframe"]
                    task_id = f"{julia_ticker}_{tf}"

                    try:
                        # 拉取该频段数据
                        raw_df = fetcher.fetch_ohlcv(symbol, timeframe=tf, limit=5)
                        freq_str = "1min" if tf == "1m" else "1h"
                        clean_df = fetcher.clean_and_validate(raw_df, timeframe_freq=freq_str)
                        
                        # 永远只取倒数第二根 (死死收盘盖棺定论的 K 线)
                        latest_closed_row = clean_df.iloc[-2]
                        current_candle_ts = int(latest_closed_row.name.timestamp() * 1000)

                        # 如果这根 K 线还没发过
                        if current_candle_ts > last_sent_timestamps[task_id]:
                            
                            # 构建符合回测器 nested assets 结构的高级数据包
                            payload = {
                                "ticker": julia_ticker,
                                "resolution": tf, # 🚨 核心标签：告诉 Julia 路由给谁
                                "timestamp": current_candle_ts,
                                "price_at_signal": float(latest_closed_row["close"]),
                                "assets": {
                                    julia_ticker: serialize_row_to_dict(latest_closed_row, julia_ticker)
                                }
                            }

                            logger.info(f"[{tf}] Candle Closed! Dispatching {julia_ticker} -> Close: {payload['price_at_signal']}")
                            
                            # ZMQ 发送给 Julia
                            signal = zmq_client.send_market_slice(payload)

                            # 执行 OMS 路由
                            if signal and signal.get("action") != "HOLD":
                                
                                logger.warning(f"ALPHA TRIGGERED: {signal.get('alpha_id')} requests {signal.get('action')} on {julia_ticker}!")
                                signal["price_at_signal"] = payload["price_at_signal"]
                                
                                # ==========================================
                                # 🚨🚨🚨 空跑拦截开关 (Dry Run) 🚨🚨🚨
                                # ==========================================
                                DRY_RUN = True  # 【实盘前必看】True = 模拟拦截不花钱；False = 真实去交易所下单
                                
                                if DRY_RUN:
                                    # 拦截订单，只打印日志，不调用 executor
                                    logger.info(f"🛡️ [DRY RUN MODE] Intercepted Order: {signal.get('action')} on {julia_ticker} at {signal['price_at_signal']}. No real order sent.")
                                else:
                                    # 真正向 Kraken 交易所发送订单 (会扣除真实资金)
                                    executor.execute_signal(signal)
                                    
                            # 更新锁
                            last_sent_timestamps[task_id] = current_candle_ts

                    except Exception as loop_e:
                        logger.error(f"Error processing task {task_id}: {loop_e}")

                    # 保护交易所 API 限频
                    time.sleep(1)
                
                # 一轮遍历结束，休息 10 秒钟再开始下一轮扫盘
                time.sleep(10)

        except KeyboardInterrupt:
            logger.info("Live feed stopped by user.")
        except Exception as e:
            logger.error(f"Critical error in live feed: {e}", exc_info=True)

if __name__ == "__main__":
    run_live_feed()