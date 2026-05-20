import os
import time
import uuid
import logging
import sqlite3
import threading
from queue import Queue
from typing import Dict, Any
import ccxt
from dotenv import load_dotenv

# Force load environment variables securely
load_dotenv()

logger = logging.getLogger("OrderManager")

# ============================================================================
# STEP 1: Asynchronous SQLite State Machine (TradeLedger)
# ============================================================================
class TradeLedger:
    """
    Industrial-grade async database writer.
    Uses a dedicated background thread and a Thread-Safe Queue to ensure 
    SQLite I/O NEVER blocks the main high-frequency event loop.
    """
    def __init__(self, db_path: str = "executions.db"):
        self.db_path = db_path
        self.write_queue = Queue()
        
        # Start the background writer thread
        self.writer_thread = threading.Thread(target=self._db_writer_worker, daemon=True)
        self.writer_thread.start()
        logger.info(f"TradeLedger background thread started. DB: {self.db_path}")

    def _db_writer_worker(self):
        """Dedicated thread strictly for DB operations to prevent thread-safety issues."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Auto-create the ledger table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS executions (
                id TEXT PRIMARY KEY,
                timestamp REAL,
                ticker TEXT,
                action TEXT,
                filled_price REAL,
                amount REAL,
                fee REAL,
                order_id TEXT
            )
        ''')
        conn.commit()

        while True:
            # Block until there is data to write
            trade_data = self.write_queue.get()
            if trade_data is None:  # Poison pill for graceful shutdown
                break
                
            try:
                cursor.execute('''
                    INSERT INTO executions 
                    (id, timestamp, ticker, action, filled_price, amount, fee, order_id) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    trade_data['id'],
                    trade_data['timestamp'],
                    trade_data['ticker'],
                    trade_data['action'],
                    trade_data['filled_price'],
                    trade_data['amount'],
                    trade_data['fee'],
                    trade_data['order_id']
                ))
                conn.commit()
                logger.debug(f"Trade {trade_data['id']} persisted to SQLite asynchronously.")
            except Exception as e:
                logger.error(f"Failed to write trade to DB: {e}", exc_info=True)
            finally:
                self.write_queue.task_done()

    def record_trade(self, ticker: str, action: str, filled_price: float, amount: float, fee: float, order_id: str):
        """Pushes trade data to the queue. Non-blocking."""
        payload = {
            "id": str(uuid.uuid4()),
            "timestamp": time.time(),
            "ticker": ticker,
            "action": action,
            "filled_price": filled_price,
            "amount": amount,
            "fee": fee,
            "order_id": order_id
        }
        self.write_queue.put(payload)

# ============================================================================
# STEP 3: Risk Management Interceptor
# ============================================================================
class RiskManager:
    def __init__(self, cooldown_seconds: float = 10.0, max_notional_usd: float = 50000.0):
        self.cooldown_seconds = cooldown_seconds
        self.max_notional_usd = max_notional_usd
        self.last_trade_time = 0.0

    def check_risk(self, notional: float) -> tuple[bool, str]:
        """Evaluates strict risk rules before allowing execution."""
        current_time = time.time()
        
        # 1. Cooldown Rule
        time_since_last_trade = current_time - self.last_trade_time
        if time_since_last_trade < self.cooldown_seconds:
            return False, f"COOLDOWN_ACTIVE: Wait {self.cooldown_seconds - time_since_last_trade:.2f}s"
            
        # 2. Fat Finger Rule (Max Position Sizing)
        if notional > self.max_notional_usd:
            return False, f"FAT_FINGER_BREACH: Notional {notional:.2f} exceeds {self.max_notional_usd}"
            
        return True, "PASS"

    def mark_trade_executed(self):
        """Updates the cooldown timer only after an order is actually sent."""
        self.last_trade_time = time.time()

# ============================================================================
# STEP 2: Order Executor (Binance)
# ============================================================================
class ExchangeExecution:
    # 建议将类型提示改为通用的 ccxt.Exchange 或者 ccxt.kraken
    def __init__(self, exchange: ccxt.kraken):
        self.exchange = exchange
        self.ledger = TradeLedger()
        self.risk_manager = RiskManager()
        
        logger.info("Initializing Kraken Execution Engine. Loading Markets...")
        self.exchange.load_markets()

    def execute_signal(self, signal: Dict[str, Any]):
        """Translates alpha signal into a concrete CCXT market order."""
        action = signal.get("action", "HOLD").upper()
        if action == "HOLD":
            return
            
        ticker = signal.get("ticker")
        weight = float(signal.get("weight", 0.0))
        signal_price = float(signal.get("price_at_signal", 0.0))
        
        if weight <= 0:
            logger.warning("Received trade signal with weight <= 0. Ignored.")
            return

        try:
            # Determine Base and Quote currencies (e.g., BTC/USDT -> Base: BTC, Quote: USDT)
            base_currency, quote_currency = ticker.split('/')
            
            # Fetch balances
            balance = self.exchange.fetch_free_balance()
            
            # Calculate intended Order Size
            amount_to_transact = 0.0
            notional_value = 0.0
            
            if action == "BUY":
                quote_balance = balance.get(quote_currency, 0.0)
                # Buy sizing is based on available Quote currency (USDT)
                notional_value = quote_balance * weight
                amount_to_transact = notional_value / signal_price
            elif action == "SELL":
                base_balance = balance.get(base_currency, 0.0)
                # Sell sizing is based on available Base currency (BTC)
                amount_to_transact = base_balance * weight
                notional_value = amount_to_transact * signal_price

            # --- RISK INTERCEPTION ---
            risk_passed, risk_reason = self.risk_manager.check_risk(notional_value)
            if not risk_passed:
                logger.warning(f"🛡️ RISK INTERCEPTED [{action} {ticker}]: {risk_reason}")
                return

            # Avoid dust orders (Exchange specific minimums)
            if notional_value < 5.0:  # Prevent extremely small orders that get rejected
                logger.warning(f"Trade ignored: Notional {notional_value:.2f} is below minimum.")
                return

            # Format precision based on exchange rules
            # e.g., converts 1.2345678 to '1.23456' string matching lot size
            precise_amount_str = self.exchange.amount_to_precision(ticker, amount_to_transact)
            
            logger.info(f"Executing MARKET {action} for {precise_amount_str} {base_currency} on {ticker}")
            
            # --- EXECUTE ORDER ---
            # NOTE: Uncomment the line below in real production. Kept commented to prevent accidental real orders during initial run.
            # order_receipt = self.exchange.create_market_order(ticker, action.lower(), float(precise_amount_str))
            
            # MOCK RECEIPT FOR SAFETY DURING TESTING (Remove in production)
            order_receipt = {
                'id': str(uuid.uuid4())[:8],
                'average': signal_price, # Mocks filled price
                'filled': float(precise_amount_str),
                'fee': {'cost': notional_value * 0.001} # Mocks 0.1% fee
            }

            # Update Risk Manager
            self.risk_manager.mark_trade_executed()
            
            # Extract filled details
            filled_price = order_receipt.get('average', signal_price)
            actual_amount = order_receipt.get('filled', amount_to_transact)
            fee_cost = order_receipt.get('fee', {}).get('cost', 0.0)
            order_id = order_receipt.get('id', 'UNKNOWN')
            
            logger.info(f"✅ Order Filled! ID: {order_id} | Price: {filled_price} | Amt: {actual_amount}")
            
            # --- ASYNC LOGGING ---
            self.ledger.record_trade(
                ticker=ticker,
                action=action,
                filled_price=filled_price,
                amount=actual_amount,
                fee=fee_cost,
                order_id=order_id
            )

        except ccxt.InsufficientFunds as e:
            logger.error(f"Insufficient funds to execute {action} {ticker}: {e}")
        except Exception as e:
            logger.error(f"Critical execution failure for {action} {ticker}: {e}", exc_info=True)