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

# 强制安全加载环境变量
load_dotenv()

logger = logging.getLogger("OrderManager")

# ============================================================================
# STEP 1: Asynchronous SQLite State Machine (TradeLedger)
# ============================================================================
class TradeLedger:
    """异步 SQLite 账本，确保高频读写不会阻塞主线程"""
    def __init__(self, db_path: str = "executions.db"):
        self.db_path = db_path
        self.write_queue = Queue()
        self.writer_thread = threading.Thread(target=self._db_writer_worker, daemon=True)
        self.writer_thread.start()
        logger.info(f"TradeLedger background thread started. DB: {self.db_path}")

    def _db_writer_worker(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS executions (
                id TEXT PRIMARY KEY,
                timestamp REAL,
                ticker TEXT,
                action TEXT,
                filled_price REAL,
                amount REAL,
                fee REAL,
                order_id TEXT,
                strategy_id TEXT
            )
        ''')
        conn.commit()

        while True:
            trade_data = self.write_queue.get()
            if trade_data is None:
                break
            try:
                cursor.execute('''
                    INSERT INTO executions 
                    (id, timestamp, ticker, action, filled_price, amount, fee, order_id, strategy_id) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    trade_data['id'], trade_data['timestamp'], trade_data['ticker'],
                    trade_data['action'], trade_data['filled_price'], trade_data['amount'],
                    trade_data['fee'], trade_data['order_id'], trade_data.get('strategy_id', 'UNKNOWN')
                ))
                conn.commit()
            except Exception as e:
                logger.error(f"Failed to write trade to DB: {e}", exc_info=True)
            finally:
                self.write_queue.task_done()

    def record_trade(self, ticker: str, action: str, filled_price: float, amount: float, fee: float, order_id: str, strategy_id: str):
        payload = {
            "id": str(uuid.uuid4()), "timestamp": time.time(), "ticker": ticker,
            "action": action, "filled_price": filled_price, "amount": amount,
            "fee": fee, "order_id": order_id, "strategy_id": strategy_id
        }
        self.write_queue.put(payload)

# ============================================================================
# STEP 2: Risk Management Interceptor (风控拦截器)
# ============================================================================
class RiskManager:
    """全局物理风控：防止连环下单爆仓与胖手指巨量下单"""
    def __init__(self, cooldown_seconds: float = 10.0, max_notional_usd: float = 50000.0):
        self.cooldown_seconds = cooldown_seconds
        self.max_notional_usd = max_notional_usd
        self.last_trade_time = 0.0

    def check_risk(self, notional: float) -> tuple[bool, str]:
        current_time = time.time()
        time_since_last_trade = current_time - self.last_trade_time
        
        if time_since_last_trade < self.cooldown_seconds:
            return False, f"COOLDOWN_ACTIVE: Wait {self.cooldown_seconds - time_since_last_trade:.2f}s"
            
        if notional > self.max_notional_usd:
            return False, f"FAT_FINGER_BREACH: Notional {notional:.2f} exceeds {self.max_notional_usd}"
            
        return True, "PASS"

    def mark_trade_executed(self):
        self.last_trade_time = time.time()

# ============================================================================
# STEP 3: Continuous Volatility-Scaled Kelly Sizer
# ============================================================================
class KellyPositionSizer:
    def __init__(self, default_weight=0.1, kelly_fraction=0.5, max_weight=0.25):
        self.default_weight = default_weight
        self.kelly_fraction = kelly_fraction  
        self.max_weight = max_weight          
        self.alpha_stats = {}                 

    def register_alpha_stats(self, alpha_id: str, win_rate: float, win_loss_ratio: float):
        self.alpha_stats[alpha_id] = {'p': win_rate, 'b': win_loss_ratio}
        logger.info(f"Continuous Kelly registered stats for {alpha_id}: p={win_rate}, b={win_loss_ratio}")

    def calculate_target_weight(self, signal: Dict[str, Any]) -> float:
        alpha_id = signal.get("alpha_id", "UNKNOWN")
        vol_scalar = float(signal.get("vol_scalar", 1.0))
        
        if alpha_id in self.alpha_stats:
            p = self.alpha_stats[alpha_id]['p']
            b = self.alpha_stats[alpha_id]['b']
            
            if b <= 0: return 0.0 
            
            full_kelly = p - ((1.0 - p) / b)
            if full_kelly <= 0:
                logger.warning(f"📉 [Kelly Negative] {alpha_id} has negative edge. Weight truncated to 0.")
                return 0.0
                
            base_safe_kelly = full_kelly * self.kelly_fraction
            adjusted_kelly = base_safe_kelly * vol_scalar
            final_weight = min(adjusted_kelly, self.max_weight)
            
            logger.info(f"📊 [Continuous Kelly] {alpha_id} -> Base: {base_safe_kelly*100:.1f}% | Vol-Scalar: {vol_scalar:.2f}x | Final Execution: {final_weight*100:.1f}%")
            return final_weight
            
        else:
            fallback_w = float(signal.get("weight", self.default_weight))
            logger.debug(f"[Continuous Kelly] No stats for {alpha_id}, fallback to signal weight: {fallback_w}")
            return fallback_w

# ============================================================================
# STEP 4: Order Executor (Kraken/Binance) - DRY_RUN EQUIPPED
# ============================================================================
class ExchangeExecution:
    """物理订单执行器"""
    def __init__(self, exchange: ccxt.kraken):
        self.exchange = exchange
        self.ledger = TradeLedger()
        self.risk_manager = RiskManager(cooldown_seconds=10.0, max_notional_usd=20000.0)
        
        self.kelly_sizer = KellyPositionSizer(kelly_fraction=0.5, max_weight=0.25)
        
        # 【填入 Alpha 的历史表现用于计算基础凯利】
        self.kelly_sizer.register_alpha_stats("Alpha005_MacroReflexivity", win_rate=0.48, win_loss_ratio=1.65)
        
        # =====================================================
        # 🚨 全局模拟开关 (DRY_RUN BUTTON)
        # 默认从 .env 环境变量读取，找不到则默认为 True (绝对安全)
        # =====================================================
        self.dry_run = os.getenv("DRY_RUN", "True").lower() in ('true', '1', 't')
        
        if self.dry_run:
            logger.warning("🛡️ SYSTEM IS IN [DRY RUN] MODE. NO REAL MONEY WILL BE TRADED.")
        else:
            logger.critical("🔥 SYSTEM IS IN [LIVE] MODE. REAL API CALLS WILL BE EXECUTED.")
            logger.info("Initializing CCXT Execution Engine. Loading Markets...")
            self.exchange.load_markets() # 只有实盘才需要加载真实的交易所市场规则

    def execute_signal(self, signal: Dict[str, Any]):
        action = signal.get("action", "HOLD").upper()
        if action == "HOLD":
            return
            
        ticker = signal.get("ticker")
        alpha_id = signal.get("alpha_id", "UNKNOWN")
        signal_price = float(signal.get("price_at_signal", 0.0))
        
        weight = self.kelly_sizer.calculate_target_weight(signal)
        if weight <= 0:
            logger.warning(f"Trade ignored: Kelly Position Sizer evaluated weight <= 0 for {ticker}.")
            return

        try:
            base_currency, quote_currency = ticker.replace("_", "/").split('/')
            
            # --- 1. 获取账户余额：真假分流 ---
            if self.dry_run:
                balance = {quote_currency: 100000.0, base_currency: 0.0} # 沙盒模拟 10 万刀
            else:
                balance = self.exchange.fetch_free_balance()             # 拉取真实资产
            
            amount_to_transact = 0.0
            notional_value = 0.0
            
            if action == "BUY":
                quote_balance = balance.get(quote_currency, 0.0)
                notional_value = quote_balance * weight
                amount_to_transact = notional_value / signal_price
            elif action == "SELL":
                base_balance = balance.get(base_currency, 0.0)
                amount_to_transact = base_balance * weight if base_balance > 0 else 0
                notional_value = amount_to_transact * signal_price

            if notional_value < 10.0:  
                logger.warning(f"Trade ignored: Notional {notional_value:.2f} is below minimum order size.")
                return

            risk_passed, risk_reason = self.risk_manager.check_risk(notional_value)
            if not risk_passed:
                logger.warning(f"🛡️ RISK INTERCEPTED [{action} {ticker}]: {risk_reason}")
                return

            # --- 2. 下单操作：真假分流 ---
            if self.dry_run:
                # 模拟精度转换
                precise_amount_str = str(round(amount_to_transact, 6)) 
                logger.info(f"🛡️ [DRY RUN] Would execute MARKET {action} for {precise_amount_str} {base_currency} on {ticker}")
                
                # 模拟交易所返回的回执
                order_receipt = {
                    'id': f"mock-{str(uuid.uuid4())[:8]}",
                    'average': signal_price, 
                    'filled': float(precise_amount_str),
                    'fee': {'cost': notional_value * 0.001} 
                }
            else:
                # 真实的精度要求极严，必须用 CCXT 内置函数处理
                precise_amount_str = self.exchange.amount_to_precision(ticker, amount_to_transact)
                logger.critical(f"🔥 [LIVE] EXECUTING MARKET {action} for {precise_amount_str} {base_currency} on {ticker}")
                
                # 真实呼叫 Kraken / Binance 的 API
                order_receipt = self.exchange.create_market_order(ticker, action.lower(), float(precise_amount_str))

            self.risk_manager.mark_trade_executed()
            
            filled_price = order_receipt.get('average', signal_price)
            actual_amount = order_receipt.get('filled', float(precise_amount_str))
            fee_cost = order_receipt.get('fee', {}).get('cost', 0.0)
            order_id = order_receipt.get('id', 'UNKNOWN')
            
            logger.info(f"✅ Order Filled! ID: {order_id} | Price: {filled_price} | Amt: {actual_amount} | Strategy: {alpha_id}")
            
            self.ledger.record_trade(
                ticker=ticker, action=action, filled_price=filled_price,
                amount=actual_amount, fee=fee_cost, order_id=order_id, strategy_id=alpha_id
            )

        except Exception as e:
            logger.error(f"Critical execution failure for {action} {ticker}: {e}", exc_info=True)