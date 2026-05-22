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
                order_id TEXT
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
                    (id, timestamp, ticker, action, filled_price, amount, fee, order_id) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    trade_data['id'], trade_data['timestamp'], trade_data['ticker'],
                    trade_data['action'], trade_data['filled_price'], trade_data['amount'],
                    trade_data['fee'], trade_data['order_id']
                ))
                conn.commit()
            except Exception as e:
                logger.error(f"Failed to write trade to DB: {e}", exc_info=True)
            finally:
                self.write_queue.task_done()

    def record_trade(self, ticker: str, action: str, filled_price: float, amount: float, fee: float, order_id: str):
        payload = {
            "id": str(uuid.uuid4()), "timestamp": time.time(), "ticker": ticker,
            "action": action, "filled_price": filled_price, "amount": amount,
            "fee": fee, "order_id": order_id
        }
        self.write_queue.put(payload)

# ============================================================================
# STEP 2: Risk Management Interceptor
# ============================================================================
class RiskManager:
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
# STEP 3: 🚀 Kelly Criterion Position Sizer Plugin 🚀
# ============================================================================
class KellyPositionSizer:
    """
    基于凯利公式的动态资金管理模块
    公式: f* = p - (1-p) / b
    (p = 胜率, b = 盈亏比 Profit/Loss Ratio)
    """
    def __init__(self, default_weight=0.1, kelly_fraction=0.5, max_weight=0.3):
        self.default_weight = default_weight
        self.kelly_fraction = kelly_fraction  # 默认 0.5 即半凯利 (Half-Kelly)，降低回撤方差
        self.max_weight = max_weight          # 硬性顶盖，单次最高不超过总仓位的 30%
        self.alpha_stats = {}                 # 记录各策略的回测性能参数

    def register_alpha_stats(self, alpha_id: str, win_rate: float, win_loss_ratio: float):
        """注入历史回测统计数据，用于启动时计算凯利阀值"""
        self.alpha_stats[alpha_id] = {
            'p': win_rate,
            'b': win_loss_ratio
        }
        logger.info(f"Kelly Sizer registered stats for {alpha_id}: p={win_rate}, b={win_loss_ratio}")

    def calculate_target_weight(self, signal: Dict[str, Any]) -> float:
        alpha_id = signal.get("alpha_id", "UNKNOWN")
        
        # 1. 检查是否存在该 Alpha 的统计性能
        if alpha_id in self.alpha_stats:
            p = self.alpha_stats[alpha_id]['p']
            b = self.alpha_stats[alpha_id]['b']
            
            if b <= 0: return 0.0 # 盈亏比异常保护
            
            # 2. 计算标准凯利比例
            q = 1.0 - p
            full_kelly = p - (q / b)
            
            # 3. 凯利公式安全修正 (小于0表示该策略期望为负，不应交易)
            if full_kelly <= 0:
                logger.warning(f"📉 [Kelly Negative] {alpha_id} has negative edge. Weight truncated to 0.")
                return 0.0
                
            # 4. 应用半凯利折扣与硬顶限制
            adjusted_kelly = full_kelly * self.kelly_fraction
            final_weight = min(adjusted_kelly, self.max_weight)
            
            logger.info(f"📊 [Kelly Sizer] {alpha_id} -> Full Kelly: {full_kelly*100:.1f}%, Applied Half-Kelly: {final_weight*100:.1f}%")
            return final_weight
            
        else:
            # 如果是新策略未注册数据，退回到 Julia 原生传过来的固定 weight
            fallback_w = float(signal.get("weight", self.default_weight))
            logger.debug(f"[Kelly Sizer] No stats for {alpha_id}, using default fallback weight: {fallback_w}")
            return fallback_w

# ============================================================================
# STEP 4: Order Executor (Kraken/Binance)
# ============================================================================
class ExchangeExecution:
    def __init__(self, exchange: ccxt.kraken):
        self.exchange = exchange
        self.ledger = TradeLedger()
        self.risk_manager = RiskManager()
        
        # --- 注入凯利管理插件 ---
        self.kelly_sizer = KellyPositionSizer(kelly_fraction=0.5, max_weight=0.3)
        
        # 【重要】：在这里预先填入你 Julia 回测出来的策略胜率(p)和盈亏比(b)
        # 例如我们假设 Alpha005 宏观策略表现为：胜率 65%，平均盈亏比 2.4
        self.kelly_sizer.register_alpha_stats("Alpha005_Reflexivity", win_rate=0.65, win_loss_ratio=2.4)
        # 假设 Alpha003 高频动量表现为：胜率 45%，平均盈亏比 1.5
        self.kelly_sizer.register_alpha_stats("Alpha003_PriceKinematics", win_rate=0.45, win_loss_ratio=1.5)
        
        logger.info("Initializing CCXT Execution Engine. Loading Markets...")
        self.exchange.load_markets()

    def execute_signal(self, signal: Dict[str, Any]):
        action = signal.get("action", "HOLD").upper()
        if action == "HOLD":
            return
            
        ticker = signal.get("ticker")
        signal_price = float(signal.get("price_at_signal", 0.0))
        
        # 🚨 动态仓位接管：用凯利插件计算出的仓位比例，覆盖掉默认的 weight
        weight = self.kelly_sizer.calculate_target_weight(signal)
        
        if weight <= 0:
            logger.warning(f"Trade ignored: Kelly Position Sizer evaluated weight <= 0 for {ticker}.")
            return

        try:
            base_currency, quote_currency = ticker.split('/')
            balance = self.exchange.fetch_free_balance()
            
            amount_to_transact = 0.0
            notional_value = 0.0
            
            if action == "BUY":
                quote_balance = balance.get(quote_currency, 0.0)
                notional_value = quote_balance * weight
                amount_to_transact = notional_value / signal_price
            elif action == "SELL":
                base_balance = balance.get(base_currency, 0.0)
                amount_to_transact = base_balance * weight
                notional_value = amount_to_transact * signal_price

            # --- RISK INTERCEPTION ---
            risk_passed, risk_reason = self.risk_manager.check_risk(notional_value)
            if not risk_passed:
                logger.warning(f"🛡️ RISK INTERCEPTED [{action} {ticker}]: {risk_reason}")
                return

            if notional_value < 5.0:  
                logger.warning(f"Trade ignored: Notional {notional_value:.2f} is below minimum.")
                return

            precise_amount_str = self.exchange.amount_to_precision(ticker, amount_to_transact)
            logger.info(f"Executing MARKET {action} for {precise_amount_str} {base_currency} on {ticker}")
            
            # --- MOCK RECEIPT FOR SAFETY DURING TESTING ---
            order_receipt = {
                'id': str(uuid.uuid4())[:8],
                'average': signal_price, 
                'filled': float(precise_amount_str),
                'fee': {'cost': notional_value * 0.001} 
            }

            self.risk_manager.mark_trade_executed()
            
            filled_price = order_receipt.get('average', signal_price)
            actual_amount = order_receipt.get('filled', amount_to_transact)
            fee_cost = order_receipt.get('fee', {}).get('cost', 0.0)
            order_id = order_receipt.get('id', 'UNKNOWN')
            
            logger.info(f"✅ Order Filled! ID: {order_id} | Price: {filled_price} | Amt: {actual_amount}")
            
            self.ledger.record_trade(
                ticker=ticker, action=action, filled_price=filled_price,
                amount=actual_amount, fee=fee_cost, order_id=order_id
            )

        except Exception as e:
            logger.error(f"Critical execution failure for {action} {ticker}: {e}", exc_info=True)