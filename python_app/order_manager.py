import os
import time
import uuid
import logging
import sqlite3
import threading
from queue import Queue
from typing import Dict, Any
from dataclasses import dataclass
from datetime import datetime, timezone
import ccxt
from dotenv import load_dotenv

# 强制安全加载环境变量
load_dotenv()

logger = logging.getLogger("OrderManager")

# ============================================================================
# STEP 0: Per-Alpha Independent Capital Structures
# ============================================================================
@dataclass
class AlphaAccount:
    """State tracker for per-alpha independent capital allocation."""
    alpha_id: str
    initial_capital: float      # Starting USD allocation
    current_capital: float      # Current USD value (updated after each trade)
    total_pnl: float            # Cumulative P&L in USD
    total_trades: int           # Total number of trades executed
    winning_trades: int         # Number of profitable trades
    is_active: bool             # False if capital drops below 20% of initial
    
    # Internal variables for calculating realized P&L vs Entry Price
    current_position: float = 0.0 
    average_entry_price: float = 0.0

    @property
    def win_rate(self) -> float:
        """Calculate win rate as a fraction."""
        return self.winning_trades / self.total_trades if self.total_trades > 0 else 0.0

    @property
    def pnl_pct(self) -> float:
        """Calculate P&L as percentage of initial capital."""
        return (self.total_pnl / self.initial_capital) * 100


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
        
        # Backward Compatibility: Safely inject new columns into existing database
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='executions'")
        if cursor.fetchone():
            for col in ['alpha_capital_before', 'alpha_capital_after', 'trade_pnl']:
                try:
                    cursor.execute(f"ALTER TABLE executions ADD COLUMN {col} REAL")
                except sqlite3.OperationalError:
                    pass  # Column already exists

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
                strategy_id TEXT,
                alpha_capital_before REAL,
                alpha_capital_after REAL,
                trade_pnl REAL
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
                    (id, timestamp, ticker, action, filled_price, amount, fee, order_id, strategy_id, 
                     alpha_capital_before, alpha_capital_after, trade_pnl) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    trade_data['id'], trade_data['timestamp'], trade_data['ticker'],
                    trade_data['action'], trade_data['filled_price'], trade_data['amount'],
                    trade_data['fee'], trade_data['order_id'], trade_data.get('strategy_id', 'UNKNOWN'),
                    trade_data.get('alpha_capital_before', 0.0), trade_data.get('alpha_capital_after', 0.0),
                    trade_data.get('trade_pnl', 0.0)
                ))
                conn.commit()
            except Exception as e:
                logger.error(f"Failed to write trade to DB: {e}", exc_info=True)
            finally:
                self.write_queue.task_done()

    def record_trade(self, ticker: str, action: str, filled_price: float, amount: float, fee: float, order_id: str, strategy_id: str, alpha_capital_before: float = 0.0, alpha_capital_after: float = 0.0, trade_pnl: float = 0.0):
        payload = {
            "id": str(uuid.uuid4()), "timestamp": time.time(), "ticker": ticker,
            "action": action, "filled_price": filled_price, "amount": amount,
            "fee": fee, "order_id": order_id, "strategy_id": strategy_id,
            "alpha_capital_before": alpha_capital_before, 
            "alpha_capital_after": alpha_capital_after, 
            "trade_pnl": trade_pnl
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
    
    ALPHA_CAPITAL_CONFIG = {
        "Alpha005_MacroReflexivity": 50.0,   # Initial allocation in USD
        "Alpha003_PriceKinematics":  0.0,   # Initial allocation in USD
    }

    def __init__(self, exchange: ccxt.kraken):
        self.exchange = exchange
        self.ledger = TradeLedger()
        self.risk_manager = RiskManager(cooldown_seconds=10.0, max_notional_usd=20000.0)
        
        self.kelly_sizer = KellyPositionSizer(kelly_fraction=0.5, max_weight=0.25)
        self.kelly_sizer.register_alpha_stats("Alpha005_MacroReflexivity", win_rate=0.48, win_loss_ratio=1.65)

        # --- Fix 2: Validate ALPHA_CAPITAL_CONFIG keys match registered Kelly stats ---
        for aid in self.ALPHA_CAPITAL_CONFIG:
            if aid not in self.kelly_sizer.alpha_stats:
                logger.warning(
                    f"Alpha '{aid}' has a capital account but no Kelly stats. "
                    f"Will use fallback weight from signal."
                )
        
        self.alpha_accounts: Dict[str, AlphaAccount] = {}
        for aid, init_cap in self.ALPHA_CAPITAL_CONFIG.items():
            self.alpha_accounts[aid] = AlphaAccount(
                alpha_id=aid,
                initial_capital=init_cap,
                current_capital=init_cap,
                total_pnl=0.0,
                total_trades=0,
                winning_trades=0,
                is_active=True
            )
            
        # Reconstruct current_capital and state from historical DB if it exists
        self._reconstruct_alpha_accounts_from_db()
        
        # =====================================================
        # 🚨 全局模拟开关 (DRY_RUN BUTTON)
        # =====================================================
        self.dry_run = os.getenv("DRY_RUN", "True").lower() in ('true', '1', 't')
        
        if self.dry_run:
            logger.warning("🛡️ SYSTEM IS IN [DRY RUN] MODE. NO REAL MONEY WILL BE TRADED.")
        else:
            logger.critical("🔥 SYSTEM IS IN [LIVE] MODE. REAL API CALLS WILL BE EXECUTED.")
            logger.info("Initializing CCXT Execution Engine. Loading Markets...")
            self.exchange.load_markets()

        # --- Fix 4: Log initial capital allocation on startup ---
        logger.info("=" * 60)
        logger.info("INITIAL CAPITAL ALLOCATION")
        logger.info("=" * 60)
        for aid, acc in self.alpha_accounts.items():
            logger.info(
                f"  {aid}: ${acc.current_capital:.2f} "
                f"({'ACTIVE' if acc.is_active else 'DISABLED'})"
            )
        logger.info("=" * 60)

    def _reconstruct_alpha_accounts_from_db(self) -> None:
        """Helper to reconstruct the current_capital of alpha accounts from trade history on startup."""
        try:
            conn = sqlite3.connect(self.ledger.db_path)
            cursor = conn.cursor()
            
            # Check if execution table has the new schemas yet
            cursor.execute("PRAGMA table_info(executions)")
            columns = [info[1] for info in cursor.fetchall()]
            if 'trade_pnl' not in columns:
                return  # Skip reconstruction on legacy schemas
                
            cursor.execute("SELECT strategy_id, action, filled_price, amount, fee, trade_pnl, alpha_capital_after FROM executions ORDER BY timestamp ASC")
            for row in cursor.fetchall():
                sid, act, f_price, amt, fe, pnl, cap_after = row
                if sid in self.alpha_accounts:
                    acc = self.alpha_accounts[sid]
                    
                    if cap_after is not None and cap_after > 0:
                        acc.current_capital = cap_after
                        
                    if act == "BUY":
                        new_pos = acc.current_position + amt
                        if new_pos > 0:
                            acc.average_entry_price = ((acc.current_position * acc.average_entry_price) + (amt * f_price)) / new_pos
                        acc.current_position = new_pos
                        
                    elif act == "SELL":
                        acc.current_position = max(0.0, acc.current_position - amt)
                        if acc.current_position == 0.0:
                            acc.average_entry_price = 0.0
                            
                        # Increment stats on Sell (Round Trip)
                        acc.total_trades += 1
                        if pnl is not None:
                            acc.total_pnl += pnl
                            if pnl > 0:
                                acc.winning_trades += 1
                                
                    if acc.current_capital < acc.initial_capital * 0.20:
                        acc.is_active = False
            conn.close()
        except Exception as e:
            logger.warning(f"Skipped DB reconstruction logic due to DB error/absence: {e}")

    # --- Fix 3: Add manual re-enable method for disabled alphas ---
    def re_enable_alpha(self, alpha_id: str, reset_capital: bool = False) -> None:
        """
        Manually re-enable a disabled alpha account after review.
        
        Args:
            alpha_id: The alpha strategy identifier to re-enable.
            reset_capital: If True, reset current_capital to initial_capital.
                           Use only after manual capital injection.
        """
        if alpha_id not in self.alpha_accounts:
            logger.error(f"Cannot re-enable unknown alpha: {alpha_id}")
            return
        
        account = self.alpha_accounts[alpha_id]
        account.is_active = True
        
        if reset_capital:
            account.current_capital = account.initial_capital
            logger.warning(
                f"Alpha {alpha_id} re-enabled with RESET capital "
                f"${account.initial_capital:.2f}. "
                f"Ensure manual capital injection has been made."
            )
        else:
            logger.warning(
                f"Alpha {alpha_id} re-enabled with current capital "
                f"${account.current_capital:.2f}. Monitor closely."
            )

    def _update_alpha_account(
        self,
        alpha_id: str,
        action: str,
        filled_price: float,
        amount: float,
        fee: float,
        entry_price: float | None = None
    ) -> float:
        """
        Update the alpha's virtual account after a trade.
        
        For BUY: deduct notional + fee from capital
        For SELL: add proceeds - fee to capital, calculate P&L vs entry price
        
        Returns:
            float: The P&L of the specific trade (0.0 for BUY orders)
        """
        account = self.alpha_accounts[alpha_id]
        trade_pnl = 0.0
        
        if action == "BUY":
            # Deduct notional + fee from capital
            cost = (amount * filled_price) + fee
            account.current_capital -= cost
            
            # Recalculate rolling average entry price
            new_pos = account.current_position + amount
            if new_pos > 0:
                account.average_entry_price = ((account.current_position * account.average_entry_price) + (amount * filled_price)) / new_pos
            account.current_position = new_pos
            
        elif action == "SELL":
            # Add proceeds - fee to capital
            proceeds = (amount * filled_price) - fee
            account.current_capital += proceeds
            
            # Calculate P&L vs entry price (use passed explicit entry or rolling VWAP)
            use_entry = entry_price if entry_price is not None else account.average_entry_price
            trade_pnl = (filled_price - use_entry) * amount - fee
            
            # Update running stats on completion of round trip (SELL)
            account.total_pnl += trade_pnl
            account.total_trades += 1
            if trade_pnl > 0:
                account.winning_trades += 1
                
            account.current_position = max(0.0, account.current_position - amount)
            if account.current_position == 0.0:
                account.average_entry_price = 0.0
                
        # Auto-disable alpha if capital drops below threshold
        DISABLE_THRESHOLD = 0.20
        if account.current_capital < account.initial_capital * DISABLE_THRESHOLD and account.is_active:
            account.is_active = False
            logger.critical(
                f"🚨 ALPHA DISABLED: {alpha_id} capital dropped to "
                f"${account.current_capital:.2f} "
                f"({(account.current_capital/account.initial_capital)*100:.1f}% of initial). "
                f"Manual review required before re-enabling."
            )
            
        return trade_pnl

    def log_portfolio_summary(self) -> None:
        """
        Log a formatted summary of all alpha accounts.
        Called every hour by live_feed_integration.py.
        """
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:00 UTC")
        
        lines = [
            "=" * 60,
            f"PORTFOLIO SUMMARY — {now_str}",
            "=" * 60
        ]
        
        total_cap = 0.0
        total_initial = 0.0
        total_pnl = 0.0
        
        for aid, acc in self.alpha_accounts.items():
            pnl_sign = "+" if acc.total_pnl >= 0 else ""
            lines.append(
                f"{aid:<25} | Capital: ${acc.current_capital:.2f} | "
                f"P&L: {pnl_sign}${acc.total_pnl:.2f} ({pnl_sign}{acc.pnl_pct:.1f}%) | "
                f"Trades: {acc.total_trades} | Win: {acc.win_rate * 100:.1f}%"
            )
            total_cap += acc.current_capital
            total_initial += acc.initial_capital
            total_pnl += acc.total_pnl
            
        total_pnl_pct = (total_pnl / total_initial * 100) if total_initial > 0 else 0.0
        pnl_sign_tot = "+" if total_pnl >= 0 else ""
        
        lines.append("─" * 60)
        lines.append(
            f"{'TOTAL':<25} | Capital: ${total_cap:.2f} | "
            f"P&L: {pnl_sign_tot}${total_pnl:.2f} ({pnl_sign_tot}{total_pnl_pct:.1f}%)"
        )
        lines.append("=" * 60)
        
        for line in lines:
            logger.info(line)

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
            
            # --- 1. 获取账户余额：真假分流 & 独立资金池映射 ---
            account = self.alpha_accounts.get(alpha_id)
            
            if self.dry_run:
                if account is None:
                    # Backward compatibility fallback
                    logger.warning(f"No capital account for {alpha_id}. Falling back to default behavior.")
                    balance = {quote_currency: 100000.0, base_currency: 0.0}
                else:
                    if not account.is_active:
                        logger.warning(f"Alpha {alpha_id} account is DISABLED (capital too low). Trade ignored.")
                        return
                    # Use per-alpha capital for position sizing 
                    # (Injecting account.current_position to base_currency to mathematically allow SELL orders to size properly)
                    available_capital = account.current_capital
                    balance = {quote_currency: available_capital, base_currency: account.current_position}
            else:
                balance = self.exchange.fetch_free_balance()
                # If Live, strictly respect the alpha's maximum allowed capital relative to real balance
                if account is not None:
                    if not account.is_active:
                        logger.warning(f"Alpha {alpha_id} account is DISABLED (capital too low). Trade ignored.")
                        return
                    balance = {
                        quote_currency: min(balance.get(quote_currency, 0.0), account.current_capital),
                        base_currency: min(balance.get(base_currency, 0.0), account.current_position)
                    }

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
                precise_amount_str = str(round(amount_to_transact, 6)) 
                logger.info(f"🛡️ [DRY RUN] Would execute MARKET {action} for {precise_amount_str} {base_currency} on {ticker}")
                
                order_receipt = {
                    'id': f"mock-{str(uuid.uuid4())[:8]}",
                    'average': signal_price, 
                    'filled': float(precise_amount_str),
                    'fee': {'cost': notional_value * 0.001} 
                }
            else:
                precise_amount_str = self.exchange.amount_to_precision(ticker, amount_to_transact)
                logger.critical(f"🔥 [LIVE] EXECUTING MARKET {action} for {precise_amount_str} {base_currency} on {ticker}")
                
                order_receipt = self.exchange.create_market_order(ticker, action.lower(), float(precise_amount_str))

            self.risk_manager.mark_trade_executed()
            
            filled_price = order_receipt.get('average', signal_price)
            actual_amount = order_receipt.get('filled', float(precise_amount_str))
            fee_cost = order_receipt.get('fee', {}).get('cost', 0.0)
            order_id = order_receipt.get('id', 'UNKNOWN')
            
            # --- 3. Capital Account Deductions & DB Logging ---
            alpha_capital_before = account.current_capital if account else 0.0
            alpha_capital_after = alpha_capital_before
            trade_pnl = 0.0

            if account is not None:
                trade_pnl = self._update_alpha_account(
                    alpha_id=alpha_id,
                    action=action,
                    filled_price=filled_price,
                    amount=actual_amount,
                    fee=fee_cost
                )
                alpha_capital_after = account.current_capital
            
            logger.info(f"✅ Order Filled! ID: {order_id} | Price: {filled_price} | Amt: {actual_amount} | Strategy: {alpha_id}")
            
            self.ledger.record_trade(
                ticker=ticker, action=action, filled_price=filled_price,
                amount=actual_amount, fee=fee_cost, order_id=order_id, strategy_id=alpha_id,
                alpha_capital_before=alpha_capital_before,
                alpha_capital_after=alpha_capital_after,
                trade_pnl=trade_pnl
            )

        except Exception as e:
            logger.error(f"Critical execution failure for {action} {ticker}: {e}", exc_info=True)