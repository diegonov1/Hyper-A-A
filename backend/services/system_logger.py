"""
System Log Collector Service
：AI
"""

import logging
from collections import deque
from datetime import datetime
from typing import Dict, List, Optional, Deque
from dataclasses import dataclass, asdict
import threading
import json


@dataclass
class LogEntry:
    """"""
    timestamp: str
    level: str  # INFO, WARNING, ERROR
    category: str  # price_update, ai_decision, system_error
    message: str
    details: Optional[Dict] = None

    def to_dict(self):
        """"""
        return asdict(self)


class SystemLogCollector:
    """"""

    def __init__(self, max_logs: int = 500):
        """
        

        Args:
            max_logs: 
        """
        self._logs: Deque[LogEntry] = deque(maxlen=max_logs)
        self._lock = threading.Lock()
        self._listeners = []  # WebSocket

    def add_log(self, level: str, category: str, message: str, details: Optional[Dict] = None):
        """
        

        Args:
            level:  (INFO, WARNING, ERROR)
            category:  (price_update, ai_decision, system_error)
            message: 
            details: 
        """
        entry = LogEntry(
            timestamp=datetime.now().isoformat(),
            level=level,
            category=category,
            message=message,
            details=details or {}
        )

        with self._lock:
            self._logs.append(entry)

        # 
        self._notify_listeners(entry)

    _LEVEL_ORDER = {
        "INFO": 1,
        "WARNING": 2,
        "ERROR": 3,
    }

    def get_logs(
        self,
        level: Optional[str] = None,
        category: Optional[str] = None,
        limit: int = 100,
        min_level: Optional[str] = None,
    ) -> List[Dict]:
        """
        

        Args:
            level: 
            category: 
            limit: 

        Returns:
            
        """
        with self._lock:
            logs = list(self._logs)

        # （）
        logs.reverse()

        # 
        if level:
            logs = [log for log in logs if log.level == level]
        elif min_level:
            threshold = self._LEVEL_ORDER.get(min_level.upper(), 1)
            logs = [
                log for log in logs
                if self._LEVEL_ORDER.get(log.level.upper(), 1) >= threshold
            ]

        if category:
            logs = [log for log in logs if log.category == category]

        # 
        logs = logs[:limit]

        return [log.to_dict() for log in logs]

    def clear_logs(self):
        """"""
        with self._lock:
            self._logs.clear()

    def add_listener(self, callback):
        """WebSocket"""
        self._listeners.append(callback)

    def remove_listener(self, callback):
        """WebSocket"""
        if callback in self._listeners:
            self._listeners.remove(callback)

    def _notify_listeners(self, entry: LogEntry):
        """"""
        for callback in self._listeners:
            try:
                callback(entry.to_dict())
            except Exception as e:
                logging.error(f"Failed to notify log listener: {e}")

    def log_price_update(self, symbol: str, price: float, change_percent: Optional[float] = None):
        """"""
        details = {
            "symbol": symbol,
            "price": price
        }
        if change_percent is not None:
            details["change_percent"] = change_percent

        self.add_log(
            level="INFO",
            category="price_update",
            message=f"{symbol} price updated: ${price:.4f}",
            details=details
        )

    def log_ai_decision(
        self,
        account_name: str,
        model: str,
        operation: str,
        symbol: Optional[str],
        reason: str,
        success: bool = True
    ):
        """AI"""
        self.add_log(
            level="INFO" if success else "WARNING",
            category="ai_decision",
            message=f"[{account_name}] {operation.upper()} {symbol or 'N/A'}: {reason[:100]}",
            details={
                "account": account_name,
                "model": model,
                "operation": operation,
                "symbol": symbol,
                "reason": reason,
                "success": success
            }
        )

    def log_error(self, error_type: str, message: str, details: Optional[Dict] = None):
        """"""
        self.add_log(
            level="ERROR",
            category="system_error",
            message=f"[{error_type}] {message}",
            details=details or {}
        )

    def log_warning(self, warning_type: str, message: str, details: Optional[Dict] = None):
        """"""
        self.add_log(
            level="WARNING",
            category="system_error",
            message=f"[{warning_type}] {message}",
            details=details or {}
        )


# 
system_logger = SystemLogCollector(max_logs=500)


class SystemLogHandler(logging.Handler):
    """Python logging Handler，SystemLogCollector"""

    def emit(self, record: logging.LogRecord):
        """"""
        try:
            # 
            module = record.name
            level = record.levelname
            message = self.format(record)

            # 
            category = "system_error"
            if "price" in message.lower() or "market" in module:
                category = "price_update"
            elif "ai_decision" in module or "trading" in module:
                category = "ai_decision"

            # 
            details = {
                "module": module,
                "function": record.funcName,
                "line": record.lineno
            }

            # 
            if record.exc_info:
                import traceback
                details["exception"] = ''.join(traceback.format_exception(*record.exc_info))

            # WARNING,INFO
            if record.levelno >= logging.WARNING:
                system_logger.add_log(
                    level=level,
                    category=category,
                    message=message,
                    details=details
                )
            elif record.levelno == logging.INFO and "Strategy triggered" in message:
                # INFO
                system_logger.add_log(
                    level=level,
                    category="ai_decision",
                    message=message,
                    details=details
                )
            elif record.levelno == logging.INFO and "Strategy execution completed" in message:
                # INFO
                system_logger.add_log(
                    level=level,
                    category="ai_decision",
                    message=message,
                    details=details
                )
        except Exception as e:
            # 
            print(f"SystemLogHandler error: {e}")


class PriceSnapshotLogger:
    """60"""

    def __init__(self):
        self._timer: Optional[threading.Timer] = None
        self._interval = 60  # 60 seconds
        self._running = False
        self._last_prices: Dict[str, float] = {}

    def start(self):
        """"""
        if self._running:
            return
        self._running = True
        self._schedule_next()
        logging.info("Price snapshot logger started (60-second interval)")

    def stop(self):
        """"""
        self._running = False
        if self._timer:
            self._timer.cancel()
            self._timer = None
        logging.info("Price snapshot logger stopped")

    def _schedule_next(self):
        """"""
        if not self._running:
            return
        self._timer = threading.Timer(self._interval, self._take_snapshot)
        self._timer.daemon = True
        self._timer.start()

    def _take_snapshot(self):
        """"""
        try:
            from services.price_cache import get_cached_price
            from services.trading_commands import AI_TRADING_SYMBOLS

            prices_info = []
            for symbol in AI_TRADING_SYMBOLS:
                price = get_cached_price(symbol, "CRYPTO")
                if price is not None:
                    prices_info.append(f"{symbol}=${price:.4f}")
                    self._last_prices[symbol] = price

            if prices_info:
                message = "Price snapshot: " + ", ".join(prices_info)
                system_logger.add_log(
                    level="INFO",
                    category="price_update",
                    message=message,
                    details={"prices": self._last_prices.copy(), "symbols": AI_TRADING_SYMBOLS}
                )
        except Exception as e:
            logging.error(f"Failed to take price snapshot: {e}")
        finally:
            # 
            self._schedule_next()


# 
price_snapshot_logger = PriceSnapshotLogger()


def setup_system_logger():
    """（）"""
    handler = SystemLogHandler()
    handler.setLevel(logging.WARNING)  # WARNING

    # logger
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)

    logging.info("System log collector initialized")
