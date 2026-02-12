"""
K - 
"""

from abc import ABC, abstractmethod
from typing import List, Optional
from datetime import datetime
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class KlineData:
    """K"""
    exchange: str
    symbol: str
    timestamp: int  # Unix timestamp in seconds
    period: str     # "1m", "5m", "1h", etc.
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float


class BaseKlineCollector(ABC):
    """K - """

    def __init__(self, exchange_id: str):
        self.exchange_id = exchange_id
        self.logger = logging.getLogger(f"{__name__}.{exchange_id}")

    @abstractmethod
    async def fetch_current_kline(self, symbol: str, period: str = "1m") -> Optional[KlineData]:
        """K"""
        pass

    @abstractmethod
    async def fetch_historical_klines(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        period: str = "1m"
    ) -> List[KlineData]:
        """K"""
        pass

    @abstractmethod
    def get_supported_symbols(self) -> List[str]:
        """"""
        pass


class HyperliquidKlineCollector(BaseKlineCollector):
    """Hyperliquid K"""

    def __init__(self):
        super().__init__("hyperliquid")
        #  hyperliquid_market_data 
        from .hyperliquid_market_data import HyperliquidClient
        self.market_data = HyperliquidClient()

    async def fetch_current_kline(self, symbol: str, period: str = "1m") -> Optional[KlineData]:
        """K"""
        try:
            # K (， await)
            klines = self.market_data.get_kline_data(symbol, period, count=1)
            if not klines:
                return None

            latest = klines[0]
            return KlineData(
                exchange=self.exchange_id,
                symbol=symbol,
                timestamp=int(latest['timestamp']),
                period=period,
                open_price=float(latest['open']),
                high_price=float(latest['high']),
                low_price=float(latest['low']),
                close_price=float(latest['close']),
                volume=float(latest['volume'])
            )
        except Exception as e:
            self.logger.error(f"Failed to fetch current kline for {symbol}: {e}")
            return None

    async def fetch_historical_klines(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        period: str = "1m"
    ) -> List[KlineData]:
        """K"""
        try:
            # 
            time_diff = end_time - start_time
            if period == "1m":
                limit = int(time_diff.total_seconds() / 60)
            else:
                # 
                limit = 1000  # 

            #  (，await)
            klines = self.market_data.get_kline_data(
                symbol, period, count=min(limit, 5000)
            )

            result = []
            for kline in klines:
                kline_time = datetime.fromtimestamp(kline['timestamp'])
                if start_time <= kline_time <= end_time:
                    result.append(KlineData(
                        exchange=self.exchange_id,
                        symbol=symbol,
                        timestamp=int(kline['timestamp']),
                        period=period,
                        open_price=float(kline['open']),
                        high_price=float(kline['high']),
                        low_price=float(kline['low']),
                        close_price=float(kline['close']),
                        volume=float(kline['volume'])
                    ))

            return result
        except Exception as e:
            self.logger.error(f"Failed to fetch historical klines for {symbol}: {e}")
            return []

    def get_supported_symbols(self) -> List[str]:
        """Watch List（）"""
        try:
            from .hyperliquid_symbol_service import get_selected_symbols
            symbols = get_selected_symbols()
            if symbols:
                return symbols
        except Exception as e:
            self.logger.warning(f"Failed to get symbols from hyperliquid_symbol_service: {e}")

        # 
        return ["BTC", "ETH", "SOL", "BNB"]


class BinanceKlineCollector(BaseKlineCollector):
    """Binance K"""

    def __init__(self):
        super().__init__("binance")
        from .binance_market_data import BinanceClient

        self.market_data = BinanceClient()

    async def fetch_current_kline(self, symbol: str, period: str = "1m") -> Optional[KlineData]:
        try:
            klines = self.market_data.get_kline_data(symbol, period, count=1)
            if not klines:
                return None

            latest = klines[0]
            return KlineData(
                exchange=self.exchange_id,
                symbol=symbol,
                timestamp=int(latest["timestamp"]),
                period=period,
                open_price=float(latest["open"]),
                high_price=float(latest["high"]),
                low_price=float(latest["low"]),
                close_price=float(latest["close"]),
                volume=float(latest["volume"]),
            )
        except Exception as e:
            self.logger.error(f"Failed to fetch current Binance kline for {symbol}: {e}")
            return None

    async def fetch_historical_klines(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        period: str = "1m"
    ) -> List[KlineData]:
        try:
            time_diff = end_time - start_time
            if period == "1m":
                limit = int(time_diff.total_seconds() / 60)
            else:
                limit = 1000

            klines = self.market_data.get_kline_data(symbol, period, count=min(limit, 5000))
            result = []
            for kline in klines:
                kline_time = datetime.fromtimestamp(kline["timestamp"])
                if start_time <= kline_time <= end_time:
                    result.append(
                        KlineData(
                            exchange=self.exchange_id,
                            symbol=symbol,
                            timestamp=int(kline["timestamp"]),
                            period=period,
                            open_price=float(kline["open"]),
                            high_price=float(kline["high"]),
                            low_price=float(kline["low"]),
                            close_price=float(kline["close"]),
                            volume=float(kline["volume"]),
                        )
                    )

            return result
        except Exception as e:
            self.logger.error(f"Failed to fetch historical Binance klines for {symbol}: {e}")
            return []

    def get_supported_symbols(self) -> List[str]:
        try:
            symbols = self.market_data.get_all_symbols()
            if symbols:
                return symbols[:100]
        except Exception as e:
            self.logger.warning(f"Failed to load Binance symbol list: {e}")

        return ["BTC/USDT", "ETH/USDT", "SOL/USDT"]


class AsterKlineCollector(BaseKlineCollector):
    """Aster DEX K - """

    def __init__(self):
        super().__init__("aster")

    async def fetch_current_kline(self, symbol: str, period: str = "1m") -> Optional[KlineData]:
        # TODO: Aster API
        self.logger.warning("Aster collector not implemented yet")
        return None

    async def fetch_historical_klines(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        period: str = "1m"
    ) -> List[KlineData]:
        # TODO: Aster
        self.logger.warning("Aster historical data not implemented yet")
        return []

    def get_supported_symbols(self) -> List[str]:
        return ["BTC/USDT", "ETH/USDT"]  # 


class ExchangeDataSourceFactory:
    """ - """

    _collectors = {
        "hyperliquid": HyperliquidKlineCollector,
        "binance": BinanceKlineCollector,
        "aster": AsterKlineCollector
    }

    @classmethod
    def get_collector(cls, exchange_id: str) -> BaseKlineCollector:
        """ID"""
        if exchange_id not in cls._collectors:
            raise ValueError(f"Unsupported exchange: {exchange_id}")

        collector_class = cls._collectors[exchange_id]
        return collector_class()

    @classmethod
    def get_supported_exchanges(cls) -> List[str]:
        """"""
        return list(cls._collectors.keys())
