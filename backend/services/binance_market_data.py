"""
Binance market data service using CCXT.

Provides a normalized response format aligned with the existing
Hyperliquid market data service.
"""
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import ccxt

logger = logging.getLogger(__name__)


class BinanceClient:
    def __init__(self):
        self.exchange = None
        self._initialize_exchange()

    def _initialize_exchange(self):
        """Initialize CCXT Binance exchange (USD-M futures market)."""
        try:
            self.exchange = ccxt.binance(
                {
                    "enableRateLimit": True,
                    "options": {
                        "defaultType": "future",
                        "adjustForTimeDifference": True,
                    },
                }
            )

            try:
                self.exchange.load_markets()
                logger.info(
                    "CCXT Binance markets pre-loaded: %d markets",
                    len(self.exchange.markets),
                )
            except Exception as market_err:
                logger.warning(
                    "Failed to pre-load Binance markets (will lazy-load): %s",
                    market_err,
                )

            logger.info("Binance exchange initialized successfully")
        except Exception as e:
            logger.error("Failed to initialize Binance exchange: %s", e)
            raise

    def get_last_price(self, symbol: str) -> Optional[float]:
        """Get last traded price for symbol."""
        try:
            if not self.exchange:
                self._initialize_exchange()

            formatted_symbol = self._format_symbol(symbol)
            ticker = self.exchange.fetch_ticker(formatted_symbol)
            price = ticker.get("last")
            return float(price) if price else None
        except Exception as e:
            logger.error("Error fetching Binance price for %s: %s", symbol, e)
            return None

    def get_ticker_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get complete ticker data."""
        try:
            if not self.exchange:
                self._initialize_exchange()

            formatted_symbol = self._format_symbol(symbol)
            ticker = self.exchange.fetch_ticker(formatted_symbol)

            price = ticker.get("last")
            change = ticker.get("change")
            percentage = ticker.get("percentage")
            quote_volume = ticker.get("quoteVolume")
            base_volume = ticker.get("baseVolume")

            return {
                "symbol": symbol,
                "price": float(price) if price is not None else 0.0,
                "change24h": float(change) if change is not None else 0.0,
                "volume24h": float(quote_volume or base_volume or 0.0),
                "percentage24h": float(percentage) if percentage is not None else 0.0,
            }
        except Exception as e:
            logger.error("Error fetching Binance ticker for %s: %s", symbol, e)
            return None

    def get_kline_data(
        self,
        symbol: str,
        period: str = "1d",
        count: int = 100,
    ) -> List[Dict[str, Any]]:
        """Get kline/candlestick data for symbol."""
        try:
            if not self.exchange:
                self._initialize_exchange()

            formatted_symbol = self._format_symbol(symbol)
            timeframe = self._map_timeframe(period)

            ohlcv = self.exchange.fetch_ohlcv(formatted_symbol, timeframe, limit=count)

            klines: List[Dict[str, Any]] = []
            for candle in ohlcv:
                timestamp_ms = candle[0]
                open_price = candle[1]
                high_price = candle[2]
                low_price = candle[3]
                close_price = candle[4]
                volume = candle[5]

                change = close_price - open_price if open_price else 0
                percent = (change / open_price * 100) if open_price else 0

                klines.append(
                    {
                        "timestamp": int(timestamp_ms / 1000),
                        "datetime": datetime.fromtimestamp(
                            timestamp_ms / 1000, tz=timezone.utc
                        ).isoformat(),
                        "open": float(open_price) if open_price is not None else None,
                        "high": float(high_price) if high_price is not None else None,
                        "low": float(low_price) if low_price is not None else None,
                        "close": float(close_price) if close_price is not None else None,
                        "volume": float(volume) if volume is not None else None,
                        "amount": (
                            float(volume * close_price)
                            if volume is not None and close_price is not None
                            else None
                        ),
                        "chg": float(change),
                        "percent": float(percent),
                    }
                )

            return klines
        except Exception as e:
            logger.error("Error fetching Binance klines for %s: %s", symbol, e)
            return []

    def get_market_status(self, symbol: str) -> Dict[str, Any]:
        """Get market status for symbol."""
        try:
            if not self.exchange:
                self._initialize_exchange()

            formatted_symbol = self._format_symbol(symbol)
            markets = self.exchange.load_markets()
            market_info = markets.get(formatted_symbol)
            market_exists = market_info is not None

            status = {
                "market_status": "OPEN" if market_exists else "CLOSED",
                "is_trading": bool(market_exists and market_info.get("active", True)),
                "symbol": formatted_symbol,
                "exchange": "Binance",
                "market_type": "crypto",
                "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
                "current_time": datetime.now(timezone.utc).isoformat(),
            }

            if market_info:
                status.update(
                    {
                        "base_currency": market_info.get("base"),
                        "quote_currency": market_info.get("quote"),
                        "active": market_info.get("active", True),
                    }
                )

            return status
        except Exception as e:
            logger.error("Error getting Binance market status for %s: %s", symbol, e)
            return {
                "market_status": "ERROR",
                "is_trading": False,
                "error": str(e),
            }

    def get_all_symbols(self) -> List[str]:
        """Get Binance symbols, prioritizing liquid USDT pairs."""
        try:
            if not self.exchange:
                self._initialize_exchange()

            markets = self.exchange.load_markets()
            all_symbols = list(markets.keys())
            usdt_symbols = [
                s
                for s in all_symbols
                if "/USDT:USDT" in s and markets.get(s, {}).get("active", True)
            ]

            priority = [
                "BTC/USDT:USDT",
                "ETH/USDT:USDT",
                "SOL/USDT:USDT",
                "BNB/USDT:USDT",
                "XRP/USDT:USDT",
                "DOGE/USDT:USDT",
            ]
            prioritized = [s for s in priority if s in usdt_symbols]
            remaining = [s for s in usdt_symbols if s not in prioritized]

            return prioritized + remaining[:100]
        except Exception as e:
            logger.error("Error getting Binance symbols: %s", e)
            return ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT"]

    def _map_timeframe(self, period: str) -> str:
        timeframe_map = {
            "1m": "1m",
            "3m": "3m",
            "5m": "5m",
            "15m": "15m",
            "30m": "30m",
            "1h": "1h",
            "2h": "2h",
            "4h": "4h",
            "6h": "6h",
            "8h": "8h",
            "12h": "12h",
            "1d": "1d",
            "3d": "3d",
            "1w": "1w",
            "1M": "1M",
        }
        return timeframe_map.get(period, "1d")

    def _format_symbol(self, symbol: str) -> str:
        """
        Format incoming symbol to Binance CCXT format.
        Examples:
        - BTC -> BTC/USDT:USDT
        - BTCUSDT -> BTC/USDT:USDT
        - BTC/USDT -> BTC/USDT:USDT
        """
        s = symbol.upper().strip()

        if ":" in s:
            return s

        if "/" in s:
            base, quote = s.split("/", 1)
            return f"{base}/{quote}:{quote}"

        for quote in ["USDT", "USDC", "BUSD", "BTC", "ETH", "BNB"]:
            if s.endswith(quote) and len(s) > len(quote):
                base = s[: -len(quote)]
                return f"{base}/{quote}:{quote}"

        return f"{s}/USDT:USDT"


_binance_client: Optional[BinanceClient] = None


def get_binance_client() -> BinanceClient:
    global _binance_client
    if _binance_client is None:
        _binance_client = BinanceClient()
    return _binance_client


def get_last_price_from_binance(symbol: str) -> Optional[float]:
    return get_binance_client().get_last_price(symbol)


def get_ticker_data_from_binance(symbol: str) -> Optional[Dict[str, Any]]:
    return get_binance_client().get_ticker_data(symbol)


def get_kline_data_from_binance(
    symbol: str,
    period: str = "1d",
    count: int = 100,
) -> List[Dict[str, Any]]:
    return get_binance_client().get_kline_data(symbol, period, count)


def get_market_status_from_binance(symbol: str) -> Dict[str, Any]:
    return get_binance_client().get_market_status(symbol)


def get_all_symbols_from_binance() -> List[str]:
    return get_binance_client().get_all_symbols()
