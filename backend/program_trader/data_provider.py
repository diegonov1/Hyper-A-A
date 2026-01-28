"""
Data provider for Program Trader.
Connects to existing data services (klines, indicators, flow, regime).
"""

from typing import Dict, List, Any, Optional
from sqlalchemy.orm import Session

from .models import Kline, Position, Trade, RegimeInfo, Order


class DataProvider:
    """
    Provides market data to strategy scripts.
    Wraps existing data services for unified access.
    """

    def __init__(
        self,
        db: Session,
        account_id: int,
        environment: str = "mainnet",
        trading_client: Any = None,
        record_queries: bool = False
    ):
        self.db = db
        self.account_id = account_id
        self.environment = environment
        self.trading_client = trading_client
        self.record_queries = record_queries
        self._query_log: List[Dict[str, Any]] = []
        self._kline_cache: Dict[str, List[Kline]] = {}
        self._account_cache: Optional[Dict[str, Any]] = None
        self._positions_cache: Optional[Dict[str, Position]] = None
        self._open_orders_cache: Optional[List[Order]] = None
        self._recent_trades_cache: Optional[List[Trade]] = None

    def _log_query(self, method: str, args: Dict[str, Any], result: Any) -> None:
        """Record a data query for preview run debugging."""
        if self.record_queries:
            self._query_log.append({
                "method": method,
                "args": args,
                "result": result
            })

    def get_query_log(self) -> List[Dict[str, Any]]:
        """Get all recorded data queries."""
        return self._query_log

    def get_price_change(self, symbol: str, period: str) -> Dict[str, float]:
        """Get price change for symbol over period.

        Returns:
            Dict with change_percent (percentage) and change_usd (absolute USD change)
        """
        from services.market_flow_indicators import get_flow_indicators_for_prompt
        import time

        current_time_ms = int(time.time() * 1000)
        result = {"change_percent": 0.0, "change_usd": 0.0}
        try:
            # Use get_flow_indicators_for_prompt to get full data structure
            # _get_price_change_data returns: {current, start_price, end_price, last_5, period}
            results = get_flow_indicators_for_prompt(
                self.db, symbol, period, ["PRICE_CHANGE"], current_time_ms
            )
            data = results.get("PRICE_CHANGE")
            if data:
                change_pct = data.get("current", 0.0)
                start_price = data.get("start_price", 0.0)
                end_price = data.get("end_price", 0.0)
                change_usd = (end_price - start_price) if start_price and end_price else 0.0
                result = {
                    "change_percent": change_pct,
                    "change_usd": change_usd,
                }
        except Exception:
            pass
        self._log_query("get_price_change", {"symbol": symbol, "period": period}, result)
        return result

    def get_klines(self, symbol: str, period: str, count: int = 50) -> List[Kline]:
        """Get K-line data from Hyperliquid API (real-time).

        Uses the same data source as AI Trader's {BTC_klines_15m} variable.
        Always fetches fresh data from Hyperliquid API, not from database.
        """
        from services.market_data import get_kline_data

        cache_key = f"{symbol}_{period}_{count}"
        if cache_key in self._kline_cache:
            klines = self._kline_cache[cache_key]
            self._log_query("get_klines", {"symbol": symbol, "period": period, "count": count},
                           {"count": len(klines), "cached": True})
            return klines

        klines = []
        try:
            # Use same API as AI Trader: get_kline_data() -> get_kline_data_from_hyperliquid()
            # Fetch more candles for indicator calculation, return requested count
            fetch_count = max(count, 100)  # At least 100 for indicator accuracy
            raw_data = get_kline_data(
                symbol=symbol,
                market="CRYPTO",
                period=period,
                count=fetch_count,
                environment=self.environment,
                persist=False  # Don't write to DB, real-time only
            )
            if raw_data:
                # Convert to Kline objects, take last 'count' candles
                all_klines = [
                    Kline(
                        timestamp=int(row.get('timestamp', 0)),
                        open=float(row.get('open', 0)),
                        high=float(row.get('high', 0)),
                        low=float(row.get('low', 0)),
                        close=float(row.get('close', 0)),
                        volume=float(row.get('volume', 0)),
                    )
                    for row in raw_data
                ]
                klines = all_klines[-count:] if len(all_klines) > count else all_klines
                # Cache the full fetch for indicator calculation reuse
                self._kline_cache[f"{symbol}_{period}_raw"] = raw_data
                self._kline_cache[cache_key] = klines
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"get_klines failed for {symbol} {period}: {e}")
        self._log_query("get_klines", {"symbol": symbol, "period": period, "count": count},
                       {"count": len(klines)})
        return klines

    def get_indicator(self, symbol: str, indicator: str, period: str) -> Dict[str, Any]:
        """Get technical indicator values based on real-time K-line data.

        Uses the same calculation flow as AI Trader:
        1. Fetch real-time K-line data from Hyperliquid API
        2. Calculate indicator using calculate_indicators()

        This ensures Programs and AI Trader see identical indicator values.
        """
        from services.market_data import get_kline_data
        from services.technical_indicators import calculate_indicators
        import logging

        logger = logging.getLogger(__name__)
        result = {}
        try:
            # Check if we have cached raw kline data from get_klines()
            raw_cache_key = f"{symbol}_{period}_raw"
            if raw_cache_key in self._kline_cache:
                kline_data = self._kline_cache[raw_cache_key]
            else:
                # Fetch real-time K-line data (same as AI Trader)
                # Use 500 candles for accurate indicator calculation
                kline_data = get_kline_data(
                    symbol=symbol,
                    market="CRYPTO",
                    period=period,
                    count=500,
                    environment=self.environment,
                    persist=False
                )
                if kline_data:
                    self._kline_cache[raw_cache_key] = kline_data

            if not kline_data:
                logger.warning(f"No kline data for indicator {indicator} on {symbol} {period}")
                self._log_query("get_indicator", {"symbol": symbol, "indicator": indicator, "period": period}, result)
                return result

            # Calculate indicator using same function as AI Trader
            indicator_upper = indicator.upper()
            calculated = calculate_indicators(kline_data, [indicator_upper])

            if indicator_upper in calculated and calculated[indicator_upper] is not None:
                value = calculated[indicator_upper]
                # Return the latest value(s) - same format as old calculate_indicator()
                if isinstance(value, list):
                    result = {'value': value[-1] if value else None, 'series': value}
                elif isinstance(value, dict):
                    # For MACD, BOLL, STOCH etc. - return latest values
                    latest = {}
                    for k, v in value.items():
                        if isinstance(v, list) and v:
                            latest[k] = v[-1]
                        else:
                            latest[k] = v
                    result = latest
                else:
                    result = {'value': value}
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"get_indicator failed for {symbol} {indicator} {period}: {e}")
        self._log_query("get_indicator", {"symbol": symbol, "indicator": indicator, "period": period}, result)
        return result

    def get_flow(self, symbol: str, metric: str, period: str) -> Dict[str, Any]:
        """Get market flow metrics (CVD, OI, TAKER, etc.).

        Returns full data structure including current value, last_5 history, etc.
        Example for CVD: {current: float, last_5: list, cumulative: float, period: str}
        Example for TAKER: {buy: float, sell: float, ratio: float, ratio_last_5: list, ...}
        """
        from services.market_flow_indicators import get_flow_indicators_for_prompt
        import time

        current_time_ms = int(time.time() * 1000)
        result = {}
        try:
            # Use get_flow_indicators_for_prompt to get full data structure
            results = get_flow_indicators_for_prompt(
                self.db, symbol, period, [metric.upper()], current_time_ms
            )
            result = results.get(metric.upper(), {}) or {}
        except Exception:
            pass
        self._log_query("get_flow", {"symbol": symbol, "metric": metric, "period": period}, result)
        return result

    def get_regime(self, symbol: str, period: str) -> RegimeInfo:
        """Get market regime classification using real-time data.

        Uses the same parameters as AI Trader: use_realtime=True ensures
        fresh market regime calculation instead of cached/historical data.
        """
        from services.market_regime_service import get_market_regime

        regime_info = RegimeInfo(regime="noise", conf=0.0)
        try:
            # Use use_realtime=True to match AI Trader behavior
            result = get_market_regime(self.db, symbol, period, use_realtime=True)
            if result:
                regime_info = RegimeInfo(
                    regime=result.get("regime", "noise"),
                    conf=result.get("confidence", 0.0),
                    direction=result.get("direction", "neutral"),
                    reason=result.get("reason", ""),
                    indicators=result.get("indicators", {}),
                )
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(f"get_regime failed for {symbol} {period}: {e}")
        self._log_query("get_regime", {"symbol": symbol, "period": period}, {
            "regime": regime_info.regime,
            "conf": regime_info.conf,
            "direction": regime_info.direction
        })
        return regime_info

    def get_account_info(self) -> Dict[str, Any]:
        """Get account balance and margin info from trading client."""
        if self._account_cache is not None:
            return self._account_cache

        if not self.trading_client:
            # Fallback for backtest or when no trading client
            return {
                "available_balance": 10000.0,
                "total_equity": 10000.0,
                "used_margin": 0.0,
                "margin_usage_percent": 0.0,
                "maintenance_margin": 0.0,
            }

        try:
            state = self.trading_client.get_account_state(self.db)
            self._account_cache = {
                "available_balance": state.get("available_balance", 0.0),
                "total_equity": state.get("total_equity", 0.0),
                "used_margin": state.get("used_margin", 0.0),
                "margin_usage_percent": state.get("margin_usage_percent", 0.0),
                "maintenance_margin": state.get("maintenance_margin", 0.0),
            }
            return self._account_cache
        except Exception:
            return {
                "available_balance": 0.0,
                "total_equity": 0.0,
                "used_margin": 0.0,
                "margin_usage_percent": 0.0,
                "maintenance_margin": 0.0,
            }

    def get_positions(self) -> Dict[str, Position]:
        """Get current open positions from trading client."""
        if self._positions_cache is not None:
            return self._positions_cache

        if not self.trading_client:
            return {}

        try:
            # Request position timing information (aligned with AI Trader)
            raw_positions = self.trading_client.get_positions(self.db, include_timing=True)
            positions = {}
            for pos in raw_positions:
                # HyperliquidTradingClient returns 'coin', not 'symbol'
                symbol = pos.get("coin") or pos.get("symbol", "")
                if not symbol:
                    continue
                # Map field names: szi->size, entry_px->entry_price, etc.
                size = abs(float(pos.get("szi", 0) or pos.get("size", 0)))
                positions[symbol] = Position(
                    symbol=symbol,
                    side=pos.get("side", "long").lower(),
                    size=size,
                    entry_price=float(pos.get("entry_px", 0) or pos.get("entry_price", 0)),
                    unrealized_pnl=float(pos.get("unrealized_pnl", 0)),
                    leverage=int(float(pos.get("leverage", 1) or 1)),
                    liquidation_price=float(pos.get("liquidation_px", 0) or pos.get("liquidation_price", 0)),
                    # Position timing information
                    opened_at=pos.get("opened_at"),
                    opened_at_str=pos.get("opened_at_str"),
                    holding_duration_seconds=pos.get("holding_duration_seconds"),
                    holding_duration_str=pos.get("holding_duration_str"),
                )
            self._positions_cache = positions
            return positions
        except Exception:
            return {}

    def get_recent_trades(self, limit: int = 5) -> List[Trade]:
        """Get recent closed trades from trading client."""
        if self._recent_trades_cache is not None:
            return self._recent_trades_cache

        if not self.trading_client:
            return []

        try:
            raw_trades = self.trading_client.get_recent_closed_trades(self.db, limit)
            trades = []
            for t in raw_trades:
                trades.append(Trade(
                    symbol=t.get("symbol", ""),
                    side=t.get("side", ""),
                    size=float(t.get("size", 0)),
                    price=float(t.get("close_price", 0)),
                    timestamp=int(t.get("close_timestamp", 0)),
                    pnl=float(t.get("realized_pnl", 0)),
                    close_time=t.get("close_time", ""),
                ))
            self._recent_trades_cache = trades
            return trades
        except Exception:
            return []

    def get_open_orders(self, symbol: Optional[str] = None) -> List[Order]:
        """Get current open orders from trading client."""
        if self._open_orders_cache is not None:
            if symbol:
                return [o for o in self._open_orders_cache if o.symbol == symbol]
            return self._open_orders_cache

        if not self.trading_client:
            return []

        try:
            raw_orders = self.trading_client.get_open_orders(self.db, symbol)
            orders = []
            for o in raw_orders:
                orders.append(Order(
                    order_id=int(o.get("order_id", 0)),
                    symbol=o.get("symbol", ""),
                    side=o.get("side", ""),
                    direction=o.get("direction", ""),
                    order_type=o.get("order_type", ""),
                    size=float(o.get("size", 0)),
                    price=float(o.get("price", 0)),
                    trigger_price=float(o.get("trigger_price")) if o.get("trigger_price") else None,
                    reduce_only=o.get("reduce_only", False),
                    timestamp=int(o.get("timestamp", 0)),
                ))
            self._open_orders_cache = orders
            if symbol:
                return [o for o in orders if o.symbol == symbol]
            return orders
        except Exception:
            return []


    def get_market_data(self, symbol: str) -> Dict[str, Any]:
        """Get complete market data for a symbol (price, volume, OI, funding rate).

        Reuses the same data layer as AI Trader's {BTC_market_data} variable.
        Returns dict with fields: symbol, price, oracle_price, change24h, percentage24h,
        volume24h, open_interest, funding_rate.
        """
        from services.market_data import get_ticker_data

        # Check cache first
        cache_key = f"market_data_{symbol}"
        if not hasattr(self, '_market_data_cache'):
            self._market_data_cache = {}

        if cache_key in self._market_data_cache:
            result = self._market_data_cache[cache_key]
            self._log_query("get_market_data", {"symbol": symbol}, {"cached": True, **result})
            return result

        # Call the same function AI Trader uses
        try:
            result = get_ticker_data(symbol, "CRYPTO", self.environment)
            if result:
                self._market_data_cache[cache_key] = result
                self._log_query("get_market_data", {"symbol": symbol}, result)
                return result
        except Exception as e:
            self._log_query("get_market_data", {"symbol": symbol}, {"error": str(e)})

        return {}
