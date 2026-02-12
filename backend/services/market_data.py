import time
import logging
from typing import Any, Dict, List, Optional

from .hyperliquid_market_data import (
    get_last_price_from_hyperliquid,
    get_kline_data_from_hyperliquid,
    get_market_status_from_hyperliquid,
    get_all_symbols_from_hyperliquid,
    get_ticker_data_from_hyperliquid,
)
from .binance_market_data import (
    get_last_price_from_binance,
    get_kline_data_from_binance,
    get_market_status_from_binance,
    get_all_symbols_from_binance,
    get_ticker_data_from_binance,
)

logger = logging.getLogger(__name__)

SUPPORTED_EXCHANGES = {"hyperliquid", "binance"}
_EXCHANGE_CACHE_TTL_SECONDS = 10
_exchange_pref_cache: Dict[str, Optional[float | str]] = {"value": None, "expires_at": 0.0}


def _get_selected_exchange() -> Optional[str]:
    """Get selected exchange from user config with short-lived cache."""
    now = time.time()
    cached_value = _exchange_pref_cache.get("value")
    cached_expiry = float(_exchange_pref_cache.get("expires_at") or 0.0)
    if cached_value and now < cached_expiry:
        return str(cached_value)

    try:
        from database.connection import SessionLocal
        from database.models import UserExchangeConfig

        with SessionLocal() as db:
            config = db.query(UserExchangeConfig).filter(UserExchangeConfig.user_id == 1).first()
            if config and config.selected_exchange in SUPPORTED_EXCHANGES:
                selected = config.selected_exchange
                _exchange_pref_cache["value"] = selected
                _exchange_pref_cache["expires_at"] = now + _EXCHANGE_CACHE_TTL_SECONDS
                return selected
    except Exception as err:
        logger.debug("Failed to load selected exchange config: %s", err)

    _exchange_pref_cache["value"] = None
    _exchange_pref_cache["expires_at"] = now + _EXCHANGE_CACHE_TTL_SECONDS
    return None


def _resolve_exchange(market: str) -> str:
    """Resolve incoming market string to an implemented exchange."""
    token = (market or "").strip().lower()

    if token in SUPPORTED_EXCHANGES:
        return token

    if token == "aster":
        logger.warning("Exchange 'aster' is not implemented yet; falling back to Hyperliquid")
        return "hyperliquid"

    if token in {"", "crypto", "us"}:
        selected = _get_selected_exchange()
        if selected in SUPPORTED_EXCHANGES:
            return selected

    return "hyperliquid"


def _exchange_display_name(exchange: str) -> str:
    if exchange == "binance":
        return "Binance"
    return "Hyperliquid"


def get_last_price(symbol: str, market: str = "CRYPTO", environment: str = "mainnet") -> float:
    exchange = _resolve_exchange(market)
    key = f"{symbol}.{exchange}.{environment}"
    exchange_label = _exchange_display_name(exchange)

    # Check cache first (environment-specific)
    from .price_cache import get_cached_price, cache_price
    cached_price = get_cached_price(symbol, exchange, environment)
    if cached_price is not None:
        logger.debug(f"Using cached price for {key}: {cached_price}")
        return cached_price

    logger.info(f"Getting real-time price for {key} from API ({environment})...")

    try:
        if exchange == "binance":
            price = get_last_price_from_binance(symbol)
        else:
            price = get_last_price_from_hyperliquid(symbol, environment)

        if price and price > 0:
            logger.info(f"Got real-time price for {key} from {exchange_label} ({environment}): {price}")
            # Cache the price (environment-specific)
            cache_price(symbol, exchange, price, environment)
            return price
        raise Exception(f"{exchange_label} returned invalid price: {price}")
    except Exception as err:
        logger.error(f"Failed to get price from {exchange_label} ({environment}): {err}")
        raise Exception(f"Unable to get real-time price for {key}: {err}")


def get_kline_data(symbol: str, market: str = "CRYPTO", period: str = "1d", count: int = 100, environment: str = "mainnet", persist: bool = True) -> List[Dict[str, Any]]:
    exchange = _resolve_exchange(market)
    key = f"{symbol}.{exchange}.{environment}"
    exchange_label = _exchange_display_name(exchange)

    try:
        if exchange == "binance":
            data = get_kline_data_from_binance(symbol, period, count)
        else:
            data = get_kline_data_from_hyperliquid(
                symbol,
                period,
                count,
                persist=persist,
                environment=environment,
            )

        if data:
            logger.info(
                "Got K-line data for %s from %s (%s), total %d items",
                key,
                exchange_label,
                environment,
                len(data),
            )
            return data
        raise Exception(f"{exchange_label} returned empty K-line data")
    except Exception as err:
        logger.error(f"Failed to get K-line data from {exchange_label} ({environment}): {err}")
        raise Exception(f"Unable to get K-line data for {key}: {err}")


def get_market_status(symbol: str, market: str = "CRYPTO") -> Dict[str, Any]:
    exchange = _resolve_exchange(market)
    key = f"{symbol}.{exchange}"
    exchange_label = _exchange_display_name(exchange)

    try:
        if exchange == "binance":
            status = get_market_status_from_binance(symbol)
        else:
            status = get_market_status_from_hyperliquid(symbol)

        logger.info(f"Retrieved market status for {key} from {exchange_label}: {status.get('market_status')}")
        return status
    except Exception as err:
        logger.error(f"Failed to get market status from {exchange_label}: {err}")
        raise Exception(f"Unable to get market status for {key}: {err}")


def get_all_symbols() -> List[str]:
    """Get all available trading pairs"""
    exchange = _get_selected_exchange() or "hyperliquid"
    exchange_label = _exchange_display_name(exchange)

    try:
        if exchange == "binance":
            symbols = get_all_symbols_from_binance()
        else:
            symbols = get_all_symbols_from_hyperliquid()

        logger.info("Got %d trading pairs from %s", len(symbols), exchange_label)
        return symbols
    except Exception as err:
        logger.error(f"Failed to get trading pairs list from {exchange_label}: {err}")
        if exchange == "binance":
            return ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
        return ["BTC/USD", "ETH/USD", "SOL/USD"]


def get_ticker_data(symbol: str, market: str = "CRYPTO", environment: str = "mainnet") -> Dict[str, Any]:
    """Get complete ticker data including 24h change and volume"""
    exchange = _resolve_exchange(market)
    key = f"{symbol}.{exchange}.{environment}"
    exchange_label = _exchange_display_name(exchange)

    try:
        if exchange == "binance":
            ticker_data = get_ticker_data_from_binance(symbol)
        else:
            ticker_data = get_ticker_data_from_hyperliquid(symbol, environment)

        if ticker_data:
            logger.info(
                "Got ticker data for %s from %s: price=%s, change24h=%s",
                key,
                exchange_label,
                ticker_data["price"],
                ticker_data["change24h"],
            )
            return ticker_data
        raise Exception(f"{exchange_label} returned empty ticker data")
    except Exception as err:
        logger.error(f"Failed to get ticker data from {exchange_label} ({environment}): {err}")
        # Fallback to price-only data
        try:
            price = get_last_price(symbol, exchange, environment)
            fallback_data = {
                "symbol": symbol,
                "price": price,
                "change24h": 0,
                "volume24h": 0,
                "percentage24h": 0,
            }
            logger.info("Returning fallback ticker data for %s: %s", key, fallback_data)
            return fallback_data
        except Exception:
            raise Exception(f"Unable to get ticker data for {key}: {err}")
