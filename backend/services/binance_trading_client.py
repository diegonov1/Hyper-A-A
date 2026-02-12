"""
Binance Trading Client - authenticated trading execution via Binance USD-M Futures REST API.

This adapter exposes a subset of the Hyperliquid client interface so existing
execution flows can reuse the same method names:
- get_account_state
- get_positions
- get_open_orders
- place_order_with_tpsl
- cancel_order
- get_recent_closed_trades
"""
import hashlib
import hmac
import logging
import threading
import time
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import requests
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class BinanceApiError(Exception):
    """Raised when Binance REST API returns an error."""

    def __init__(
        self,
        code: Optional[int],
        msg: str,
        http_status: Optional[int] = None,
        payload: Optional[Any] = None,
    ):
        self.code = code
        self.msg = msg
        self.http_status = http_status
        self.payload = payload
        if code is not None:
            super().__init__(f'binance {{"code":{code},"msg":"{msg}"}}')
        else:
            super().__init__(f"binance {msg}")


class BinanceTradingClient:
    """
    Binance USD-M futures trading client via official REST endpoints.

    Environment:
    - "mainnet": live Binance futures
    - "testnet": Binance futures testnet
    """

    MAINNET_BASE_URL = "https://fapi.binance.com"
    TESTNET_BASE_URL = "https://testnet.binancefuture.com"
    DEFAULT_RECV_WINDOW_MS = 5000
    DEFAULT_TIMEOUT_SECONDS = 15
    TIME_SYNC_INTERVAL_SECONDS = 300
    DEFAULT_RECENT_TRADE_SYMBOLS = (
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT",
        "BNBUSDT",
        "XRPUSDT",
        "DOGEUSDT",
    )

    def __init__(
        self,
        account_id: int,
        api_key: str,
        api_secret: str,
        environment: str = "mainnet",
    ):
        if environment not in ["mainnet", "testnet"]:
            raise ValueError("Invalid environment. Must be 'mainnet' or 'testnet'")

        self.account_id = account_id
        self.environment = environment
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = (
            self.TESTNET_BASE_URL if environment == "testnet" else self.MAINNET_BASE_URL
        )
        self.recv_window_ms = self.DEFAULT_RECV_WINDOW_MS
        self.request_timeout = self.DEFAULT_TIMEOUT_SECONDS

        self._http = requests.Session()
        self._isolated_mode_initialized: set[str] = set()
        self._symbol_filters: Dict[str, Dict[str, str]] = {}
        self._time_offset_ms = 0
        self._last_time_sync_at = 0.0

        self._initialize_client()

    def _initialize_client(self) -> None:
        try:
            self._sync_server_time(force=True)
            self._load_exchange_info()
            logger.info(
                "Binance trading client initialized: account_id=%s env=%s symbols=%s",
                self.account_id,
                self.environment,
                len(self._symbol_filters),
            )
        except Exception as e:
            logger.error("Failed to initialize Binance trading client: %s", e)
            raise

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        try:
            if value is None:
                return default
            return float(value)
        except Exception:
            return default

    def _safe_int(self, value: Any, default: int = 0) -> int:
        try:
            if value is None:
                return default
            return int(float(value))
        except Exception:
            return default

    def _timestamp_ms(self) -> int:
        return int(time.time() * 1000) + int(self._time_offset_ms)

    def _ensure_time_sync(self) -> None:
        if time.time() - self._last_time_sync_at >= self.TIME_SYNC_INTERVAL_SECONDS:
            self._sync_server_time(force=False)

    def _sync_server_time(self, force: bool = False) -> None:
        if not force and (time.time() - self._last_time_sync_at) < 10:
            return

        try:
            payload = self._request(
                "GET",
                "/fapi/v1/time",
                signed=False,
                retry_on_time_sync=False,
            )
            server_time = self._safe_int(payload.get("serverTime"), 0)
            local_time = int(time.time() * 1000)
            if server_time > 0:
                self._time_offset_ms = server_time - local_time
                self._last_time_sync_at = time.time()
        except Exception as err:
            logger.warning("Failed to sync Binance server time: %s", err)

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        *,
        signed: bool,
        retry_on_time_sync: bool = True,
    ) -> Any:
        params = params or {}
        clean_params: Dict[str, Any] = {}
        for key, value in params.items():
            if value is None:
                continue
            if isinstance(value, bool):
                clean_params[key] = "true" if value else "false"
            else:
                clean_params[key] = value

        if signed:
            self._ensure_time_sync()
            clean_params.setdefault("recvWindow", self.recv_window_ms)
            clean_params["timestamp"] = self._timestamp_ms()

        query = urlencode(clean_params, doseq=True)
        if signed:
            signature = hmac.new(
                self.api_secret.encode("utf-8"),
                query.encode("utf-8"),
                hashlib.sha256,
            ).hexdigest()
            query = f"{query}&signature={signature}" if query else f"signature={signature}"

        url = f"{self.base_url}{path}"
        if query:
            url = f"{url}?{query}"

        headers = {}
        if signed:
            headers["X-MBX-APIKEY"] = self.api_key

        try:
            response = self._http.request(
                method=method,
                url=url,
                headers=headers,
                timeout=self.request_timeout,
            )
        except requests.RequestException as err:
            raise BinanceApiError(code=None, msg=str(err)) from err

        payload: Any
        try:
            payload = response.json()
        except ValueError:
            payload = {"msg": response.text}

        code = None
        msg = None
        if isinstance(payload, dict):
            code = payload.get("code")
            msg = payload.get("msg")

        if response.status_code >= 400:
            raise BinanceApiError(
                code=self._safe_int(code) if code is not None else None,
                msg=str(msg or payload),
                http_status=response.status_code,
                payload=payload,
            )

        if code is not None and self._safe_int(code) < 0:
            err = BinanceApiError(
                code=self._safe_int(code),
                msg=str(msg or "Unknown Binance error"),
                http_status=response.status_code,
                payload=payload,
            )
            if signed and retry_on_time_sync and err.code == -1021:
                self._sync_server_time(force=True)
                return self._request(
                    method,
                    path,
                    params=params,
                    signed=signed,
                    retry_on_time_sync=False,
                )
            raise err

        return payload

    def _load_exchange_info(self) -> None:
        payload = self._request("GET", "/fapi/v1/exchangeInfo", signed=False)
        symbols = payload.get("symbols", []) if isinstance(payload, dict) else []
        loaded: Dict[str, Dict[str, str]] = {}

        for symbol_info in symbols:
            symbol = str(symbol_info.get("symbol") or "").upper()
            if not symbol:
                continue

            filters = {}
            for f in symbol_info.get("filters", []) or []:
                filter_type = str(f.get("filterType") or "")
                if filter_type:
                    filters[filter_type] = f

            lot_size = filters.get("LOT_SIZE", {})
            price_filter = filters.get("PRICE_FILTER", {})
            notional_filter = filters.get("NOTIONAL", {})
            min_notional_filter = filters.get("MIN_NOTIONAL", {})

            loaded[symbol] = {
                "status": str(symbol_info.get("status") or ""),
                "step_size": str(lot_size.get("stepSize") or "0"),
                "min_qty": str(lot_size.get("minQty") or "0"),
                "tick_size": str(price_filter.get("tickSize") or "0"),
                "min_notional": str(
                    notional_filter.get("notional")
                    or notional_filter.get("minNotional")
                    or min_notional_filter.get("notional")
                    or min_notional_filter.get("minNotional")
                    or "0"
                ),
            }

        self._symbol_filters = loaded

    def _format_symbol(self, symbol: str) -> str:
        """
        Normalize symbol to Binance futures symbol format.
        Examples:
        - BTC -> BTCUSDT
        - BTCUSDT -> BTCUSDT
        - BTC/USDT -> BTCUSDT
        - BTC/USDT:USDT -> BTCUSDT
        """
        if not symbol:
            return "BTCUSDT"

        value = symbol.upper().strip()

        if ":" in value:
            value = value.split(":", 1)[0]

        if "/" in value:
            base, quote = value.split("/", 1)
            return f"{base}{quote}"

        if value.endswith(("USDT", "BUSD", "USDC")):
            return value

        return f"{value}USDT"

    def _extract_coin(self, exchange_symbol: str) -> str:
        if not exchange_symbol:
            return ""

        value = exchange_symbol.upper().split(":", 1)[0]
        if "/" in value:
            return value.split("/", 1)[0]

        for quote in ("USDT", "BUSD", "USDC"):
            if value.endswith(quote) and len(value) > len(quote):
                return value[: -len(quote)]

        return value

    def _map_time_in_force(self, time_in_force: Optional[str]) -> str:
        token = (time_in_force or "Ioc").strip().lower()
        if token == "gtc":
            return "GTC"
        if token == "alo":
            return "GTX"  # Post-only on Binance futures
        return "IOC"

    def _get_symbol_filter(self, symbol: str, key: str) -> Optional[str]:
        return (self._symbol_filters.get(symbol) or {}).get(key)

    def _decimal_to_str(self, value: Decimal) -> str:
        text = format(value, "f")
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return text or "0"

    def _round_to_step(self, value: Decimal, step: Decimal) -> Decimal:
        if step <= 0:
            return value
        steps = (value / step).to_integral_value(rounding=ROUND_DOWN)
        return steps * step

    def _normalize_quantity(self, symbol: str, quantity: float) -> str:
        qty = Decimal(str(abs(quantity)))
        if qty <= 0:
            raise ValueError("Order size must be greater than 0")

        step_size = self._get_symbol_filter(symbol, "step_size")
        min_qty = self._get_symbol_filter(symbol, "min_qty")

        if step_size and step_size not in ("0", "0.0"):
            qty = self._round_to_step(qty, Decimal(step_size))

        if min_qty and min_qty not in ("0", "0.0"):
            if qty < Decimal(min_qty):
                raise ValueError(f"Order size below minimum quantity for {symbol}: {min_qty}")

        if qty <= 0:
            raise ValueError(f"Order size too small after precision normalization for {symbol}")

        return self._decimal_to_str(qty)

    def _normalize_price(self, symbol: str, price: float) -> str:
        px = Decimal(str(price))
        if px <= 0:
            raise ValueError("Price must be greater than 0")

        tick_size = self._get_symbol_filter(symbol, "tick_size")
        if tick_size and tick_size not in ("0", "0.0"):
            px = self._round_to_step(px, Decimal(tick_size))

        if px <= 0:
            raise ValueError(f"Price too small after precision normalization for {symbol}")

        return self._decimal_to_str(px)

    def _set_leverage_if_needed(self, symbol: str, leverage: int) -> None:
        if not leverage or int(leverage) <= 0:
            return
        try:
            self._request(
                "POST",
                "/fapi/v1/leverage",
                params={"symbol": symbol, "leverage": int(leverage)},
                signed=True,
            )
        except Exception as err:
            # Non-fatal: some markets/accounts may reject repeated leverage set
            logger.debug("Failed to set leverage for %s: %s", symbol, err)

    def _set_isolated_margin_if_needed(self, symbol: str) -> None:
        """
        Enforce isolated margin mode once per symbol for safer bot operation.
        """
        if symbol in self._isolated_mode_initialized:
            return
        try:
            self._request(
                "POST",
                "/fapi/v1/marginType",
                params={"symbol": symbol, "marginType": "ISOLATED"},
                signed=True,
            )
        except BinanceApiError as err:
            # -4046: "No need to change margin type." (already isolated)
            if err.code != -4046:
                logger.debug("Failed to set isolated margin mode for %s: %s", symbol, err)
        except Exception as err:
            logger.debug("Failed to set isolated margin mode for %s: %s", symbol, err)
        finally:
            self._isolated_mode_initialized.add(symbol)

    def _place_reduce_only_trigger_order(
        self,
        *,
        formatted_symbol: str,
        side: str,
        amount: float,
        trigger_price: float,
        order_type: str,
    ) -> Optional[str]:
        """
        Create a reduce-only trigger-market order on Binance futures.
        order_type must be one of: TAKE_PROFIT_MARKET, STOP_MARKET.
        """
        if trigger_price <= 0 or amount <= 0:
            return None

        quantity = self._normalize_quantity(formatted_symbol, amount)
        stop_price = self._normalize_price(formatted_symbol, trigger_price)

        created = self._request(
            "POST",
            "/fapi/v1/order",
            params={
                "symbol": formatted_symbol,
                "side": side,
                "type": order_type,
                "quantity": quantity,
                "stopPrice": stop_price,
                "reduceOnly": "true",
                "workingType": "MARK_PRICE",
            },
            signed=True,
        )
        order_id = created.get("orderId")
        return str(order_id) if order_id is not None else None

    def _maybe_place_tpsl_reduce_only_orders(
        self,
        *,
        symbol: str,
        formatted_symbol: str,
        is_buy_entry: bool,
        amount: float,
        take_profit_price: Optional[float],
        stop_loss_price: Optional[float],
    ) -> Dict[str, Optional[str]]:
        """
        Place TP/SL reduce-only trigger orders after entry fill.
        """
        close_side = "SELL" if is_buy_entry else "BUY"
        tp_order_id = None
        sl_order_id = None

        try:
            if take_profit_price:
                tp_order_id = self._place_reduce_only_trigger_order(
                    formatted_symbol=formatted_symbol,
                    side=close_side,
                    amount=amount,
                    trigger_price=float(take_profit_price),
                    order_type="TAKE_PROFIT_MARKET",
                )
        except Exception as err:
            logger.warning(
                "Failed to place Binance TP reduce-only order: symbol=%s tp=%s err=%s",
                symbol,
                take_profit_price,
                err,
            )

        try:
            if stop_loss_price:
                sl_order_id = self._place_reduce_only_trigger_order(
                    formatted_symbol=formatted_symbol,
                    side=close_side,
                    amount=amount,
                    trigger_price=float(stop_loss_price),
                    order_type="STOP_MARKET",
                )
        except Exception as err:
            logger.warning(
                "Failed to place Binance SL reduce-only order: symbol=%s sl=%s err=%s",
                symbol,
                stop_loss_price,
                err,
            )

        return {"tp_order_id": tp_order_id, "sl_order_id": sl_order_id}

    def get_account_state(self, db: Session) -> Dict[str, Any]:
        """Get futures account state (USDT-margined)."""
        account = self._request("GET", "/fapi/v2/account", signed=True)
        now_ms = int(time.time() * 1000)

        available_balance = self._safe_float(account.get("availableBalance"))
        total_equity = self._safe_float(account.get("totalMarginBalance"))
        used_margin = self._safe_float(account.get("totalInitialMargin"))
        maintenance_margin = self._safe_float(account.get("totalMaintMargin"))

        assets = account.get("assets", []) if isinstance(account, dict) else []
        usdt_asset = next(
            (
                a
                for a in assets
                if str((a or {}).get("asset", "")).upper() == "USDT"
            ),
            None,
        )
        if usdt_asset:
            available_balance = self._safe_float(
                usdt_asset.get("availableBalance"), available_balance
            )
            total_equity = self._safe_float(
                usdt_asset.get("marginBalance"), total_equity
            )
            used_margin = self._safe_float(
                usdt_asset.get("initialMargin"), used_margin
            )
            maintenance_margin = self._safe_float(
                usdt_asset.get("maintMargin"), maintenance_margin
            )
            if total_equity <= 0:
                total_equity = self._safe_float(usdt_asset.get("walletBalance")) + self._safe_float(
                    usdt_asset.get("unrealizedProfit")
                )

        if used_margin <= 0 and total_equity > 0 and available_balance >= 0:
            used_margin = max(total_equity - available_balance, 0.0)

        margin_usage_percent = (
            used_margin / total_equity * 100 if total_equity > 0 else 0.0
        )

        return {
            "account_id": self.account_id,
            "environment": self.environment,
            "exchange": "binance",
            "wallet_address": None,
            "available_balance": available_balance,
            "total_equity": total_equity,
            "used_margin": used_margin,
            "maintenance_margin": maintenance_margin,
            "margin_usage_percent": margin_usage_percent,
            "timestamp": now_ms,
        }

    def get_positions(self, db: Session, include_timing: bool = False) -> List[Dict[str, Any]]:
        """Get open futures positions, normalized to Hyperliquid-like shape."""
        raw_positions = self._request("GET", "/fapi/v2/positionRisk", signed=True)
        if not isinstance(raw_positions, list):
            return []

        result: List[Dict[str, Any]] = []
        for pos in raw_positions:
            position_amt = self._safe_float(pos.get("positionAmt"))
            if abs(position_amt) <= 0:
                continue

            side = "short" if position_amt < 0 else "long"
            market_symbol = str(pos.get("symbol") or "").upper()
            coin = self._extract_coin(market_symbol)
            entry_price = self._safe_float(pos.get("entryPrice"))
            unrealized_pnl = self._safe_float(pos.get("unRealizedProfit"))
            notional = self._safe_float(pos.get("notional"))
            position_value = abs(notional) if abs(notional) > 0 else abs(entry_price * position_amt)
            leverage = max(self._safe_int(pos.get("leverage"), 1), 1)
            margin_used = self._safe_float(pos.get("isolatedMargin"))
            if margin_used <= 0 and position_value > 0:
                margin_used = position_value / leverage

            item: Dict[str, Any] = {
                "coin": coin,
                "symbol": coin,
                "exchange_symbol": market_symbol,
                "side": side,
                "szi": position_amt,
                "size": abs(position_amt),
                "entry_px": entry_price,
                "entry_price": entry_price,
                "unrealized_pnl": unrealized_pnl,
                "position_value": position_value,
                "margin_used": margin_used,
                "liquidation_px": self._safe_float(pos.get("liquidationPrice")),
                "liquidation_price": self._safe_float(pos.get("liquidationPrice")),
                "leverage": leverage,
            }

            if include_timing:
                item["opened_at"] = None
                item["opened_at_str"] = None
                item["holding_duration_seconds"] = None
                item["holding_duration_str"] = None

            result.append(item)

        return result

    def get_open_orders(self, db: Session, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get open orders."""
        params: Dict[str, Any] = {}
        if symbol:
            params["symbol"] = self._format_symbol(symbol)

        orders = self._request("GET", "/fapi/v1/openOrders", params=params, signed=True)
        if not isinstance(orders, list):
            return []

        result = []
        for order in orders:
            market_symbol = str(order.get("symbol") or "").upper()
            orig_qty = self._safe_float(order.get("origQty"))
            executed_qty = self._safe_float(order.get("executedQty"))
            trigger_price = self._safe_float(order.get("stopPrice"))
            reduce_only = bool(order.get("reduceOnly"))

            result.append(
                {
                    "order_id": order.get("orderId"),
                    "symbol": self._extract_coin(market_symbol),
                    "exchange_symbol": market_symbol,
                    "order_type": str(order.get("type") or "").upper(),
                    "side": str(order.get("side") or "").upper(),
                    "status": order.get("status"),
                    "price": self._safe_float(order.get("price")),
                    "size": orig_qty,
                    "amount": orig_qty,
                    "filled": executed_qty,
                    "remaining": max(orig_qty - executed_qty, 0.0),
                    "reduce_only": reduce_only,
                    "direction": "close" if reduce_only else "open",
                    "trigger_price": trigger_price if trigger_price > 0 else None,
                    "timestamp": self._safe_int(
                        order.get("updateTime") or order.get("time") or order.get("workingTime")
                    ),
                }
            )

        return result

    def cancel_order(self, db: Session, order_id: Any, symbol: str) -> bool:
        """Cancel an order by ID."""
        try:
            params: Dict[str, Any] = {"symbol": self._format_symbol(symbol)}
            try:
                params["orderId"] = int(str(order_id))
            except Exception:
                params["origClientOrderId"] = str(order_id)

            self._request("DELETE", "/fapi/v1/order", params=params, signed=True)
            return True
        except Exception as e:
            logger.error("Failed to cancel Binance order %s (%s): %s", order_id, symbol, e)
            return False

    def _normalize_order_result(
        self,
        symbol: str,
        order: Dict[str, Any],
        fallback_price: Optional[float],
    ) -> Dict[str, Any]:
        raw_status = str(order.get("status") or "").upper()
        filled_amount = self._safe_float(order.get("executedQty"), self._safe_float(order.get("filled")))
        average_price = self._safe_float(
            order.get("avgPrice"),
            self._safe_float(order.get("average"), self._safe_float(fallback_price)),
        )
        if average_price <= 0 and filled_amount > 0:
            cum_quote = self._safe_float(order.get("cumQuote"), self._safe_float(order.get("cum_quote")))
            if cum_quote > 0:
                average_price = cum_quote / filled_amount

        if raw_status in {"FILLED"}:
            status = "filled"
        elif raw_status in {"NEW", "PARTIALLY_FILLED"}:
            status = "resting"
        elif raw_status in {"CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"}:
            status = "error"
        else:
            orig_qty = self._safe_float(order.get("origQty"), self._safe_float(order.get("amount")))
            if filled_amount > 0 and (orig_qty <= 0 or filled_amount >= orig_qty):
                status = "filled"
            else:
                status = "resting"

        return {
            "status": status,
            "environment": self.environment,
            "exchange": "binance",
            "symbol": symbol,
            "order_id": order.get("orderId") or order.get("id"),
            "filled_amount": filled_amount,
            "average_price": average_price,
            "fee": 0.0,
            "tp_order_id": None,
            "sl_order_id": None,
            "raw_status": raw_status.lower(),
            "timestamp": self._safe_int(
                order.get("updateTime") or order.get("time") or order.get("timestamp")
            ),
        }

    def _fetch_order_by_id(self, formatted_symbol: str, order_id: Any) -> Optional[Dict[str, Any]]:
        try:
            return self._request(
                "GET",
                "/fapi/v1/order",
                params={
                    "symbol": formatted_symbol,
                    "orderId": int(str(order_id)),
                },
                signed=True,
            )
        except Exception:
            return None

    def _refresh_order_state(
        self,
        *,
        formatted_symbol: str,
        order: Dict[str, Any],
        max_attempts: int = 6,
        sleep_seconds: float = 0.2,
    ) -> Dict[str, Any]:
        """
        Binance can acknowledge MARKET/IOC orders as NEW before the fill is reflected.
        Poll order state briefly to capture executedQty/avgPrice for downstream TP/SL logic.
        """
        order_id = order.get("orderId") or order.get("id")
        if order_id is None:
            return order

        latest = order
        for _ in range(max_attempts):
            refreshed = self._fetch_order_by_id(formatted_symbol, order_id)
            if not refreshed:
                break

            latest = refreshed
            raw_status = str(refreshed.get("status") or "").upper()
            executed_qty = self._safe_float(refreshed.get("executedQty"))
            if raw_status in {"FILLED", "CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"}:
                break
            if executed_qty > 0 and raw_status != "NEW":
                break
            time.sleep(sleep_seconds)

        return latest

    def place_order_with_tpsl(
        self,
        db: Session,
        symbol: str,
        is_buy: bool,
        size: float,
        price: Optional[float],
        leverage: int = 1,
        time_in_force: str = "Ioc",
        reduce_only: bool = False,
        take_profit_price: Optional[float] = None,
        stop_loss_price: Optional[float] = None,
        tp_execution: str = "limit",
        sl_execution: str = "limit",
    ) -> Dict[str, Any]:
        """
        Place primary order.

        Notes:
        - For entry fills, TP/SL are auto-placed as reduce-only trigger-market orders.
        """
        try:
            formatted_symbol = self._format_symbol(symbol)
            side = "BUY" if is_buy else "SELL"
            amount = abs(self._safe_float(size))

            if amount <= 0:
                return {
                    "status": "error",
                    "error": "Order size must be greater than 0",
                    "environment": self.environment,
                    "exchange": "binance",
                    "symbol": symbol,
                }

            self._set_isolated_margin_if_needed(formatted_symbol)
            self._set_leverage_if_needed(formatted_symbol, leverage)

            order_type = "LIMIT" if price is not None else "MARKET"
            quantity = self._normalize_quantity(formatted_symbol, amount)
            params: Dict[str, Any] = {
                "symbol": formatted_symbol,
                "side": side,
                "type": order_type,
                "quantity": quantity,
            }

            normalized_price: Optional[str] = None
            if order_type == "LIMIT":
                normalized_price = self._normalize_price(formatted_symbol, float(price))
                params["price"] = normalized_price
                params["timeInForce"] = self._map_time_in_force(time_in_force)

            if reduce_only:
                params["reduceOnly"] = "true"

            order = self._request("POST", "/fapi/v1/order", params=params, signed=True)

            tif = self._map_time_in_force(time_in_force)
            if order_type == "MARKET" or tif == "IOC":
                order = self._refresh_order_state(
                    formatted_symbol=formatted_symbol,
                    order=order,
                )

            normalized = self._normalize_order_result(
                symbol=symbol,
                order=order,
                fallback_price=float(normalized_price) if normalized_price is not None else price,
            )

            if (
                not reduce_only
                and self._safe_float(normalized.get("filled_amount")) > 0
                and (take_profit_price or stop_loss_price)
            ):
                tpsl_amount = self._safe_float(normalized.get("filled_amount"), amount)
                if tpsl_amount > 0:
                    tpsl_orders = self._maybe_place_tpsl_reduce_only_orders(
                        symbol=symbol,
                        formatted_symbol=formatted_symbol,
                        is_buy_entry=is_buy,
                        amount=tpsl_amount,
                        take_profit_price=take_profit_price,
                        stop_loss_price=stop_loss_price,
                    )
                    normalized["tp_order_id"] = tpsl_orders.get("tp_order_id")
                    normalized["sl_order_id"] = tpsl_orders.get("sl_order_id")

            return normalized
        except Exception as e:
            logger.error("Failed to place Binance order: %s", e, exc_info=True)
            return {
                "status": "error",
                "error": str(e),
                "environment": self.environment,
                "exchange": "binance",
                "symbol": symbol,
            }

    def _get_recent_trade_symbols(self) -> List[str]:
        symbols = set(self.DEFAULT_RECENT_TRADE_SYMBOLS)
        try:
            positions = self._request("GET", "/fapi/v2/positionRisk", signed=True)
            if isinstance(positions, list):
                for pos in positions:
                    qty = self._safe_float(pos.get("positionAmt"))
                    market_symbol = str(pos.get("symbol") or "").upper()
                    if market_symbol and abs(qty) > 0:
                        symbols.add(market_symbol)
        except Exception:
            pass
        return sorted(symbols)

    def get_recent_closed_trades(self, db: Session, limit: int = 5) -> List[Dict[str, Any]]:
        """Get recent user trades from Binance."""
        if limit <= 0:
            return []

        all_trades: List[Dict[str, Any]] = []
        per_symbol_limit = min(max(limit * 2, 10), 50)

        for market_symbol in self._get_recent_trade_symbols()[:12]:
            try:
                trades = self._request(
                    "GET",
                    "/fapi/v1/userTrades",
                    params={"symbol": market_symbol, "limit": per_symbol_limit},
                    signed=True,
                )
            except Exception as e:
                logger.debug("Failed to fetch Binance user trades for %s: %s", market_symbol, e)
                continue

            if not isinstance(trades, list):
                continue

            for t in trades:
                close_ms = self._safe_int(t.get("time"))
                if close_ms <= 0:
                    continue

                symbol_raw = str(t.get("symbol") or market_symbol).upper()
                all_trades.append(
                    {
                        "symbol": self._extract_coin(symbol_raw),
                        "side": str(t.get("side") or "").lower(),
                        "size": self._safe_float(t.get("qty")),
                        "close_price": self._safe_float(t.get("price")),
                        "close_timestamp": int(close_ms / 1000),
                        "close_time": datetime.fromtimestamp(
                            close_ms / 1000,
                            tz=timezone.utc,
                        ).isoformat(),
                        "realized_pnl": self._safe_float(t.get("realizedPnl")),
                    }
                )

        all_trades.sort(key=lambda x: x.get("close_timestamp", 0), reverse=True)
        return all_trades[:limit]

    def test_connection(self) -> Dict[str, Any]:
        """Validate API credentials and trading account access."""
        state = self.get_account_state(db=None)  # type: ignore[arg-type]
        return {
            "success": True,
            "exchange": "binance",
            "environment": self.environment,
            "available_balance": state.get("available_balance", 0.0),
            "total_equity": state.get("total_equity", 0.0),
        }


def create_binance_client(
    account_id: int,
    api_key: str,
    api_secret: str,
    environment: str,
) -> BinanceTradingClient:
    return BinanceTradingClient(
        account_id=account_id,
        api_key=api_key,
        api_secret=api_secret,
        environment=environment,
    )


_binance_client_cache: Dict[Tuple[int, str], Dict[str, Any]] = {}
_binance_cache_lock = threading.Lock()


def get_cached_binance_client(
    account_id: int,
    api_key: str,
    api_secret: str,
    environment: str,
) -> BinanceTradingClient:
    cache_key = (account_id, environment)
    now = time.time()

    with _binance_cache_lock:
        if cache_key in _binance_client_cache:
            return _binance_client_cache[cache_key]["client"]

        client = BinanceTradingClient(
            account_id=account_id,
            api_key=api_key,
            api_secret=api_secret,
            environment=environment,
        )
        _binance_client_cache[cache_key] = {"client": client, "created_at": now}
        return client


def clear_binance_client_cache(account_id: int = None, environment: str = None) -> int:
    cleared = 0
    with _binance_cache_lock:
        if account_id is None and environment is None:
            cleared = len(_binance_client_cache)
            _binance_client_cache.clear()
            return cleared

        remove_keys = []
        for key in _binance_client_cache.keys():
            acc_id, env = key
            if (account_id is None or acc_id == account_id) and (
                environment is None or env == environment
            ):
                remove_keys.append(key)

        for key in remove_keys:
            del _binance_client_cache[key]
            cleared += 1

    return cleared
