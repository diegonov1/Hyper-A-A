"""
Binance Management API Routes

Provides endpoints for:
- Credential setup (mainnet/testnet)
- Balance and position queries
- Manual order placement/cancelation
- Connection testing
"""
from datetime import datetime, timezone
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from database.connection import get_db
from services.binance_environment import (
    delete_binance_credentials,
    get_account_binance_config,
    get_binance_client,
    get_demo_binance_client,
    get_global_binance_mode,
    has_demo_binance_credentials,
    setup_binance_account,
)
from services.binance_market_data import get_last_price_from_binance

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/binance", tags=["binance"])


class BinanceCredentialRequest(BaseModel):
    environment: str = Field("testnet", pattern="^(mainnet|testnet)$")
    api_key: str = Field(..., min_length=5, alias="apiKey")
    api_secret: str = Field(..., min_length=5, alias="apiSecret")

    class Config:
        populate_by_name = True


class BinanceManualOrderRequest(BaseModel):
    symbol: str = Field(..., description="Asset symbol, e.g. BTC or BTCUSDT")
    is_buy: bool = Field(..., alias="isBuy")
    size: float = Field(..., gt=0)
    price: Optional[float] = Field(None, gt=0)
    leverage: int = Field(1, ge=1, le=125)
    time_in_force: str = Field("Ioc", alias="timeInForce")
    reduce_only: bool = Field(False, alias="reduceOnly")
    take_profit_price: Optional[float] = Field(None, alias="takeProfitPrice")
    stop_loss_price: Optional[float] = Field(None, alias="stopLossPrice")
    environment: Optional[str] = Field(None, pattern="^(mainnet|testnet)$")

    class Config:
        populate_by_name = True


@router.post("/accounts/{account_id}/credentials")
async def configure_credentials(
    account_id: int,
    request: BinanceCredentialRequest,
    db: Session = Depends(get_db),
):
    try:
        result = setup_binance_account(
            db=db,
            account_id=account_id,
            api_key=request.api_key,
            api_secret=request.api_secret,
            environment=request.environment,
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to configure Binance credentials: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to configure credentials: {str(e)}")


@router.get("/accounts/{account_id}/config")
async def get_config(
    account_id: int,
    db: Session = Depends(get_db),
):
    try:
        return get_account_binance_config(db, account_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error("Failed to get Binance config: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get config: {str(e)}")


@router.delete("/accounts/{account_id}/credentials")
async def remove_credentials(
    account_id: int,
    environment: str = Query(..., pattern="^(mainnet|testnet)$"),
    db: Session = Depends(get_db),
):
    try:
        return delete_binance_credentials(db, account_id, environment)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to delete Binance credentials: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to delete credentials: {str(e)}")


@router.get("/accounts/{account_id}/balance")
async def get_balance(
    account_id: int,
    environment: Optional[str] = Query(None, pattern="^(mainnet|testnet)$"),
    db: Session = Depends(get_db),
):
    try:
        client = get_binance_client(db, account_id, override_environment=environment)
        return client.get_account_state(db)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to fetch Binance balance: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to fetch balance: {str(e)}")


@router.get("/accounts/{account_id}/positions")
async def get_positions(
    account_id: int,
    environment: Optional[str] = Query(None, pattern="^(mainnet|testnet)$"),
    db: Session = Depends(get_db),
):
    try:
        client = get_binance_client(db, account_id, override_environment=environment)
        positions = client.get_positions(db)
        return {
            "account_id": account_id,
            "environment": client.environment,
            "count": len(positions),
            "positions": positions,
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to fetch Binance positions: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to fetch positions: {str(e)}")


@router.get("/accounts/{account_id}/orders/open")
async def get_open_orders(
    account_id: int,
    symbol: Optional[str] = Query(None),
    environment: Optional[str] = Query(None, pattern="^(mainnet|testnet)$"),
    db: Session = Depends(get_db),
):
    try:
        client = get_binance_client(db, account_id, override_environment=environment)
        orders = client.get_open_orders(db, symbol=symbol)
        return {
            "account_id": account_id,
            "environment": client.environment,
            "count": len(orders),
            "orders": orders,
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to fetch Binance open orders: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to fetch open orders: {str(e)}")


@router.post("/accounts/{account_id}/orders/manual")
async def place_manual_order(
    account_id: int,
    request: BinanceManualOrderRequest,
    db: Session = Depends(get_db),
):
    try:
        client = get_binance_client(db, account_id, override_environment=request.environment)
        result = client.place_order_with_tpsl(
            db=db,
            symbol=request.symbol,
            is_buy=request.is_buy,
            size=request.size,
            price=request.price,
            leverage=request.leverage,
            time_in_force=request.time_in_force,
            reduce_only=request.reduce_only,
            take_profit_price=request.take_profit_price,
            stop_loss_price=request.stop_loss_price,
        )

        if result.get("status") == "error":
            raise HTTPException(status_code=400, detail=result.get("error", "Order failed"))

        return result
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to place Binance order: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to place order: {str(e)}")


@router.delete("/accounts/{account_id}/orders/{order_id}")
async def cancel_manual_order(
    account_id: int,
    order_id: str,
    symbol: str = Query(...),
    environment: Optional[str] = Query(None, pattern="^(mainnet|testnet)$"),
    db: Session = Depends(get_db),
):
    try:
        client = get_binance_client(db, account_id, override_environment=environment)
        success = client.cancel_order(db, order_id=order_id, symbol=symbol)
        if not success:
            raise HTTPException(status_code=400, detail="Failed to cancel order")
        return {
            "success": True,
            "account_id": account_id,
            "environment": client.environment,
            "order_id": order_id,
            "symbol": symbol,
        }
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to cancel Binance order: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to cancel order: {str(e)}")


@router.get("/accounts/{account_id}/test-connection")
async def test_connection(
    account_id: int,
    environment: str = Query("testnet", pattern="^(mainnet|testnet)$"),
    db: Session = Depends(get_db),
):
    try:
        client = get_binance_client(db, account_id, override_environment=environment)
        return client.test_connection()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed Binance connection test: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Connection test failed: {str(e)}")


@router.get("/demo/config")
async def get_demo_config(
    db: Session = Depends(get_db),
):
    """
    Read shared Binance demo configuration.

    Demo mode uses DEMO_BINANCE_API_KEY / DEMO_BINANCE_SECRET_KEY and does not
    require account_id-specific credentials.
    """
    try:
        return {
            "exchange": "binance",
            "demo": True,
            "configured": has_demo_binance_credentials(),
            "default_environment": get_global_binance_mode(db),
            "supported_environments": ["testnet", "mainnet"],
        }
    except Exception as e:
        logger.error("Failed to fetch Binance demo config: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to fetch demo config: {str(e)}")


@router.get("/demo/test-connection")
async def test_demo_connection(
    environment: Optional[str] = Query(None, pattern="^(mainnet|testnet)$"),
    db: Session = Depends(get_db),
):
    try:
        client = get_demo_binance_client(db=db, override_environment=environment)
        result = client.test_connection()
        result["demo"] = True
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed Binance demo connection test: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Demo connection test failed: {str(e)}")


@router.get("/demo/balance")
async def get_demo_balance(
    environment: Optional[str] = Query(None, pattern="^(mainnet|testnet)$"),
    db: Session = Depends(get_db),
):
    try:
        client = get_demo_binance_client(db=db, override_environment=environment)
        payload = client.get_account_state(db)
        payload["demo"] = True
        return payload
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to fetch Binance demo balance: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to fetch demo balance: {str(e)}")


@router.get("/demo/positions")
async def get_demo_positions(
    environment: Optional[str] = Query(None, pattern="^(mainnet|testnet)$"),
    db: Session = Depends(get_db),
):
    try:
        client = get_demo_binance_client(db=db, override_environment=environment)
        positions = client.get_positions(db)
        return {
            "exchange": "binance",
            "demo": True,
            "environment": client.environment,
            "count": len(positions),
            "positions": positions,
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to fetch Binance demo positions: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to fetch demo positions: {str(e)}")


@router.get("/demo/orders/open")
async def get_demo_open_orders(
    symbol: Optional[str] = Query(None),
    environment: Optional[str] = Query(None, pattern="^(mainnet|testnet)$"),
    db: Session = Depends(get_db),
):
    try:
        client = get_demo_binance_client(db=db, override_environment=environment)
        orders = client.get_open_orders(db, symbol=symbol)
        return {
            "exchange": "binance",
            "demo": True,
            "environment": client.environment,
            "count": len(orders),
            "orders": orders,
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to fetch Binance demo open orders: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to fetch demo open orders: {str(e)}")


@router.post("/demo/orders/manual")
async def place_demo_manual_order(
    request: BinanceManualOrderRequest,
    db: Session = Depends(get_db),
):
    try:
        client = get_demo_binance_client(db=db, override_environment=request.environment)
        result = client.place_order_with_tpsl(
            db=db,
            symbol=request.symbol,
            is_buy=request.is_buy,
            size=request.size,
            price=request.price,
            leverage=request.leverage,
            time_in_force=request.time_in_force,
            reduce_only=request.reduce_only,
            take_profit_price=request.take_profit_price,
            stop_loss_price=request.stop_loss_price,
        )

        if result.get("status") == "error":
            raise HTTPException(status_code=400, detail=result.get("error", "Order failed"))

        result["demo"] = True
        return result
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to place Binance demo order: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to place demo order: {str(e)}")


@router.delete("/demo/orders/{order_id}")
async def cancel_demo_manual_order(
    order_id: str,
    symbol: str = Query(...),
    environment: Optional[str] = Query(None, pattern="^(mainnet|testnet)$"),
    db: Session = Depends(get_db),
):
    try:
        client = get_demo_binance_client(db=db, override_environment=environment)
        success = client.cancel_order(db, order_id=order_id, symbol=symbol)
        if not success:
            raise HTTPException(status_code=400, detail="Failed to cancel order")
        return {
            "success": True,
            "exchange": "binance",
            "demo": True,
            "environment": client.environment,
            "order_id": order_id,
            "symbol": symbol,
        }
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to cancel Binance demo order: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to cancel demo order: {str(e)}")


@router.get("/health")
async def health():
    try:
        price = get_last_price_from_binance("BTC")
        return {
            "status": "healthy",
            "exchange": "binance",
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "test_price": {"symbol": "BTC", "price": price},
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "exchange": "binance",
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "error": str(e),
        }
