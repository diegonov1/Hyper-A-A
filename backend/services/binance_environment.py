"""
Binance environment and credential management.
"""
import logging
import os
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from database.models import Account, BinanceApiCredential, SystemConfig
from services.binance_trading_client import (
    BinanceTradingClient,
    clear_binance_client_cache,
    get_cached_binance_client,
)
from utils.encryption import decrypt_private_key, encrypt_private_key

logger = logging.getLogger(__name__)

BINANCE_TRADING_MODE_CONFIG_KEY = "binance_trading_mode"
DEFAULT_BINANCE_ENVIRONMENT = "testnet"
BINANCE_DEFAULT_MAX_LEVERAGE = 20
BINANCE_DEFAULT_LEVERAGE = 5
BINANCE_DEFAULT_MARGIN_MODE = "isolated"
DEMO_BINANCE_API_KEY_ENV = "DEMO_BINANCE_API_KEY"
DEMO_BINANCE_SECRET_KEY_ENV = "DEMO_BINANCE_SECRET_KEY"
DEMO_BINANCE_CLIENT_ACCOUNT_ID = 0


def setup_binance_account(
    db: Session,
    account_id: int,
    api_key: str,
    api_secret: str,
    environment: str = DEFAULT_BINANCE_ENVIRONMENT,
) -> Dict[str, Any]:
    """Create or update Binance API credentials for an account/environment."""
    if environment not in ["mainnet", "testnet"]:
        raise ValueError("Environment must be 'mainnet' or 'testnet'")

    account = db.query(Account).filter(Account.id == account_id).first()
    if not account:
        raise ValueError(f"Account {account_id} not found")

    if not api_key or not api_secret:
        raise ValueError("api_key and api_secret are required")

    encrypted_api_key = encrypt_private_key(api_key)
    encrypted_api_secret = encrypt_private_key(api_secret)

    credential = db.query(BinanceApiCredential).filter(
        BinanceApiCredential.account_id == account_id,
        BinanceApiCredential.environment == environment,
    ).first()

    if credential:
        credential.api_key_encrypted = encrypted_api_key
        credential.api_secret_encrypted = encrypted_api_secret
        credential.is_active = "true"
        action = "updated"
    else:
        credential = BinanceApiCredential(
            account_id=account_id,
            environment=environment,
            api_key_encrypted=encrypted_api_key,
            api_secret_encrypted=encrypted_api_secret,
            is_active="true",
        )
        db.add(credential)
        action = "created"

    try:
        db.commit()
        db.refresh(credential)
        clear_binance_client_cache(account_id=account_id, environment=environment)
    except Exception:
        db.rollback()
        raise

    return {
        "success": True,
        "status": action,
        "message": f"Binance credentials {action} for account {account.name} ({environment})",
        "account_id": account_id,
        "environment": environment,
        "credential_id": credential.id,
    }


def get_account_binance_config(db: Session, account_id: int) -> Dict[str, Any]:
    """Return Binance credential availability for both environments."""
    account = db.query(Account).filter(Account.id == account_id).first()
    if not account:
        raise ValueError(f"Account {account_id} not found")

    rows = db.query(BinanceApiCredential).filter(
        BinanceApiCredential.account_id == account_id
    ).all()

    testnet = None
    mainnet = None
    for row in rows:
        data = {
            "id": row.id,
            "environment": row.environment,
            "is_active": str(row.is_active).lower() == "true",
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "updated_at": row.updated_at.isoformat() if row.updated_at else None,
        }
        if row.environment == "testnet":
            testnet = data
        elif row.environment == "mainnet":
            mainnet = data

    return {
        "account_id": account_id,
        "account_name": account.name,
        "configured": bool(testnet or mainnet),
        "demo_testnet_env_available": bool(
            os.getenv(DEMO_BINANCE_API_KEY_ENV) and os.getenv(DEMO_BINANCE_SECRET_KEY_ENV)
        ),
        "testnet": testnet,
        "mainnet": mainnet,
    }


def delete_binance_credentials(
    db: Session,
    account_id: int,
    environment: str,
) -> Dict[str, Any]:
    if environment not in ["mainnet", "testnet"]:
        raise ValueError("Environment must be 'mainnet' or 'testnet'")

    credential = db.query(BinanceApiCredential).filter(
        BinanceApiCredential.account_id == account_id,
        BinanceApiCredential.environment == environment,
    ).first()

    if not credential:
        raise ValueError(f"No Binance credentials configured for {environment}")

    db.delete(credential)
    db.commit()
    clear_binance_client_cache(account_id=account_id, environment=environment)

    return {
        "success": True,
        "message": f"Deleted Binance credentials for account {account_id} ({environment})",
        "account_id": account_id,
        "environment": environment,
    }


def get_binance_client(
    db: Session,
    account_id: int,
    override_environment: Optional[str] = None,
) -> BinanceTradingClient:
    """Get authenticated Binance trading client for account."""
    account = db.query(Account).filter(Account.id == account_id).first()
    if not account:
        raise ValueError(f"Account {account_id} not found")

    environment = override_environment or get_global_binance_mode(db)
    if environment not in ["mainnet", "testnet"]:
        raise ValueError("override_environment must be 'mainnet' or 'testnet'")

    credential = db.query(BinanceApiCredential).filter(
        BinanceApiCredential.account_id == account_id,
        BinanceApiCredential.environment == environment,
    ).first()

    if not credential or str(credential.is_active).lower() == "false":
        # Testnet demo fallback from .env (shared demo key for quick bootstrap).
        if environment == "testnet":
            demo_api_key = os.getenv(DEMO_BINANCE_API_KEY_ENV, "").strip()
            demo_api_secret = os.getenv(DEMO_BINANCE_SECRET_KEY_ENV, "").strip()
            if demo_api_key and demo_api_secret:
                logger.warning(
                    "Using demo Binance testnet credentials from environment for account_id=%s",
                    account_id,
                )
                return get_cached_binance_client(
                    account_id=account_id,
                    api_key=demo_api_key,
                    api_secret=demo_api_secret,
                    environment=environment,
                )

        raise ValueError(
            f"No active Binance credentials configured for account {account.name} ({environment})"
        )

    api_key = decrypt_private_key(credential.api_key_encrypted)
    api_secret = decrypt_private_key(credential.api_secret_encrypted)

    return get_cached_binance_client(
        account_id=account_id,
        api_key=api_key,
        api_secret=api_secret,
        environment=environment,
    )


def has_demo_binance_credentials() -> bool:
    """Return whether shared demo Binance credentials are configured in .env."""
    api_key = os.getenv(DEMO_BINANCE_API_KEY_ENV, "").strip()
    api_secret = os.getenv(DEMO_BINANCE_SECRET_KEY_ENV, "").strip()
    return bool(api_key and api_secret)


def get_demo_binance_client(
    db: Optional[Session] = None,
    override_environment: Optional[str] = None,
) -> BinanceTradingClient:
    """
    Get shared demo Binance client without account_id-based credentials.

    Uses DEMO_BINANCE_API_KEY / DEMO_BINANCE_SECRET_KEY from environment.
    """
    environment = override_environment or (
        get_global_binance_mode(db) if db is not None else DEFAULT_BINANCE_ENVIRONMENT
    )
    if environment not in ["mainnet", "testnet"]:
        raise ValueError("override_environment must be 'mainnet' or 'testnet'")

    demo_api_key = os.getenv(DEMO_BINANCE_API_KEY_ENV, "").strip()
    demo_api_secret = os.getenv(DEMO_BINANCE_SECRET_KEY_ENV, "").strip()
    if not demo_api_key or not demo_api_secret:
        raise ValueError(
            "Demo Binance credentials are not configured. "
            "Set DEMO_BINANCE_API_KEY and DEMO_BINANCE_SECRET_KEY."
        )

    return get_cached_binance_client(
        account_id=DEMO_BINANCE_CLIENT_ACCOUNT_ID,
        api_key=demo_api_key,
        api_secret=demo_api_secret,
        environment=environment,
    )


def get_global_binance_mode(db: Session) -> str:
    """Return global Binance trading mode: testnet (default) or mainnet."""
    config = db.query(SystemConfig).filter(
        SystemConfig.key == BINANCE_TRADING_MODE_CONFIG_KEY
    ).first()
    if not config:
        return DEFAULT_BINANCE_ENVIRONMENT

    value = str(config.value or "").strip().lower()
    if value in ("testnet", "mainnet"):
        return value
    return DEFAULT_BINANCE_ENVIRONMENT


def get_binance_leverage_settings(db: Session, account_id: int) -> Dict[str, Any]:
    """
    Risk defaults for Binance execution.

    - Margin mode: isolated
    - Default leverage: 5x
    - Maximum leverage cap: 20x
    """
    account = db.query(Account).filter(Account.id == account_id).first()

    account_max = getattr(account, "max_leverage", None) if account else None
    account_default = getattr(account, "default_leverage", None) if account else None

    try:
        max_leverage = int(account_max) if account_max is not None else BINANCE_DEFAULT_MAX_LEVERAGE
    except Exception:
        max_leverage = BINANCE_DEFAULT_MAX_LEVERAGE
    max_leverage = max(1, min(max_leverage, BINANCE_DEFAULT_MAX_LEVERAGE))

    try:
        default_leverage = int(account_default) if account_default is not None else BINANCE_DEFAULT_LEVERAGE
    except Exception:
        default_leverage = BINANCE_DEFAULT_LEVERAGE
    default_leverage = max(1, min(default_leverage, max_leverage))

    return {
        "max_leverage": max_leverage,
        "default_leverage": default_leverage,
        "margin_mode": BINANCE_DEFAULT_MARGIN_MODE,
    }
