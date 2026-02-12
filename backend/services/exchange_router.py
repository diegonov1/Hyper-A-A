"""
Exchange selection helpers for execution routing.
"""

from typing import Optional

from sqlalchemy.orm import Session

from database.models import Account, SystemConfig, UserExchangeConfig

SUPPORTED_EXECUTION_EXCHANGES = {"hyperliquid", "binance"}
DEFAULT_EXECUTION_EXCHANGE = "hyperliquid"
FALLBACK_EXCHANGE_CONFIG_KEY = "fallback_exchange"


def _normalize_exchange(value: Optional[str]) -> str:
    token = str(value or "").strip().lower()
    if token in SUPPORTED_EXECUTION_EXCHANGES:
        return token
    return DEFAULT_EXECUTION_EXCHANGE


def get_selected_exchange_for_user(db: Session, user_id: int = 1) -> str:
    config = db.query(UserExchangeConfig).filter(UserExchangeConfig.user_id == user_id).first()
    if not config:
        return DEFAULT_EXECUTION_EXCHANGE
    return _normalize_exchange(config.selected_exchange)


def get_selected_exchange_for_account(db: Session, account: Account) -> str:
    return get_selected_exchange_for_user(db, user_id=account.user_id or 1)


def get_fallback_exchange(db: Session) -> Optional[str]:
    config = db.query(SystemConfig).filter(
        SystemConfig.key == FALLBACK_EXCHANGE_CONFIG_KEY
    ).first()
    if not config:
        return None
    value = str(config.value or "").strip().lower()
    if value in SUPPORTED_EXECUTION_EXCHANGES:
        return value
    return None
