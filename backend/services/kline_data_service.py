"""
K - 
"""

import asyncio
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import text
import logging

from database.connection import SessionLocal
from database.models import CryptoKline, UserExchangeConfig, KlineCollectionTask
from .kline_collectors import ExchangeDataSourceFactory, BaseKlineCollector, KlineData

logger = logging.getLogger(__name__)


class KlineDataService:
    """K - ，"""

    def __init__(self):
        self.exchange_id: Optional[str] = None
        self.collector: Optional[BaseKlineCollector] = None
        self._initialized = False

    async def initialize(self):
        """ - """
        if self._initialized:
            return

        try:
            # 
            with SessionLocal() as db:
                config = db.query(UserExchangeConfig).filter(
                    UserExchangeConfig.user_id == 1
                ).first()

                if config:
                    self.exchange_id = config.selected_exchange
                else:
                    self.exchange_id = "hyperliquid"  # 

            # 
            self.collector = ExchangeDataSourceFactory.get_collector(self.exchange_id)
            self._initialized = True

            logger.info(f"KlineDataService initialized with exchange: {self.exchange_id}")

        except Exception as e:
            logger.error(f"Failed to initialize KlineDataService: {e}")
            # 
            self.exchange_id = "hyperliquid"
            self.collector = ExchangeDataSourceFactory.get_collector(self.exchange_id)
            self._initialized = True

    def _ensure_initialized(self):
        """"""
        if not self._initialized:
            raise RuntimeError("KlineDataService not initialized. Call initialize() first.")

    async def collect_current_kline(self, symbol: str, period: str = "1m") -> bool:
        """K"""
        self._ensure_initialized()

        try:
            # 
            kline_data = await self.collector.fetch_current_kline(symbol, period)
            if not kline_data:
                logger.warning(f"No kline data received for {symbol}")
                return False

            # （）
            return await self._insert_kline_data([kline_data])

        except Exception as e:
            logger.error(f"Failed to collect current kline for {symbol}: {e}")
            return False

    async def collect_historical_klines(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        period: str = "1m"
    ) -> int:
        """K，"""
        self._ensure_initialized()

        try:
            # 
            klines_data = await self.collector.fetch_historical_klines(
                symbol, start_time, end_time, period
            )

            if not klines_data:
                logger.warning(f"No historical klines received for {symbol}")
                return 0

            # 
            success = await self._insert_kline_data(klines_data)
            return len(klines_data) if success else 0

        except Exception as e:
            logger.error(f"Failed to collect historical klines for {symbol}: {e}")
            return 0

    async def _insert_kline_data(self, klines_data: List[KlineData]) -> bool:
        """K（）"""
        if not klines_data:
            return True

        try:
            with SessionLocal() as db:
                for kline in klines_data:
                    # Generate datetime_str from timestamp (UTC)
                    datetime_str = datetime.utcfromtimestamp(kline.timestamp).strftime('%Y-%m-%d %H:%M:%S')

                    # SQLON CONFLICT DO NOTHING
                    # NOTE: K mainnet ，testnet 
                    db.execute(text("""
                        INSERT INTO crypto_klines (
                            exchange, symbol, market, timestamp, period, datetime_str,
                            open_price, high_price, low_price, close_price, volume,
                            environment, created_at
                        ) VALUES (
                            :exchange, :symbol, :market, :timestamp, :period, :datetime_str,
                            :open_price, :high_price, :low_price, :close_price, :volume,
                            'mainnet', CURRENT_TIMESTAMP
                        ) ON CONFLICT (exchange, symbol, market, period, timestamp, environment) DO NOTHING
                    """), {
                        'exchange': kline.exchange,
                        'symbol': kline.symbol,
                        'market': 'CRYPTO',
                        'timestamp': kline.timestamp,
                        'period': kline.period,
                        'datetime_str': datetime_str,
                        'open_price': kline.open_price,
                        'high_price': kline.high_price,
                        'low_price': kline.low_price,
                        'close_price': kline.close_price,
                        'volume': kline.volume
                    })

                db.commit()
                logger.debug(f"Inserted {len(klines_data)} klines for {klines_data[0].symbol}")
                return True

        except Exception as e:
            logger.error(f"Failed to insert kline data: {e}")
            return False

    async def get_data_coverage(self, symbols: List[str] = None) -> List[Dict[str, Any]]:
        """"""
        self._ensure_initialized()

        try:
            with SessionLocal() as db:
                query = """
                    SELECT * FROM kline_coverage_stats
                    WHERE exchange = :exchange
                """
                params = {'exchange': self.exchange_id}

                if symbols:
                    query += " AND symbol = ANY(:symbols)"
                    params['symbols'] = symbols

                query += " ORDER BY symbol, period"

                result = db.execute(text(query), params)
                return [dict(row._mapping) for row in result]

        except Exception as e:
            logger.error(f"Failed to get data coverage: {e}")
            return []

    async def detect_missing_ranges(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        period: str = "1m"
    ) -> List[tuple]:
        """"""
        self._ensure_initialized()

        try:
            with SessionLocal() as db:
                # 
                result = db.execute(text("""
                    SELECT timestamp FROM crypto_klines
                    WHERE exchange = :exchange AND symbol = :symbol
                    AND period = :period AND timestamp BETWEEN :start_ts AND :end_ts
                    ORDER BY timestamp
                """), {
                    'exchange': self.exchange_id,
                    'symbol': symbol,
                    'period': period,
                    'start_ts': int(start_time.timestamp()),
                    'end_ts': int(end_time.timestamp())
                })

                existing_timestamps = {row[0] for row in result}

                # （1）
                expected_timestamps = []
                current = start_time
                while current <= end_time:
                    expected_timestamps.append(int(current.timestamp()))
                    current += timedelta(minutes=1)

                # 
                missing_ranges = []
                range_start = None

                for ts in expected_timestamps:
                    if ts not in existing_timestamps:
                        if range_start is None:
                            range_start = ts
                    else:
                        if range_start is not None:
                            missing_ranges.append((
                                datetime.fromtimestamp(range_start),
                                datetime.fromtimestamp(ts - 60)  # 
                            ))
                            range_start = None

                # 
                if range_start is not None:
                    missing_ranges.append((
                        datetime.fromtimestamp(range_start),
                        end_time
                    ))

                return missing_ranges

        except Exception as e:
            logger.error(f"Failed to detect missing ranges: {e}")
            return []

    def get_supported_symbols(self) -> List[str]:
        """"""
        self._ensure_initialized()
        return self.collector.get_supported_symbols()

    async def refresh_exchange_config(self):
        """（）"""
        self._initialized = False
        await self.initialize()


# 
kline_service = KlineDataService()