"""
K - K
"""

import asyncio
from datetime import datetime, timedelta
from typing import List, Set
import logging

from .kline_data_service import kline_service

logger = logging.getLogger(__name__)


class KlineRealtimeCollector:
    """K"""

    def __init__(self):
        self.running = False
        self.collection_task = None
        self.gap_detection_task = None

        # 
        self.default_symbols = ["BTC", "ETH", "SOL", "ARB", "OP"]

        # K (1m1h)
        self.periods = ["1m", "3m", "5m", "15m", "30m", "1h"]

    async def start(self):
        """"""
        if self.running:
            logger.warning("Realtime collector is already running")
            return

        try:
            # 
            await kline_service.initialize()

            self.running = True
            logger.info("Starting K-line realtime collection service")

            # 
            self.collection_task = asyncio.create_task(self._realtime_collection_loop())

            # （）
            self.gap_detection_task = asyncio.create_task(self._gap_detection_loop())

            logger.info("K-line realtime collector started successfully")

        except Exception as e:
            logger.error(f"Failed to start realtime collector: {e}")
            self.running = False
            raise

    async def stop(self):
        """"""
        if not self.running:
            return

        logger.info("Stopping K-line realtime collection service")
        self.running = False

        # 
        if self.collection_task:
            self.collection_task.cancel()
            try:
                await self.collection_task
            except asyncio.CancelledError:
                pass

        if self.gap_detection_task:
            self.gap_detection_task.cancel()
            try:
                await self.gap_detection_task
            except asyncio.CancelledError:
                pass

        logger.info("K-line realtime collector stopped")

    async def _realtime_collection_loop(self):
        """ - """
        logger.info("Starting realtime collection loop")

        while self.running:
            try:
                # 
                await self._wait_for_next_minute()

                if not self.running:
                    break

                # 
                await self._collect_current_minute()

            except asyncio.CancelledError:
                logger.info("Realtime collection loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in realtime collection loop: {e}")
                # 30
                await asyncio.sleep(30)

    async def _wait_for_next_minute(self):
        """"""
        now = datetime.now()
        # 
        seconds_to_wait = 60 - now.second - now.microsecond / 1000000

        # 1，
        if seconds_to_wait < 1:
            seconds_to_wait += 60

        logger.debug(f"Waiting {seconds_to_wait:.1f} seconds for next minute")
        await asyncio.sleep(seconds_to_wait)

    async def _collect_current_minute(self):
        """K（）"""
        current_time = datetime.now()
        logger.info(f"Collecting K-lines at {current_time.strftime('%Y-%m-%d %H:%M:%S')}")

        # 
        symbols = kline_service.get_supported_symbols()
        if not symbols:
            symbols = self.default_symbols

        # 
        tasks = []
        task_info = []  # symbolperiod

        for symbol in symbols:
            for period in self.periods:
                task = asyncio.create_task(
                    self._collect_symbol_kline(symbol, period),
                    name=f"collect_{symbol}_{period}"
                )
                tasks.append(task)
                task_info.append((symbol, period))

        # 
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 
        success_count = 0
        error_count = 0

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                symbol, period = task_info[i]
                logger.error(f"Failed to collect {symbol}/{period}: {result}")
                error_count += 1
            elif result:
                success_count += 1
            else:
                error_count += 1

        logger.info(f"Collection completed: {success_count} success, {error_count} errors (total: {len(tasks)} tasks)")

    async def _collect_symbol_kline(self, symbol: str, period: str = "1m") -> bool:
        """K"""
        try:
            return await kline_service.collect_current_kline(symbol, period)
        except Exception as e:
            logger.error(f"Failed to collect kline for {symbol}/{period}: {e}")
            return False

    async def _gap_detection_loop(self):
        """ - """
        logger.info("Starting gap detection loop")

        while self.running:
            try:
                # 1
                await asyncio.sleep(3600)

                if not self.running:
                    break

                # 
                await self._detect_and_fill_gaps()

            except asyncio.CancelledError:
                logger.info("Gap detection loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in gap detection loop: {e}")

    async def _detect_and_fill_gaps(self):
        """"""
        logger.info("Starting gap detection and auto-fill")

        # 24
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=24)

        symbols = kline_service.get_supported_symbols()
        if not symbols:
            symbols = self.default_symbols

        for symbol in symbols:
            try:
                # 
                missing_ranges = await kline_service.detect_missing_ranges(
                    symbol, start_time, end_time, "1m"
                )

                if missing_ranges:
                    logger.info(f"Found {len(missing_ranges)} missing ranges for {symbol}")

                    # 
                    for range_start, range_end in missing_ranges:
                        # （6）
                        if (range_end - range_start).total_seconds() > 6 * 3600:
                            logger.warning(f"Large gap detected for {symbol}: {range_start} to {range_end}, skipping auto-fill")
                            continue

                        collected = await kline_service.collect_historical_klines(
                            symbol, range_start, range_end, "1m"
                        )

                        if collected > 0:
                            logger.info(f"Auto-filled {collected} records for {symbol} from {range_start} to {range_end}")
                        else:
                            logger.warning(f"Failed to auto-fill gap for {symbol} from {range_start} to {range_end}")

                        # API，
                        await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"Error detecting gaps for {symbol}: {e}")

        logger.info("Gap detection and auto-fill completed")


# 
realtime_collector = KlineRealtimeCollector()