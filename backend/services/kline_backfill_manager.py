"""
K - 
"""

import asyncio
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from typing import Optional
import logging

from database.connection import SessionLocal
from database.models import KlineCollectionTask
from .kline_data_service import kline_service

logger = logging.getLogger(__name__)


class BackfillManager:
    """"""

    def __init__(self):
        self.max_concurrent_tasks = 3  # 

    async def process_task(self, task_id: int):
        """"""
        logger.info(f"Starting backfill task {task_id}")

        with SessionLocal() as db:
            # 
            task = db.query(KlineCollectionTask).filter(
                KlineCollectionTask.id == task_id
            ).first()

            if not task:
                logger.error(f"Task {task_id} not found")
                return

            if task.status != "pending":
                logger.warning(f"Task {task_id} is not pending (status: {task.status})")
                return

            try:
                # 
                task.status = "running"
                task.progress = 0
                db.commit()

                # 
                await kline_service.initialize()

                # （1）
                time_diff = task.end_time - task.start_time
                expected_records = int(time_diff.total_seconds() / 60)
                task.total_records = expected_records
                db.commit()

                logger.info(f"Task {task_id}: Collecting {expected_records} records for {task.symbol}")

                # （6）
                batch_hours = 6
                current_start = task.start_time
                collected_total = 0

                while current_start < task.end_time:
                    # 
                    current_end = min(
                        current_start + timedelta(hours=batch_hours),
                        task.end_time
                    )

                    logger.debug(f"Task {task_id}: Collecting batch {current_start} to {current_end}")

                    # 
                    collected_batch = await kline_service.collect_historical_klines(
                        task.symbol,
                        current_start,
                        current_end,
                        task.period
                    )

                    collected_total += collected_batch

                    # 
                    progress = min(
                        int((current_end - task.start_time).total_seconds() / time_diff.total_seconds() * 100),
                        100
                    )

                    task.progress = progress
                    task.collected_records = collected_total
                    db.commit()

                    logger.debug(f"Task {task_id}: Progress {progress}%, collected {collected_batch} records")

                    # 
                    current_start = current_end

                    # API
                    if current_start < task.end_time:
                        await asyncio.sleep(2)

                # 
                task.status = "completed"
                task.progress = 100
                task.collected_records = collected_total
                db.commit()

                logger.info(f"Task {task_id} completed successfully. Collected {collected_total} records.")

            except Exception as e:
                # 
                error_msg = str(e)
                logger.error(f"Task {task_id} failed: {error_msg}")

                task.status = "failed"
                task.error_message = error_msg
                db.commit()

    async def process_pending_tasks(self):
        """"""
        with SessionLocal() as db:
            # 
            pending_tasks = db.query(KlineCollectionTask).filter(
                KlineCollectionTask.status == "pending"
            ).order_by(KlineCollectionTask.created_at).limit(self.max_concurrent_tasks).all()

            if not pending_tasks:
                logger.debug("No pending backfill tasks found")
                return

            logger.info(f"Processing {len(pending_tasks)} pending backfill tasks")

            # 
            tasks = []
            for task in pending_tasks:
                task_coroutine = asyncio.create_task(
                    self.process_task(task.id),
                    name=f"backfill_task_{task.id}"
                )
                tasks.append(task_coroutine)

            # 
            await asyncio.gather(*tasks, return_exceptions=True)

    async def cleanup_old_tasks(self, days: int = 30):
        """"""
        cutoff_date = datetime.now() - timedelta(days=days)

        with SessionLocal() as db:
            # 30
            deleted = db.query(KlineCollectionTask).filter(
                KlineCollectionTask.created_at < cutoff_date,
                KlineCollectionTask.status.in_(["completed", "failed"])
            ).delete()

            db.commit()

            if deleted > 0:
                logger.info(f"Cleaned up {deleted} old backfill tasks")

    def get_task_status(self, task_id: int) -> Optional[dict]:
        """"""
        with SessionLocal() as db:
            task = db.query(KlineCollectionTask).filter(
                KlineCollectionTask.id == task_id
            ).first()

            if not task:
                return None

            return {
                "task_id": task.id,
                "exchange": task.exchange,
                "symbol": task.symbol,
                "status": task.status,
                "progress": task.progress,
                "total_records": task.total_records or 0,
                "collected_records": task.collected_records or 0,
                "error_message": task.error_message,
                "created_at": task.created_at,
                "updated_at": task.updated_at
            }