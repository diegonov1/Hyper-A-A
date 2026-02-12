#!/usr/bin/env python3
"""
Migration: Add kline collection system tables and optimize crypto_klines
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, text
from database.connection import DATABASE_URL

def migrate():
    """Add kline collection system tables and optimize existing ones"""
    engine = create_engine(DATABASE_URL)

    with engine.connect() as conn:
        # 1. crypto_klines（）
        conn.execute(text("""
            DO $$
            BEGIN
                -- 
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'uq_crypto_klines_unique'
                ) THEN
                    -- 
                    ALTER TABLE crypto_klines
                    ADD CONSTRAINT uq_crypto_klines_unique
                    UNIQUE (exchange, symbol, timestamp, period);
                END IF;
            END $$;
        """))

        # 2. crypto_klines
        conn.execute(text("""
            -- 
            CREATE INDEX IF NOT EXISTS idx_crypto_klines_exchange_symbol_time
            ON crypto_klines(exchange, symbol, timestamp DESC);

            -- 
            CREATE INDEX IF NOT EXISTS idx_crypto_klines_timestamp_range
            ON crypto_klines(timestamp DESC) WHERE exchange IS NOT NULL;
        """))

        # 3. K
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS kline_collection_tasks (
                id SERIAL PRIMARY KEY,
                exchange VARCHAR(20) NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                start_time TIMESTAMP NOT NULL,
                end_time TIMESTAMP NOT NULL,
                period VARCHAR(10) NOT NULL DEFAULT '1m',
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                progress INTEGER NOT NULL DEFAULT 0,
                total_records INTEGER DEFAULT 0,
                collected_records INTEGER DEFAULT 0,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))

        # 4. 
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_kline_tasks_status
            ON kline_collection_tasks(status, created_at DESC);

            CREATE INDEX IF NOT EXISTS idx_kline_tasks_exchange_symbol
            ON kline_collection_tasks(exchange, symbol);
        """))

        # 5. （）
        conn.execute(text("""
            CREATE OR REPLACE VIEW kline_coverage_stats AS
            SELECT
                exchange,
                symbol,
                period,
                MIN(timestamp) as earliest_time,
                MAX(timestamp) as latest_time,
                COUNT(*) as total_records,
                (MAX(timestamp) - MIN(timestamp)) as time_span_seconds,
                ROUND(
                    (COUNT(*) * 60.0) / NULLIF((MAX(timestamp) - MIN(timestamp)), 0) * 100, 2
                ) as coverage_percentage
            FROM crypto_klines
            WHERE period = '1m' AND timestamp IS NOT NULL
            GROUP BY exchange, symbol, period
            HAVING COUNT(*) > 1;
        """))

        conn.commit()
        print("✅ K")
        print("   - crypto_klines")
        print("   - kline_collection_tasks")
        print("   - ")
        print("   - ")

if __name__ == "__main__":
    migrate()