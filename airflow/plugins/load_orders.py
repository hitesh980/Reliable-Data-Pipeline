"""Robust loader for raw_orders.

Approach:
- Generate CSV (calls generate_orders)
- Create `raw_orders` if missing
- Create a TEMP staging table `raw_orders_stage`
- COPY CSV into staging table using `COPY ... FROM STDIN`
- INSERT ... SELECT FROM staging ON CONFLICT DO UPDATE (UPSERT)

This is safe to run inside the Airflow container (uses host `postgres`).
"""

import os
import logging
import sys
import psycopg2

# ensure plugins can import each other when executed inside container
sys.path.insert(0, '/opt/airflow/plugins')
from generate_orders import generate_orders

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

CSV_PATH = os.environ.get('ORDERS_CSV_PATH', '/opt/airflow/data/raw/orders.csv')


def load_orders(num_records: int = 500):
    """Generate synthetic orders CSV and load into Postgres with upsert.

    - Creates `raw_orders` if it does not exist
    - Loads CSV into a TEMP staging table using COPY
    - Upserts from staging into `raw_orders`
    """
    # Generate data file
    generate_orders(num_records=num_records)

    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"Orders CSV not found at {CSV_PATH}")

    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            host='postgres',
            port=5432,
            database='analytics',
            user='data_user',
            password='data_pass',
        )
        cur = conn.cursor()

        # Create main table (use BIGINT for order_id to avoid overflow)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_orders (
                order_id BIGINT PRIMARY KEY,
                user_id INT,
                order_amount NUMERIC,
                currency TEXT,
                order_ts TIMESTAMP,
                status TEXT
            );
            """
        )
        conn.commit()

        # Create a temporary staging table; it will be dropped at end of session
        cur.execute(
            """
            CREATE TEMP TABLE raw_orders_stage (
                order_id BIGINT,
                user_id DOUBLE PRECISION,
                order_amount NUMERIC,
                currency TEXT,
                order_ts TIMESTAMP,
                status TEXT
            );
            """
        )

        # COPY CSV into staging table
        with open(CSV_PATH, 'r') as f:
            cur.copy_expert(
                "COPY raw_orders_stage (order_id, user_id, order_amount, currency, order_ts, status) FROM STDIN WITH CSV HEADER",
                f,
            )

        # Upsert from staging into raw_orders. Cast user_id to integer if present.
        cur.execute(
            """
            INSERT INTO raw_orders (order_id, user_id, order_amount, currency, order_ts, status)
            SELECT
                order_id::BIGINT,
                CASE WHEN user_id IS NULL THEN NULL ELSE (user_id::numeric)::INT END,
                order_amount::NUMERIC,
                currency,
                order_ts,
                status
            FROM raw_orders_stage
            ON CONFLICT (order_id) DO UPDATE SET
                user_id = EXCLUDED.user_id,
                order_amount = EXCLUDED.order_amount,
                currency = EXCLUDED.currency,
                order_ts = EXCLUDED.order_ts,
                status = EXCLUDED.status;
            """
        )

        conn.commit()
        logger.info("Loaded orders from %s into raw_orders", CSV_PATH)

    except Exception:
        if conn:
            conn.rollback()
        logger.exception("Failed to load orders")
        raise
    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        if conn:
            conn.close()


if __name__ == '__main__':
    load_orders()
