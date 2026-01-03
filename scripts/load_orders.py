import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch

# Import generate_orders from airflow/plugins (which is in PYTHONPATH inside Airflow)
import sys
sys.path.insert(0, '/opt/airflow/plugins')
from generate_orders import generate_orders

REQUIRED_COLUMNS = {
     "order_id",
    "user_id",
    "order_amount",
    "currency",
    "order_ts",
    "status"
}
def validate_schema(df:pd.DataFrame):
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
def load_orders():

    #generate fresh synthic data
    generate_orders(num_records=500)


    # Read generated CSV
    df = pd.read_csv("/opt/airflow/data/raw/orders.csv")
    validate_schema(df)

    #connect to postgres
    conn = psycopg2.connect(
        host = "postgres",
        database = "analytics",
        user = "data_user",
        password = "data_pass"
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_orders (
            order_id INT PRIMARY KEY ,
            user_id INT,
            order_amount NUMERIC,
            currency TEXT,
            order_ts TIMESTAMP,
            status TEXT
        );
                """)
    
    # Prepare UPSERT query
    upsert_query = """
       INSERT INTO raw_orders(
       order_id, user_id, order_amount, currency, order_ts, status
       )
       VALUES (%s, %s, %s, %s, %s, %s)
       ON CONFLICT (order_id)  DO UPDATE SET
            user_id = EXCLUDED.user_id,
            order_amount = EXCLUDED.order_amount,
            currency = EXCLUDED.currency,
            order_ts = EXCLUDED.order_ts,
            status = EXCLUDED.status
    """

    records = list(df.itertuples(index = False ,name = None))


    #Bulk load 
    execute_batch(cur,upsert_query,records,page_size =100)

    
    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    load_orders()