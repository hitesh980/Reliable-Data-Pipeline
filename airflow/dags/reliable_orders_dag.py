from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys

# Add plugins directory to path so we can import load_orders
sys.path.insert(0, '/opt/airflow/plugins')
from load_orders import load_orders

default_args = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

with DAG(
    dag_id='reliable_orders_dag',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['data-pipeline', 'orders'],
) as dag:
    
    load_task = PythonOperator(
        task_id='load_orders_to_postgres',
        python_callable=load_orders,
        doc='Generate synthetic orders and load to Postgres raw_orders table',
        retries=2,
    )
    
    dbt_run_staging = BashOperator(
        task_id = "dbt_run_staging",
        bash_command = "cd /opt/airflow/dbt && dbt run --models stg_orders",
        retries =1,
    )

    dbt_test_staging = BashOperator(
        task_id = "dbt_test",
        bash_command = "cd /opt/airflow/dbt && dbt test --models stg_orders",
    )

    dbt_run_mart = BashOperator(
        task_id = "dbt_run_mart",
        bash_command = "cd /opt/airflow/dbt && dbt run --models fct_orders",
    )

    dbt_test_mart = BashOperator(
        task_id = "dbt_test_mart",
        bash_command="cd /opt/airflow/dbt && dbt test --models fct_orders",
    )

    load_task \
    >> dbt_run_staging \
    >> dbt_test_staging \
    >> dbt_run_mart \
    >> dbt_test_mart
