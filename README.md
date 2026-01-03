# Reliable Data Pipeline – Orders

## Overview
This project demonstrates a production-style reliable batch data pipeline using
Airflow, Postgres, and dbt.

Synthetic order data is generated, ingested into Postgres, transformed using dbt,
and validated using data quality tests.

## Architecture
CSV (Synthetic)  
→ Airflow (PythonOperator)  
→ Postgres (raw_orders)  
→ dbt staging (stg_orders)  
→ dbt marts (fct_orders)  
→ dbt tests (fail-fast)

## Reliability Features
- Idempotent loads
- Schema validation before load
- dbt tests for:
  - not null constraints
  - uniqueness
  - value ranges
- Test-after-run pattern in Airflow
- Clear separation of ingestion vs transformation

## Technologies
- Apache Airflow
- PostgreSQL
- dbt
- Docker
- Python (pandas, psycopg2, Faker)

## How to Run
1. docker compose up -d
2. Open Airflow UI
3. Trigger `reliable_orders_dag`
