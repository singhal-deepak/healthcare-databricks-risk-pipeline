from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

sys.path.append("/opt/airflow/scripts")

from extract_orders_api import extract_orders
from validate_orders import validate_orders
from log_ingestion_metadata import log_ingestion_metadata

default_args = {
    "owner": "deepak",
    "retries": 2
}

with DAG(
    dag_id="ecommerce_orders_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["ecommerce", "ingestion", "production-style"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_orders",
        python_callable=extract_orders
    )

    validate_task = PythonOperator(
        task_id="validate_orders",
        python_callable=validate_orders
    )

    metadata_task = PythonOperator(
        task_id="log_ingestion_metadata",
        python_callable=log_ingestion_metadata
    )

    extract_task >> validate_task >> metadata_task
