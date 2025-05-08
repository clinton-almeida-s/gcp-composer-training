from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPullOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.sql_to_gcs import SqlToGCSOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import random
import logging

# Default arguments
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# Dummy API fetch function
def fetch_realtime_prices(**context):
    stock_symbols = ["GOOG", "AAPL", "TSLA", "MSFT", "AMZN"]
    prices = {symbol: round(random.uniform(100, 500), 2) for symbol in stock_symbols}
    logging.info("Fetched prices: %s", prices)
    return prices

with DAG(
    dag_id="etl_pipeline",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "multi-source"],
    description="ETL pipeline with multiple data sources and transformation"
) as dag:

    load_csv_to_bigquery = GCSToBigQueryOperator(
        task_id="load_csv_to_bigquery",
        bucket="your-bucket-name",
        source_objects=["data/batch_data.csv"],
        destination_project_dataset_table="your-project-id.etl_dataset.raw_data",
        write_disposition="WRITE_APPEND",
        skip_leading_rows=1,
        source_format="CSV",
        gcp_conn_id="google_cloud_default"
    )

    pull_pubsub_messages = PubSubPullOperator(
        task_id="pull_pubsub_messages",
        project_id="your-project-id",
        subscription="realtime-events-sub",
        max_messages=10,
        gcp_conn_id="google_cloud_default"
    )

    load_sql_to_gcs = SqlToGCSOperator(
        task_id="load_sql_to_gcs",
        sql="SELECT id, name, salary FROM employees;",
        bucket="your-bucket-name",
        filename="data/sql_data.json",
        export_format="json",
        cloud_sql_conn_id="your-cloudsql-conn-id",
        gcp_conn_id="google_cloud_default"
    )

    fetch_api_task = PythonOperator(
        task_id="fetch_api_data",
        python_callable=fetch_realtime_prices
    )

    transform_data = BigQueryInsertJobOperator(
        task_id="transform_data",
        configuration={
            "query": {
                "query": """
                    SELECT id, name, salary * 1.1 AS adjusted_salary
                    FROM `your-project-id.etl_dataset.raw_data`
                """,
                "useLegacySql": False,
            }
        },
        location="US",
        gcp_conn_id="google_cloud_default"
    )

    # Set dependencies
    [fetch_api_task, load_sql_to_gcs, load_csv_to_bigquery, pull_pubsub_messages] >> transform_data
