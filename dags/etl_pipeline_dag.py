from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPullOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.sql_to_gcs import CloudSQLToGCSOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import random

def fetch_realtime_prices():
    stock_symbols = ["GOOG", "AAPL", "TSLA", "MSFT", "AMZN"]
    prices = {symbol: round(random.uniform(100, 500), 2) for symbol in stock_symbols}
    print("Fetched prices:", prices)
    return prices

with DAG(
    dag_id="etl_pipeline",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "multi-source"]
) as dag:

    load_csv_to_bigquery = GCSToBigQueryOperator(
        task_id="load_csv_to_bigquery",
        bucket="<YOUR_BUCKET_NAME>",
        source_objects=["data/batch_data.csv"],
        destination_project_dataset_table="<PROJECT_ID>.etl_dataset.raw_data",
        write_disposition="WRITE_APPEND"
    )

    pull_pubsub_messages = PubSubPullOperator(
        task_id="pull_pubsub_messages",
        project_id="<YOUR_PROJECT_ID>",
        subscription="realtime-events-sub",
        max_messages=10
    )

    load_sql_to_gcs = CloudSQLToGCSOperator(
        task_id="load_sql_to_gcs",
        sql="SELECT id, name, salary FROM employees;",
        bucket="<YOUR_BUCKET_NAME>",
        filename="data/sql_data.json"
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
                    FROM `<PROJECT_ID>.etl_dataset.raw_data`
                """,
                "useLegacySql": False,
            }
        }
    )

    [fetch_api_task, load_sql_to_gcs, load_csv_to_bigquery, pull_pubsub_messages] >> transform_data
