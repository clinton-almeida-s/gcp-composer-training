from airflow import DAG
from datetime import datetime
from plugins.operators.api_operator import APIOperator

with DAG(
    dag_id="api_operator_example",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:

    api_task = APIOperator(
        task_id="fetch_api_data",
        endpoint="https://jsonplaceholder.typicode.com/posts/1",
        dag=dag
    )
