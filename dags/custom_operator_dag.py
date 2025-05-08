from airflow import DAG
from datetime import datetime
from custom_operator import MyCustomOperator  # Import the custom operator

with DAG(
    dag_id="custom_operator_example",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:

    custom_task = MyCustomOperator(
        task_id="print_message",
        message="Hello from MyCustomOperator!",
        dag=dag
    )
