from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
}

with DAG(
    dag_id='taskgroup_parallel_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    start_task = PythonOperator(
        task_id='start_task',
        python_callable=lambda: print("Starting parallel tasks")
    )

    with TaskGroup("parallel_tasks") as group:
        task1 = PythonOperator(
            task_id='parallel_task_1',
            python_callable=lambda: print("Parallel Task 1 executed")
        )

        task2 = PythonOperator(
            task_id='parallel_task_2',
            python_callable=lambda: print("Parallel Task 2 executed")
        )

    start_task >> group
