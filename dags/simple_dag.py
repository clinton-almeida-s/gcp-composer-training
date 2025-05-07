from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='simple_composer_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['example', 'composer'],
    description='A simple DAG to test Composer',
) as dag:

    start = BashOperator(
        task_id='start_task',
        bash_command='echo "Starting the DAG run!"'
    )

    process = BashOperator(
        task_id='process_task',
        bash_command='echo "Processing data..."'
    )

    end = BashOperator(
        task_id='end_task',
        bash_command='echo "DAG run complete."'
    )

    start >> process >> end
