from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
}

def create_dynamic_task(task_id):
    def _task(**kwargs):
        print(f"Running dynamic task: {task_id}")
    return _task

with DAG(
    dag_id="advanced_dependencies_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    start_task = PythonOperator(
        task_id="start_task",
        python_callable=lambda: print("Starting the DAG")
    )

    dynamic_tasks = []
    for i in range(5):
        task = PythonOperator(
            task_id=f"dynamic_task_{i}",
            python_callable=create_dynamic_task(f"dynamic_task_{i}")
        )
        dynamic_tasks.append(task)

    # Define dependencies
    start_task >> dynamic_tasks[0]
    dynamic_tasks[0] >> [dynamic_tasks[1], dynamic_tasks[2]]
    dynamic_tasks[1] >> dynamic_tasks[3]
    dynamic_tasks[2] >> dynamic_tasks[3]
    dynamic_tasks[3] >> dynamic_tasks[4]
