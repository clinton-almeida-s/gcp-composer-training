from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime

def decide_next_task(**kwargs):
    return "task_branch_a" if kwargs['execution_date'].day % 2 == 0 else "task_branch_b"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
}

with DAG(
    dag_id='branching_dag',
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    branching_task = BranchPythonOperator(
        task_id='branching_task',
        python_callable=decide_next_task,
        provide_context=True
    )

    task_branch_a = PythonOperator(
        task_id='task_branch_a',
        python_callable=lambda: print("Task in Branch A executed")
    )

    task_branch_b = PythonOperator(
        task_id='task_branch_b',
        python_callable=lambda: print("Task in Branch B executed")
    )

    branching_task >> [task_branch_a, task_branch_b]
