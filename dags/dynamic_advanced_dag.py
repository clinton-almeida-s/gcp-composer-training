from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Function for dynamic task generation
def create_dynamic_task(task_id):
    def dynamic_task_function(**kwargs):
        print(f"Running dynamic task: {task_id}")
    return dynamic_task_function

# Define the DAG
with DAG(
    dag_id="dynamic_dag_example",
    description="Dynamic DAG with advanced dependencies",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:
    
    # Static task
    start_task = PythonOperator(
        task_id="start_task",
        python_callable=lambda: print("Starting the DAG"),
        dag=dag
    )

    # Dynamically create tasks
    dynamic_tasks = []
    for i in range(5):  # Create 5 dynamic tasks
        dynamic_task = PythonOperator(
            task_id=f"dynamic_task_{i}",
            python_callable=create_dynamic_task(f"dynamic_task_{i}"),
            dag=dag
        )
        dynamic_tasks.append(dynamic_task)

    # Define advanced task dependencies
    start_task >> dynamic_tasks[0]
    dynamic_tasks[0] >> [dynamic_tasks[1], dynamic_tasks[2]]  # Parallel dependencies
    dynamic_tasks[1] >> dynamic_tasks[3]  # Sequential dependency
    dynamic_tasks[2] >> dynamic_tasks[3]
