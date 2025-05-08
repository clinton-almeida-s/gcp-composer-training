import csv
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define the CSV input file path
file_path = "/path/to/your/input_data.csv"

# Function to read the input file
def read_csv_file(file_path, **kwargs):
    with open(file_path, 'r') as file:
        csv_reader = csv.DictReader(file)
        rows = [row for row in csv_reader]
        print(f"Loaded data: {rows}")
        return rows

# Function to process each row dynamically
def process_row(row, **kwargs):
    print(f"Processing row: {row}")

# Define the DAG
with DAG(
    dag_id="dynamic_dag_with_csv_input",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:

    # Dynamic task generation
    def create_tasks(file_path):
        rows = read_csv_file(file_path)
        dynamic_tasks = []
        for row in rows:
            task = PythonOperator(
                task_id=f"process_{row['id']}",
                python_callable=process_row,
                op_args=[row],
                dag=dag
            )
            dynamic_tasks.append(task)
        return dynamic_tasks

    # Generate dynamic tasks based on the CSV input
    dynamic_tasks = create_tasks(file_path)

    # Set sequential dependencies
    for i in range(len(dynamic_tasks) - 1):
        dynamic_tasks[i] >> dynamic_tasks[i + 1]
