from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

BUCKET_NAME = 'your-bucket-name'
SOURCE_OBJECT = 'data/sample.csv'
BQ_DATASET = 'your_dataset'
BQ_TABLE = 'sample_table'

def log_upload():
    print(f"Assuming file gs://{BUCKET_NAME}/{SOURCE_OBJECT} is ready for load.")

with DAG(
    dag_id='gcs_to_bq_example',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['composer', 'gcs', 'bigquery'],
    description='DAG to load data from GCS to BigQuery and run a query',
) as dag:

    log_gcs = PythonOperator(
        task_id='log_upload_step',
        python_callable=log_upload
    )

    load_to_bq = GCSToBigQueryOperator(
        task_id='load_gcs_to_bq',
        bucket=BUCKET_NAME,
        source_objects=[SOURCE_OBJECT],
        destination_project_dataset_table=f'{BQ_DATASET}.{BQ_TABLE}',
        schema_fields=[
            {'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        source_format='CSV',
    )

    query = BigQueryInsertJobOperator(
        task_id='run_query',
        configuration={
            "query": {
                "query": f"SELECT name, COUNT(*) as count FROM `{BQ_DATASET}.{BQ_TABLE}` GROUP BY name",
                "useLegacySql": False,
            }
        }
    )

    log_gcs >> load_to_bq >> query
