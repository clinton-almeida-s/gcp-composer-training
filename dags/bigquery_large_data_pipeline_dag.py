from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCheckOperator,
    BigQueryCreateEmptyTableOperator
)
from datetime import datetime

with DAG(
    dag_id="bigquery_large_data_pipeline",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["bigquery", "large-data"]
) as dag:

    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id="<DATASET_NAME>",
        table_id="<TABLE_NAME>",
        project_id="<PROJECT_ID>",
        schema_fields=[
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "salary", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "load_date", "type": "DATE", "mode": "REQUIRED"}
        ],
        time_partitioning={"field": "load_date"}
    )

    load_data = GCSToBigQueryOperator(
        task_id="load_data",
        bucket="<YOUR_BUCKET_NAME>",
        source_objects=["data/large_data.parquet"],
        destination_project_dataset_table="<PROJECT_ID>.<DATASET_NAME>.<TABLE_NAME>",
        write_disposition="WRITE_APPEND",
        source_format="PARQUET"
    )

    transform_data = BigQueryInsertJobOperator(
        task_id="transform_data",
        configuration={
            "query": {
                "query": """
                    SELECT 
                        id, 
                        name, 
                        salary * 1.2 AS bonus_salary,
                        CURRENT_DATE() AS load_date
                    FROM `<PROJECT_ID>.<DATASET_NAME>.<TABLE_NAME>`
                """,
                "useLegacySql": False,
            }
        }
    )

    validate_data = BigQueryCheckOperator(
        task_id="validate_data",
        sql="SELECT COUNT(*) FROM `<PROJECT_ID>.<DATASET_NAME>.<TABLE_NAME>` WHERE bonus_salary > 0",
        use_legacy_sql=False
    )

    create_table >> load_data >> transform_data >> validate_data
