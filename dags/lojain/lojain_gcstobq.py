# implement Hello World DAG

import logging
from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator




dag = DAG(
    dag_id="lojain_gcstobq",
    description="Simple tutorial DAG",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)


start_task = EmptyOperator(task_id="start", dag=dag)
DATASET_NAME="lojain_fromgcs"
TABLE_NAME="taxi"

load_csv = GCSToBigQueryOperator(
    task_id="lojain_gcstobq",
    bucket="chicago-taxi-test-de24",

    source_objects=["chicago-taxi-test-de24/data/*.csv"],
    destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
    schema_fields=[
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "post_abbr", "type": "STRING", "mode": "NULLABLE"},
    ],
    create_disposition='CREATE_IF_NEEDED',
    write_disposition="WRITE_TRUNCATE",
    autodetect = True
)

end_task = EmptyOperator(task_id="end", dag=dag)

start_task >> load_csv >> end_task

