import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

dag = DAG(
    dag_id="GCS_TO_BIGQUERY_OT",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

start_task = EmptyOperator(task_id="start_task", dag=dag)

load_csv = GCSToBigQueryOperator(
    task_id="gcs_to_bigquery",
    bucket="chicago-taxi-test-de24",
    source_objects=["data/*.csv"],
    destination_project_dataset_table="chicago_taxi_OT.chicago-taxi-test-de24_OT",
    dag=dag,
    field_delimiter=";",
    max_bad_records=1000000,
    ignore_unknown_values=True,
    skip_leading_rows=1,
    source_format="CSV",
)
end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> load_csv >> end_task
