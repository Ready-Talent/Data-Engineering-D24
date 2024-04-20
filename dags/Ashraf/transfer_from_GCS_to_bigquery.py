
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

dag = DAG(
    dag_id="transfer_from_GCS_to_biqquery",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

start_task = EmptyOperator(task_id="start_task", dag=dag)

load_csv = GCSToBigQueryOperator(
    task_id="gcs_to_bigquery_example",
    bucket="chicago-taxi-test-de24",
    source_objects=["data/*.csv"],
    destination_project_dataset_table="chicago_taxi_01.test_table",
)
end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> load_csv >> end_task