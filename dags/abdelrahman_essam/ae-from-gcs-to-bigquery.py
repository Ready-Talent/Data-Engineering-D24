import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


def move_data():
    logging.info("Hello world by Abdelrahman Essam")
    return "printed"


dag = DAG(
    dag_id="from_gcs_to_bigquery_abdelrahman_essam",
    description="Simple tutorial DAG",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

start_task = EmptyOperator(task_id="start_task", dag=dag)

load_csv = GCSToBigQueryOperator(
    task_id="gcs_to_bigquery_example",
    bucket="chicago-taxi-test-de24",
    source_objects=["data/*.csv"],
    destination_project_dataset_table=f"chicago_11.chicago_taxi",
    schema_fields=[
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "post_abbr", "type": "STRING", "mode": "NULLABLE"},
    ],
    write_disposition="WRITE_TRUNCATE",
    create_disposition="CREATE_IF_NEEDED"
)

end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> load_csv >> end_task
