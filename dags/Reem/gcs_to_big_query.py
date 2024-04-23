import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


def move_data():
    logging.info("Move data from GCS to bigQuery")
    return "printed"


dag = DAG(
    dag_id="move_from_gcs_to_bigquery_reemaa",
    description="GCS to BQ DAG",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)


DATASET_NAME="Reema_Airflow"
TABLE_NAME="chicago-taxi-test-de24"

start_task = EmptyOperator(task_id="start_task", dag=dag)

load_csv = GCSToBigQueryOperator(
    task_id="gcs_to_bigquery",
    bucket="chicago-taxi-test-de24",
    source_objects=["data/*.csv"],
    destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    field_delimiter=';',
    skip_leading_rows=1,
    ignore_unknown_values=True,
    dag=dag
)

end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> load_csv >> end_task
