# implement Hello World DAG

import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator



def print_hello():
    logging.info("Galal")
    return "printed"


dag = DAG(
    dag_id="Galal_Transfer_Dag",
    description="Transfer",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

DATASET_NAME="Galal_Airflow"
TABLE_NAME="chicago-taxi-test-de24"

start_task = EmptyOperator(task_id="start_task", dag=dag)

load_csv = GCSToBigQueryOperator(
    task_id="gcs_to_bigquery_example_Galal",
    bucket="chicago-taxi-test-de24",
    source_objects=["data/*.csv"],
    destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",create_disposition='CREATE_IF_NEEDED',
    write_disposition="WRITE_TRUNCATE",
    field_delimiter=';',
    skip_leading_rows =1,
    ignore_unknown_values=True,
    max_bad_records=1000

)

end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> load_csv >> end_task