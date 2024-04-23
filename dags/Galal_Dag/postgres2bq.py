# implement Hello World DAG

import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator





dag = DAG(
    dag_id="Pg2Bq_Galal",
    description="Transfer",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

DATASET_NAME="Galal_Airflow"
TABLE_NAME="chicago-taxi-test-de24"
bucket="postgres-to-gcs",

start_task = EmptyOperator(task_id="start_task", dag=dag)
get_data = PostgresToGCSOperator(
    task_id="postgres_to_gcs_example_Galal",
    postgres_conn_id="Galal_Pg_Connection",
    sql="SELECT * from src09.product",
    bucket=bucket,
    filename="Galal/",
    gzip=False,
    export_format = 'CSV',
)

load_csv = GCSToBigQueryOperator(
    task_id="gcs_to_bigquery_example_Galal",
    bucket="chicago-taxi-test-de24",
    source_objects=["data/*.csv"],
    destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",create_disposition='CREATE_IF_NEEDED',
    write_disposition="WRITE_TRUNCATE",
    field_delimiter=';', 
    skip_leading_rows =1, # skip Headerss 
    ignore_unknown_values=True,
    max_bad_records=100000

)

end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> get_data >> end_task