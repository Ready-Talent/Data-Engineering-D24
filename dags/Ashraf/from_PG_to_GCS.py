
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

PG_SCHEMA = "src01"
PG_TABLE = "order"
BQ_BUCKET = "postgres-to-gcs"
FILENAME = "Ashraf/ORDER.csv"
PG_CONN_ID = "postgres_01"

dag = DAG(
    dag_id="PG_to_GCS",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

start_task = EmptyOperator(task_id="start_task", dag=dag)

postgres_to_gcs = PostgresToGCSOperator(
    task_id = 'postgres_stock_to_gcs',
    sql = f'SELECT * FROM "{PG_SCHEMA}"."{PG_TABLE}";',
    bucket = BQ_BUCKET,
    filename = FILENAME,
    postgres_conn_id = PG_CONN_ID,
    export_format = 'CSV',  # You can change the export format as needed
    dag = dag,
)


end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> postgres_to_gcs >> end_task
