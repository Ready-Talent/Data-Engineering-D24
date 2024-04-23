import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator

postgres_to_gcs = DAG(
    dag_id="Maged_second_Dag",
    description="second trial",
    schedule_interval=None,
    start_date=datetime(2024, 4, 20),
    catchup=False,
) 

GCS_BUCKET = 'postgres-to-gcs/Maged'
GCS_OBJECT_PATH = 'Maged'
SOURCE_TABLE_NAME = 'dim_customer'
FILE_FORMAT = 'csv'
#POSTGRESS_CONNECTION_ID = 'postgres'

postgres_to_gcs_task = PostgresToGCSOperator(
    task_id ='postgres_to_gcs',
    postgres_conn_id="postgresConnection_Maged",
    bucket=GCS_BUCKET,
    sql = 'SELECT * FROM dim_customer'
    filename=f'{GCS_OBJECT_PATH}/{SOURCE_TABLE_NAME}.{FILE_FORMAT}',
    export_format='csv',
    gzip=False,
    use_server_side_cursor=False,
)

first_task = EmptyOperator(task_id="first_task", dag=postgres_to_gcs)

last_task = EmptyOperator(task_id="last_task", dag=postgres_to_gcs)


first_task >> postgres_to_gcs_task >> last_task