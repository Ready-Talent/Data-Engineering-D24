# implement Hello World DAG

import logging
from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator



DATASET_NAME="lojain_fromgcs"
TABLE_NAME="order"

dag = DAG(
    dag_id="lojain_pgtobq",
    description="Simple tutorial DAG",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)


start_task = EmptyOperator(task_id="start", dag=dag)

pgtogcs= PostgresToGCSOperator(
        task_id="get_pg_data",
        postgres_conn_id="lojain_pg_connection",
        sql="SELECT * FROM src01.order;",
        bucket="postgres-to-gcs",
        filename="Lojain/order.csv",
        export_format='CSV')


gcstobq=GCSToBigQueryOperator(
    task_id="transfer_to_bq",
    bucket="postgres-to-gcs",
    source_objects=["Lojain/order.csv"],
    destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
    create_disposition='CREATE_IF_NEEDED',
    write_disposition="WRITE_TRUNCATE",
    autodetect = True
    skip_leading_rows=1
)




end_task = EmptyOperator(task_id="end", dag=dag)

start_task >> pgtogcs>> gcstobq >> end_task

