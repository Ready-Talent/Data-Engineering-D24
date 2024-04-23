import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator


def move_data():
    logging.info("Hello world by Abdelrahman Essam")
    return "printed"


dag = DAG(
    dag_id="from_postgresql_to_bigquery_abdelrahman_essam",
    description="Simple tutorial DAG",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

start_task = EmptyOperator(task_id="start_task", dag=dag)

load_csv = GCSToBigQueryOperator(
    task_id="gcs_to_bigquery_example",
    bucket="postgres-to-gcs",
    source_objects=["essam/orders.csv"],
    destination_project_dataset_table="fromPgToBq.orders",
    write_disposition="WRITE_TRUNCATE",
    create_disposition="CREATE_IF_NEEDED",
    skip_leading_rows=1,
    schema_object=None,
    field_delimiter=';',
    dag=dag,
    max_bad_records=10000
)

get_data = PostgresToGCSOperator(
    task_id='postgres_to_gcs',
    sql='SELECT * FROM src01.order;',
    bucket="postgres-to-gcs",
    filename="essam/orders.csv",
    export_format='CSV',  # You can change the export format as needed
    postgres_conn_id="pg_conn_essam",
    field_delimiter=';',  # Optional, specify field delimiter for CSV
    dag=dag,
)

end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> get_data >> load_csv >> end_task
