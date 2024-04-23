import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator


def move_data():
    logging.info("Move data from postgres to gcs")
    return "printed"


dag = DAG(
    dag_id="move_from_postgres_to_gcs_reemaa",
    description="postgres_to gcs DAG",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

start_task = EmptyOperator(task_id="start_task", dag=dag)

get_data = PostgresToGCSOperator(
    task_id="get_data_reema",
    postgres_conn_id="postgres_conn_reema",
    sql="SELECT * from src01.order",
    bucket="postgres-to-gcs",
    export_format="CSV",
    filename="Reemaa/order.csv",
    gzip=False,
    dag=dag
)


DATASET_NAME="Reema_AirFlow"
TABLE_NAME="order"

load_csv = GCSToBigQueryOperator(
    task_id="gcs_to_bigquery",
    bucket="postgres-to-gcs",
    source_objects=["Reemaa/order.csv"],
    destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    field_delimiter=';',
    skip_leading_rows=1,
    ignore_unknown_values=True,
    max_bad_records= 100,
    dag=dag
)

end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> get_data >> load_csv >>end_task
