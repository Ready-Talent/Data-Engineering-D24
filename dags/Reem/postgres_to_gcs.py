import logging
from datetime import datetime
from webbrowser import get

from airflow import DAG
from airflow.operators.empty import EmptyOperator
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
    source_format="CSV",
    filename="Reemaa/order.csv",
    gzip=False,
    dag=dag
)

end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> get_data >> end_task
