import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
def print_hello():
    logging.info("Abduallah")
    return "printed"


dag = DAG(
    dag_id="Abduallah_Transfer_Dag",
    description="Transfer",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

start_task = EmptyOperator(task_id="start_task", dag=dag)

transfer_data = PostgresToGCSOperator(
        task_id="transfer_data_to_GCS",
        postgres_conn_id='Abduallah_03_postgres',
        sql='select * from src01."order"',
        bucket='postgres-to-gcs',
        filename='abduallah/order.csv',
        gzip=False,
    )

load_order = GCSToBigQueryOperator(
    task_id="gcs_to_bigquery",
    bucket="postgres-to-gcs",
    source_objects=["abduallah/order.csv"],
    destination_project_dataset_table=f"Abduallah_03.order",
    create_disposition = "CREATE_IF_NEEDED",
    write_disposition="WRITE_TRUNCATE"
)

end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> transfer_data >> end_task