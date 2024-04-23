import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

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
        postgres_conn_id=Abduallah_03_postgres,
        sql='select * from src01."order"',
        bucket='postgres-to-gcs',
        filename='abduallah',
        gzip=False,
    )

end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> transfer_data >> end_task