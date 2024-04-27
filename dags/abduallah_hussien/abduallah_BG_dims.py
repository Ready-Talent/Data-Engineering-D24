import logging
from datetime import datetime
from pathlib import Path
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator

def print_hello():
    logging.info("Abduallah")
    return "printed"

dag = DAG(
    dag_id="Abduallah_transfer_dims_Dag",
    description="Transfer",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)
import os
start_task = EmptyOperator(task_id="start_task", dag=dag)

parent_path = str(Path(__file__).parent)
path = os.path.join(parent_path, "product.json" )

create_table = BigQueryCreateEmptyTableOperator(
    task_id="create_product_table",
    dataset_id='Data_Platform_Abduallah',
    table_id="dim_product",
    table_resource=os.open(path)
)

end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> create_table >> end_task
