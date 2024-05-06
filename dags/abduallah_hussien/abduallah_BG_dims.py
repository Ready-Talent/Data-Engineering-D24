import logging
from datetime import datetime
from pathlib import Path
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


def print_hello():
    logging.info("Abduallah")
    return "printed"

dag = DAG(
    dag_id="Abduallah_transfer_all_dims_Dag",
    description="Transfer",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

start_task = EmptyOperator(task_id="start_task", dag=dag)

table_list = ['dim_product','dim_date','dim_customer']
table_list_sql = ['dim_product.sql','dim_date.sql','dim_customer.sql']

with TaskGroup('dynamic_tasks_group') as dynamic_group:
    tasks = [BigQueryExecuteQueryOperator(
            task_id=table_list[i],
            sql=table_list_sql[i],
            use_legacy_sql=False,
            dag=dag
        ) for i in range(len(table_list))]


end_task = EmptyOperator(task_id="end_task", dag=dag)


start_task >> dynamic_group >> end_task
