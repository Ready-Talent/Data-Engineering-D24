import logging
from datetime import datetime
from pathlib import Path
from airflow import DAG
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

table_list = ['dim_product','dim_customer','junk_dim','fact_sales','dim_date']
table_list_sql = ['dim_product.sql','dim_customer.sql','junk_dim.sql','fact_sales.sql','dim_date']

tasks = [BigQueryExecuteQueryOperator(
        task_id=f'create_and_insert_{table_list[i]}',
        sql=table_list_sql[i],
        use_legacy_sql=False,
        dag=dag
    ) for i in range(len(table_list))]


end_task = EmptyOperator(task_id="end_task", dag=dag)


start_task >> tasks[0]
for i in range(1, len(tasks)):
    tasks[i] << tasks[i-1]
tasks[-1] >> end_task
