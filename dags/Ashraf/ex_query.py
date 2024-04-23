
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

dag = DAG(
    dag_id="BigQuery_create_Query_01",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

start_task = EmptyOperator(task_id="start_task", dag=dag)


ecx_query = BigQueryExecuteQueryOperator(
    task_id="BigQuery_Execute_Query",
    sql= 'create_table.sql',
    dag=dag
)

end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> ecx_query >> end_task
