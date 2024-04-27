# implement Hello World DAG

import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator 

from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

#hhhhhhhhhh




dag = DAG(
    dag_id="Create_Dim_Galal",
    description="Transfer",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

start_task = EmptyOperator(task_id="start_task", dag=dag)
end_task = EmptyOperator(task_id="end_task", dag=dag)
create_dim=BigQueryExecuteQueryOperator(task_id='Create_Dim',sql='sql/Create_dim.sql',dag=dag,legacy_sql=True)
start_task >> create_dim >>end_task