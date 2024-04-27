import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


def move_data():
    logging.info("Dim_Customer_Reemaa")
    return "printed"


dag = DAG(
    dag_id= "adding_dim_customer_Reemaa",
    description="Simple tutorial DAG",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

start_task = EmptyOperator(task_id="start_task", dag=dag)



insert_dim_customer = BigQueryInsertJobOperator(
    task_id="creating_dim_customer",
    configuration={
        "query": "/sql/populate_dim_customer.sql",
        "useLegacySql": False,
        "timeoutMs": 1000000,

    },
)

end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> insert_dim_customer >> end_task