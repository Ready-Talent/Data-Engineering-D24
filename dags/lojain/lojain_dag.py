# implement Hello World DAG

import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def print_hello():
    print("hello from lojain")


dag = DAG(
    dag_id="hello_lojain",
    description="Simple tutorial DAG",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)


start_task = EmptyOperator(task_id="start", dag=dag)

hello_task = PythonOperator(
    task_id="hello_task",
    python_callable=print_hello,
    dag=dag,
)

end_task = EmptyOperator(task_id="end", dag=dag)

start_task >> hello_task >> end_task