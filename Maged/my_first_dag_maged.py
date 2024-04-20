import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def print_hello() :
    logging.error("hello worldd")
    return "printed"

dag = DAG(
    dag_id="hello_world",
    description="first trial",
    schedule_interval=None,
    start_date=datetime(2024, 20, 4),
    catchup=False,
)

first_task = EmptyOperator(task_id="first_task", dag=dag)

last_task = EmptyOperator(task_id="last_task", dag=dag)

middle_task = PythonOperator(
    task_id="middle_task",
    python_callable=print_hello,
    dag=dag,
)

first_task >> middle_task >> last_task