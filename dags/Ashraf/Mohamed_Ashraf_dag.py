import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def print_hello():
    logging.info("Hello world")
    return "printed"

dag=DAG(
    dag_id="Ashraf_dag",
    description="Simple tutorial DAG",
    schedule_interval=None,
    start_date=datetime(2024, 4, 1),
    catchup=False,
    )

start_task = EmptyOperator(task_id="start_task", dag=dag)

hello_task = PythonOperator(
    task_id="hello_task",
    python_callable=print_hello,
    dag=dag,
)

end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> hello_task >> end_task