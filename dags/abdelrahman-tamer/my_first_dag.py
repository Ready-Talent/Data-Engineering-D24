# implement Hello World DAG

import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def print_hello():
    print("hello world")
    

dag = DAG(dag_id="my_first_dag", schedule_interval=None, start_date=datetime(2021, 1, 1), catchup=False)


task1 = EmptyOperator(task_id="start_task", dag=dag)
task2 = PythonOperator(task_id="print_hello_task", python_callable=print_hello, dag=dag)
task3 = EmptyOperator(task_id="end_task", dag=dag)

task1 >> task2 >> task3




