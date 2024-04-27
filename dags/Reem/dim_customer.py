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



create_dim_customer = BigQueryInsertJobOperator(
    task_id="creating_dim_customer",
    sql = "sql/create_dim_customer.sql",
    use_legacy_sql=False,
    dag=dag,
)


populate_dim_customer = BigQueryInsertJobOperator(
    task_id="insert_in_dim_customer",
    sql = "sql/populate_dim_customer.sql",\
    use_legacy_sql=False,
    dag=dag,
)

end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> create_dim_customer >> populate_dim_customer >> end_task