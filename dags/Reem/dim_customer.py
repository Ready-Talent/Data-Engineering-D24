import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)

dag = DAG(
    dag_id= "adding_dim_customer_Reemaa",
    description="Simple tutorial DAG",
    schedule_interval=None,
    start_date=datetime(2024, 4, 20),
    catchup=False,
)

start = EmptyOperator(task_id="start", dag=dag)

end = EmptyOperator(task_id="end", dag=dag)

create_table_ = BigQueryExecuteQueryOperator(
    task_id="creating_dim_customer",
    sql = "Sql/create_dim_customer.sql",
    use_legacy_sql=False,
    dag=dag,
)

populate_table = BigQueryExecuteQueryOperator(
    task_id="insert_in_dim_customer",
    sql = "Sql/populate_dim_customer.sql",
    use_legacy_sql=False,
    dag=dag
)

start >> create_table_ >> populate_table >> end