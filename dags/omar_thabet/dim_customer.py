from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)


dag = DAG(
    dag_id="create_dim_customer",
    description="Simple tutorial DAG",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

start_task = EmptyOperator(task_id="start_task", dag=dag)

create_dim_customer_table = BigQueryExecuteQueryOperator(
    task_id="create_table",
    sql="sql/create_dim_customer.sql",
    dag=dag,
    use_legacy_sql=False,
)

insert_dim_customer = BigQueryExecuteQueryOperator(
    task_id="BigQuery_Execute_Query",
    sql="sql/dim_customer.sql",
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
    use_legacy_sql=False,
)

end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> create_dim_customer_table >> insert_dim_customer >> end_task
