import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteOperator

dag = DAG(
    dag_id="create_and_populate_dim_tables",
    description="second trial",
    schedule_interval=None,
    start_date=datetime(2024, 4, 20),
    catchup=False,
) 
first_task = EmptyOperator(task_id="first_task", dag=dag)

last_task = EmptyOperator(task_id="last_task", dag=dag)


tables = ["customer","product","date"]


for table in tables:

    create_dim_table = BigQueryExecuteOperator(
        task_id=f"create_table_{table}",
        sql=f"sql/create_dim_{table}.sql",
        use_legacy_sql=False,
        dag=dag,
    )

    first_task >> create_dim_table >> last_task



