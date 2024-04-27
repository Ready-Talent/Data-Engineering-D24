from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)

dag = DAG(
    dag_id="lojain_create_dim_tables",
    description="create dim",
    schedule_interval=None,
    start_date=datetime(2024, 4, 20),
    catchup=False,
)

start = EmptyOperator(task_id="start", dag=dag)

end = EmptyOperator(task_id="end", dag=dag)

create_tables = BigQueryExecuteQueryOperator(
    task_id="create_table",
    sql="create_tables.sql",
    use_legacy_sql=False,
    dag=dag,
)


start >> create_tables>> end