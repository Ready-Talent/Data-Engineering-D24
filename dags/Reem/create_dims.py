from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryExecuteQueryOperator,
)

dag = DAG(
    dag_id="transfer_all_dims_Reemaa",
    description="create dims",
    schedule_interval=None,
    start_date=datetime(2024, 4, 20),
    catchup=False,
)

start = EmptyOperator(task_id="start", dag=dag)

end = EmptyOperator(task_id="end", dag=dag)

transfer_tables = BigQueryExecuteQueryOperator(
    task_id="transfer_all_tables",
    sql = "/Sql/create_tables.sql",
    use_legacy_sql=False,
    dag=dag,
)


start >> transfer_tables >> end