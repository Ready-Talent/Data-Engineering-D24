from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
)
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

create_dim_customer_table = BigQueryCreateEmptyTableOperator(
    task_id="create_table",
    dataset_id="data_platform",
    table_id="dim_customer",
    schema_fields=[
        {"name": "customer_key", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "customer_id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "customer_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "email", "type": "STRING", "mode": "NULLABLE"},
        {"name": "phone", "type": "STRING", "mode": "NULLABLE"},
        {"name": "address_id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "address_street", "type": "STRING", "mode": "NULLABLE"},
        {"name": "address_zipcode", "type": "STRING", "mode": "NULLABLE"},
        {"name": "city_id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "city_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "state_id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "state_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "country_id", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "country_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "created_by", "type": "STRING", "mode": "NULLABLE"},
        {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "modified_by", "type": "STRING", "mode": "NULLABLE"},
        {"name": "modified_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    ],
    exists_ok=True,
)

insert_dim_customer = BigQueryExecuteQueryOperator(
    task_id="BigQuery_Execute_Query",
    sql="sql/dim_customer.sql",
    dag=dag,
    use_legacy_sql=False,
)

end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> create_dim_customer_table >> insert_dim_customer >> end_task
