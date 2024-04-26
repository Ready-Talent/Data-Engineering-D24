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
    dag_id="create_dim_tables",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

start_task = EmptyOperator(task_id="start_task", dag=dag)
end_task = EmptyOperator(task_id="end_task", dag=dag)

tables = ["dim_customer", "dim_date", "dim_product"]


for table in tables:

    create_table = BigQueryCreateEmptyTableOperator(
        task_id=f"create_{table}",
        dataset_id="data_platform",
        table_id=table,
        schema_fields=f"schema/" + table + ".json",
    )

    run_query = BigQueryExecuteQueryOperator(
        task_id=f"execute_{table}",
        sql="sql/" + table + ".sql",
        dag=dag,
        use_legacy_sql=False,
    )

    start_task >> create_table >> run_query >> end_task
