from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
)
import json
import os
from pathlib import Path


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
parent_path = str(Path(__file__).parent)


for table in tables:

    schema_file = os.path.join(parent_path, "schema/" + table + ".json")

    create_table = BigQueryCreateEmptyTableOperator(
        task_id=f"create_{table}",
        dataset_id="data_platform",
        table_id=table,
        table_resource=json.load(open(schema_file)),
        dag=dag,
    )

    run_query = BigQueryExecuteQueryOperator(
        task_id=f"execute_{table}",
        sql="sql/" + table + ".sql",
        write_disposition="WRITE_TRUNCATE",
        dag=dag,
        use_legacy_sql=False,
    )

    start_task >> create_table >> run_query >> end_task
