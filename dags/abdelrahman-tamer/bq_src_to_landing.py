from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator
from helpers.tables import data_platform_tables
from schemas.product import product_schema

DATA_PLATFORM = "data_platform_abdelrahman_tamer"

dag  = DAG(
        dag_id="abdelrahman_06_transfer_src_to_data_platform", 
        schedule_interval=None, 
        start_date=datetime(2021, 1, 1), 
        catchup=False
    )


start_task = EmptyOperator(task_id="start_task", dag=dag)


create_table = BigQueryCreateEmptyTableOperator(
    task_id=f"create_table",
    dataset_id=DATA_PLATFORM,
    table_id="product",
    exists_ok=True,
    schema_fields=product_schema,
    dag=dag
)

populate_table = BigQueryExecuteQueryOperator(
    task_id="populate_table",
    sql="sql/populate_dim_product.sql",
    use_legacy_sql=False,
    dag=dag
)


end_task = EmptyOperator(task_id="end_task", dag=dag)


start_task >> create_table >> populate_table >> end_task
