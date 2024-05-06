from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryCreateEmptyTableOperator
import json

DATA_PLATFORM = "data_platform_abdelrahman_tamer"

dag  = DAG(
        dag_id="abdelrahman_06_transfer_src_to_data_platform", 
        schedule_interval=None, 
        start_date=datetime(2021, 1, 1), 
        catchup=False
    )
       

dim_tables = ["dim_customer", "dim_product", "dim_junk"]
    

start_task = EmptyOperator(task_id="start_task", dag=dag)
end_task = EmptyOperator(task_id="end_task", dag=dag)


for table in dim_tables:
    
    schema_file_path = f"/home/airflow/gcs/dags/abdelrahman-tamer/schemas/{table}.json"
    with open(schema_file_path, 'r') as f:
        schema_data = json.load(f)
        
    create_table = BigQueryCreateEmptyTableOperator(
        task_id=f"create_table_{table}",
        dataset_id=DATA_PLATFORM,
        table_id=table,
        exists_ok=True,
        schema_fields=schema_data["fields"],
        dag=dag
    )

    populate_table = BigQueryExecuteQueryOperator(
        task_id=f"populate_table_{table}",
        sql=f"sql/populate_{table}.sql",
        use_legacy_sql=False,
        dag=dag
    )
    
    start_task >> create_table >> populate_table >> end_task
    

