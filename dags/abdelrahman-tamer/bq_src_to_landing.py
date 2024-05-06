from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator
import json
import os

DATA_PLATFORM = "data_platform_abdelrahman_tamer"

dag  = DAG(
        dag_id="abdelrahman_06_transfer_src_to_data_platform", 
        schedule_interval=None, 
        start_date=datetime(2021, 1, 1), 
        catchup=False
    )


# customer_schema = {
#     "table_name": "dim_customer",
#     "fields": [
#         {"name": "customer_id", "type": "INT64", "mode": "NULLABLE"},
#         {"name": "customer_name", "type": "STRING", "mode": "NULLABLE"},
#         {"name": "address_id", "type": "INT64", "mode": "NULLABLE"},
#         {"name": "address_street", "type": "STRING", "mode": "NULLABLE"},
#         {"name": "zipcode", "type": "STRING", "mode": "NULLABLE"},
#         {"name": "created_by", "type": "STRING", "mode": "NULLABLE"},
#         {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
#         {"name": "modified_by", "type": "STRING", "mode": "NULLABLE"},
#         {"name": "modified_at", "type": "TIMESTAMP", "mode": "NULLABLE"}
#     ]
# }

# product_schema = {
    
#     "table_name": "dim_product",
#     "fields": [
#         {"name": "product_id", "type": "INT64", "mode": "NULLABLE"},
#         {"name": "brand_id", "type": "INT64", "mode": "NULLABLE"},
#         {"name": "category_id", "type": "INT64", "mode": "NULLABLE"},
#         {"name": "product_name", "type": "STRING", "mode": "NULLABLE"},
#         {"name": "price", "type": "FLOAT64", "mode": "NULLABLE"},
#         {"name": "description", "type": "STRING", "mode": "NULLABLE"},
#         {"name": "brand_name", "type": "STRING", "mode": "NULLABLE"},
#         {"name": "category_name", "type": "STRING", "mode": "NULLABLE"},
#         {"name": "created_by", "type": "STRING", "mode": "NULLABLE"},
#         {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
#         {"name": "modified_by", "type": "STRING", "mode": "NULLABLE"},
#         {"name": "modified_at", "type": "TIMESTAMP", "mode": "NULLABLE"}
#     ]
# }

# junk_dim_schema = {
    
#     "table_name": "dim_junk",    
#     "fields": [
#         {"name": "junk_id", "type": "INT64", "mode": "NULLABLE"},
#         {"name": "payment_type_code", "type": "INT64", "mode": "NULLABLE"},
#         {"name": "payment_type_name", "type": "STRING", "mode": "NULLABLE"},
#         {"name": "channel_code", "type": "INT64", "mode": "NULLABLE"},
#         {"name": "channel_name", "type": "STRING", "mode": "NULLABLE"},
#         {"name": "created_by", "type": "STRING", "mode": "NULLABLE"},
#         {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
#         {"name": "modified_by", "type": "STRING", "mode": "NULLABLE"},
#         {"name": "modified_at", "type": "TIMESTAMP", "mode": "NULLABLE"}
#     ]
# }
       

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
    



# start_task >> create_table >> populate_table >> end_task
