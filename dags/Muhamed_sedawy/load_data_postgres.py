from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta

bucket = 'postgres-to-gcs'

dag = DAG(
    dag_id="postgres_to_bigquery_sedawy",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

table_list = ['address', 'channel', 'customer', 'order', 'order_detail', 'payment', 'payment_type', 'product']

with dag:
    extract_tasks = []
    load_tasks = []

    # Define extraction tasks for each table
    for table in table_list:
        extract_task = PostgresOperator(
            task_id=f"extract_data_{table}",
            sql=f"select * from src01.{table}",
            postgres_conn_id='sedawy_connections',
            bucket=bucket,
            filename=f'Sedawy/{table}.csv',
            export_format='CSV',
        )
        extract_tasks.append(extract_task)

    # Define loading tasks for each table
    for table in table_list:
        load_task = GCSToBigQueryOperator(
            task_id=f"load_data_{table}",
            bucket=bucket,
            source_objects=[f"Sedawy/{table}.csv"],
            destination_project_dataset_table=f'Data_Platform_Sedawy.{table}',
            create_disposition='CREATE_IF_NEEDED',
            skip_leading_rows=1,
            source_format='CSV',
        )
        load_tasks.append(load_task)

    # Set up dependencies to run extraction and loading tasks in parallel
    for extract_task, load_task in zip(extract_tasks, load_tasks):
        extract_task >> load_task
