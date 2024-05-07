import logging
from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator



dag = DAG(
    dag_id="move_from_postgres_to_gcs_reemaa",
    description="postgres_to gcs DAG",
    schedule_interval=None,
    start_date=datetime(2024, 4, 24),
    catchup=False,
)

start_task = EmptyOperator(task_id="start_task", dag=dag)
end_task = EmptyOperator(task_id="end_task", dag=dag)


tables = ['product','order','order_detail','channel','payment','payment_type','address','customer',]
for table in tables:
    postgres_to_gcs = PostgresToGCSOperator(
    task_id=f"postgres_to_gcs{table}",
    postgres_conn_id="postgres_conn_reema",
    sql=f"SELECT * FROM src01.{table}",
    bucket="postgres-to-gcs",
    export_format = "CSV",
    filename=f"Reemaa/{table}.csv",
    gzip=False,
    dag = dag
    )
    
    
    gcs_to_bigquery = GCSToBigQueryOperator(
    task_id=f"gcs_to_bigquery{table}",
    bucket="postgres-to-gcs",
    source_objects=[f"Reemaa/{table}.csv"],
    destination_project_dataset_table= f"Reema_AirFlow.{table}",
    field_delimiter=',',
    max_bad_records = 1000000,
    skip_leading_rows = 1,
    ignore_unknown_values = True,
    source_format = "CSV",
    autodetect = True,
    write_disposition = "WRITE_TRUNCATE",
    dag = dag
    ) 
    start_task >> postgres_to_gcs >> gcs_to_bigquery >> end_task