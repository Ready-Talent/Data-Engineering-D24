import logging
from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator



dag = DAG(
    dag_id="rowym_move_data_postgres_GCS",
    description="Moves data from Postgresto GCS",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

start_task = EmptyOperator(task_id="start_task", dag=dag)

get_data = PostgresToGCSOperator(
    task_id="get_data_from_postgres_to_GCS",
    postgres_conn_id="Postgres_Rowym_conn",
    sql='sqlPostgrestoGCS.sql',
    bucket="postgres-to-gcs",
    export_format = "CSV",
    filename="Rowym/order.csv",
    gzip=False,
    dag = dag
)

load_data_to_bigquery = GCSToBigQueryOperator(
task_id="from_gcs_to_bigquery",
bucket="postgres-to-gcs",
source_objects=["Rowym/order.csv"],
destination_project_dataset_table= "Rowym_from_GCS.orders",
field_delimiter=',',
max_bad_records = 1000000,
skip_leading_rows = 1,
ignore_unknown_values = True,
source_format = "CSV",
autodetect = True,
write_disposition = "WRITE_TRUNCATE",
dag = dag
) 

end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> get_data >> load_data_to_bigquery >> end_task
