import logging
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator



dag = DAG(
    dag_id="rowym_move_data_to_bigquery",
    description="Simple tutorial DAG",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

start_task = EmptyOperator(task_id="start_task", dag=dag)

# Function that transfers data
load_csv = GCSToBigQueryOperator(
task_id="gcs_to_bigquery_rowym",
bucket="chicago-taxi-test-de24",
source_objects=["data/*csv"],
destination_project_dataset_table= "Rowym_from_GCS.chicago_taxi_05",
field_delimiter=';',
max_bad_records = 1000000,
skip_leading_rows = 1,
ignore_unknown_values = True,
source_format = "CSV",
autodetect = True,
write_disposition = "WRITE_TRUNCATE",
dag = dag
)
    

end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> load_csv >> end_task