import logging
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


# Function that transfers data
def move_data():
    load_csv = GCSToBigQueryOperator(
    task_id="gcs_to_bigquery_rowym",
    bucket="chicago-taxi-test-de24",
    source_objects=["data/*.csv"],
    destination_project_dataset_table=f"ready-data-engineering-p24.Rowym_from_GCS.chicago_taxi_05",
    field_delimiter=';',
    max_bad_records = 10000,
    skip_leading_rows = 1,
    ignore_unknown_values = True,
    source_format = "CSV",
    write_disposition = "WRITE_TRUNCATE",
    )
    

dag = DAG(
    dag_id="rowym_move_data_to_bigquery",
    description="Simple tutorial DAG",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

start_task = EmptyOperator(task_id="start_task", dag=dag)

GCS_move_to_BigQuery = PythonOperator(
    task_id="GCS_to_BigQuery",
    python_callable=move_data,
    dag=dag,
)

end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> GCS_move_to_BigQuery >> end_task