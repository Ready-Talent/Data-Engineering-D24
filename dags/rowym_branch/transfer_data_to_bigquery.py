import logging
from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator




def move_data():
    load_csv = GCSToBigQueryOperator(
    task_id="gcs_to_bigquery_rowym",
    bucket="chicago-taxi-test-de24",
    source_objects=["chicago-taxi-test-de24/data/*.csv"],
    destination_project_dataset_table=f"ready-data-engineering-p24.Rowym_from_GCS",
    schema_fields = [
    {'name': 'unique_key', 'type': 'STRING'},
    {'name': 'taxi_id', 'type': 'STRING'},
    {'name': 'trip_start_timestamp', 'type': 'TIMESTAMP'},
    {'name': 'trip_end_timestamp', 'type': 'TIMESTAMP'},
    {'name': 'trip_seconds', 'type': 'INTEGER'},
    {'name': 'trip_miles', 'type': 'FLOAT'},
    {'name': 'pickup_census_tract', 'type': 'STRING'},
    {'name': 'dropoff_census_tract', 'type': 'STRING'},
    {'name': 'pickup_community_area', 'type': 'INTEGER'},
    {'name': 'dropoff_community_area', 'type': 'INTEGER'},
    {'name': 'fare', 'type': 'FLOAT'},
    {'name': 'tips', 'type': 'FLOAT'},
    {'name': 'tolls', 'type': 'FLOAT'},
    {'name': 'extras', 'type': 'FLOAT'},
    {'name': 'trip_total', 'type': 'FLOAT'},
    {'name': 'payment_type', 'type': 'STRING'},
    {'name': 'company', 'type': 'STRING'},
    {'name': 'pickup_latitude', 'type': 'FLOAT'},
    {'name': 'pickup_longitude', 'type': 'FLOAT'},
    {'name': 'pickup_location', 'type': 'STRING'},
    {'name': 'dropoff_latitude', 'type': 'FLOAT'},
    {'name': 'dropoff_longitude', 'type': 'FLOAT'},
    {'name': 'dropoff_location', 'type': 'STRING'}
    ],
    write_disposition="WRITE_APPEND",
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