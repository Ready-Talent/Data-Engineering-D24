import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator




move_data_dag = DAG(
    dag_id="Maged_second_Dag",
    description="second trial",
    schedule_interval=None,
    start_date=datetime(2024, 4, 20),
    catchup=False,
) 


load_csv = GCSToBigQueryOperator(
    task_id="gcs_to_bigquery_Maged",
    bucket="chicago-taxi-test-de24",
    source_objects=[
        "data/*.csv"
    ],
    source_format="CSV",
    destination_project_dataset_table="SRC_08.trips",
    write_disposition="WRITE_TRUNCATE",
    max_bad_records = 10000,
    create_disposition="CREATE_IF_NEEDED",
    autodetect=True,
    ignore_unknown_values=True,
    field_delimiter=";",
    dag = move_data_dag,
    skip_leading_rows=1,
)

first_task = EmptyOperator(task_id="first_task", dag=move_data_dag)

last_task = EmptyOperator(task_id="last_task", dag=move_data_dag)


first_task >> load_csv >> last_task
