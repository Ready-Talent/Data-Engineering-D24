import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.google import GCSToBigQueryOperator



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
        "chicago-taxi-test-de24/data/*.csv",
    ],
    source_format="CSV",
    destination_project_dataset_table=f"chicagoTaxi.trips",
    write_disposition="WRITE_TRUNCATE",
    create_disposition="CREATE_IF_NEEDED",
    autodetect=True,
    field_delimiter=";",
    dag = move_data_dag,
)

first_task = EmptyOperator(task_id="first_task", dag=dag)

last_task = EmptyOperator(task_id="last_task", dag=dag)


first_task >> load_csv >> last_task
