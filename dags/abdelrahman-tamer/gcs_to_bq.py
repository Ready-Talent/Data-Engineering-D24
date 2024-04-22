from airflow import DAG
from datetime import datetime
import logging
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

logging.basicConfig(level=logging.INFO)

def log_error(context):
    logging.error(f"Error Occurred: {context['exception']}")


dag = DAG (
        dag_id="abdelrahman_06_gcs_to_bq", 
        schedule_interval=None, 
        start_date=datetime(2021, 1, 1), 
        catchup=False,
        on_failure_callback=log_error
    )


start = EmptyOperator(task_id="start_task", dag=dag)



load_data = GCSToBigQueryOperator(
    task_id = "chicago_taxi_gcs_to_bq",
    bucket="chicago-taxi-test-de24",
    source_objects="data/*.csv",
    source_format="CSV",
    destination_project_dataset_table="SRC_06.chicago_taxi",
    autodetect=True,
    field_delimiter=';',
    skip_leading_rows=1,
    write_disposition="WRITE_TRUNCATE",
    create_disposition="CREATE_IF_NEEDED",
    encoding='UTF-8',
    dag=dag
    )

end = EmptyOperator(task_id="end_task", dag=dag)

start >> load_data >> end
