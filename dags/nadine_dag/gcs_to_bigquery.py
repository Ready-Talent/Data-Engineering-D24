from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.operators.gcs_list_operator import GoogleCloudStorageListOperator
from airflow import DAG
from datetime import datetime

dag = DAG(
    dag_id="gcs_to_bigquery_Nadine",
    description="Transfer data from GCS to bigquery",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

start_task = EmptyOperator(task_id="start_task", dag=dag)


list_csv_files = GoogleCloudStorageListOperator(
    task_id="list_csv_files",
    bucket="chicago-taxi-test-de24",
    prefix="bigquery/chicago-taxi-test-de24/data/",
    delimiter=".csv",
    dag=dag
)

load_csv = GCSToBigQueryOperator(
    task_id="gcs_to_bigquery_example",
    bucket="chicago-taxi-test-de24",
    source_objects="{{ task_instance.xcom_pull(task_ids='list_csv_files') }}",
    destination_project_dataset_table="ready-data-engineering-p24.Nadine_Airflow.your_table",
    autodetect=True,
    write_disposition="WRITE_APPEND",
    dag=dag
)

end_task = EmptyOperator(task_id="end_task", dag=dag)


start_task >> list_csv_files >> load_csv >> end_task