
import logging
from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def load_data():
    load_to_bigquery = GoogleCloudStorageToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket='chicago-taxi-test-de24',
        source_objects=['chicago-taxi-test-de24/data*.csv'],
        destination_project_dataset_table='ready-data-engineering-p24.sedawy_airflow',
        source_format='CSV',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='google_cloud_default',
        dag=dag,
    )

dag = DAG(
    dag_id="load_dataaa",
    description="Simple tutorial DAG",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)


start_task = EmptyOperator(task_id="start_task", dag=dag)

hello_task = PythonOperator(
    task_id="hello_task",
    python_callable=load_data,
    dag=dag,
)

end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> hello_task >> end_task