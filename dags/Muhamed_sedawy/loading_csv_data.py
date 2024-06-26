
import logging
from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator




dag = DAG(
    dag_id="Sedawy_Dag",
    description="Simple_tutorial_DAG",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)


start_task = EmptyOperator(task_id="start_task", dag=dag)
load_task = GoogleCloudStorageToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket='chicago-taxi-test-de24',
        source_objects=['data*.csv'],
        destination_project_dataset_table='sedawy_airflow.chicago-taxi-test-de24',
        source_format='CSV',
        autodetect=True,
        field_delimiter=',',
        write_disposition='Write_append',
        create_disposition='CREATE_IF_NEEDED',
        dag=dag,
    )

end_task = EmptyOperator(task_id="end_task", dag=dag )

start_task >> load_task >> end_task