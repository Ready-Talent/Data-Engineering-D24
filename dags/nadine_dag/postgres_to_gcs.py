from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow import DAG
from datetime import datetime

dag = DAG(
    dag_id="gcs_to_bigquery_Nadine",
    description="Transfer data from GCS to bigquery",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

get_data = PostgresToGCSOperator(
        task_id="get_data",
        postgres_conn_id='Nadine_07_postgres_connection',
        sql='select * from src01."order" o',
        bucket='postgres-to-gcs',
        filename='Nadine/',
        gzip=False,
    )

start_task = EmptyOperator(task_id="start_task", dag=dag)


end_task = EmptyOperator(task_id="end_task", dag=dag)


start_task >> get_data >> end_task