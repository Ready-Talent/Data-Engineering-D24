from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow import DAG
from datetime import datetime

dag = DAG(
    dag_id="postgres_to_gcs_Nadine",
    description="Transfer data from postgres to gcs",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

get_data = PostgresToGCSOperator(
        task_id="get_data",
        postgres_conn_id='Nadine_07_postgres_connection',    # connection in airflow
        sql='select * from src01."order" o',
        bucket='postgres-to-gcs',
        filename='Nadine/orders.csv',      # in gcs
        export_format = 'CSV',
        gzip=False,
        dag = dag
    )

to_bigquery = GCSToBigQueryOperator(
    task_id="postgres_to_gcs_to_bigquery_example",
    bucket="postgres-to-gcs",
    source_objects=["Nadine/orders.csv"],
    field_delimiter = ';',
    skip_leading_rows = 1,
    source_format = 'CSV',
    max_bad_records = 10000000,
    ignore_unknown_values = True,
    destination_project_dataset_table="Nadine_Airflow.orders",
    autodetect=True,
    write_disposition="WRITE_TRUNCATE",
    dag=dag
)

start_task = EmptyOperator(task_id="start_task", dag=dag)

end_task = EmptyOperator(task_id="end_task", dag=dag)


start_task >> get_data >> to_bigquery >> end_task