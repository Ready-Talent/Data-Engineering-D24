import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator

postgres_to_gcs_Dag = DAG(
    dag_id="PG_to_GCS_Maged",
    description="second trial",
    schedule_interval=None,
    start_date=datetime(2024, 4, 20),
    catchup=False,
) 

GCS_BUCKET = 'postgres-to-gcs'
GCS_OBJECT_PATH = 'Maged'
SOURCE_TABLE_NAME = 'src01.customer'
FILE_FORMAT = 'csv'
#POSTGRESS_CONNECTION_ID = 'postgres'

postgres_to_gcs_task = PostgresToGCSOperator(
    task_id ='postgres_to_gcs',
    postgres_conn_id="postgresConnection_Maged",
    bucket=GCS_BUCKET,
    sql = 'SELECT * FROM src01.customer',
    filename='Maged/customer.csv',
    export_format='csv',
    gzip=False,
    use_server_side_cursor=False,
)


load_csv = GCSToBigQueryOperator(
    task_id="gcs_to_bigquery_Maged2",
    bucket="postgres-to-gcs",
    source_objects=[
        "Maged/*.csv"
    ],
    source_format="CSV",
    destination_project_dataset_table="SRC_08.customer2",
    write_disposition="WRITE_TRUNCATE",
    create_disposition="CREATE_IF_NEEDED",
    autodetect=True,
    ignore_unknown_values=True,
    field_delimiter=",",
    dag = postgres_to_gcs_Dag,
    skip_leading_rows=1,
)


first_task = EmptyOperator(task_id="first_task", dag=postgres_to_gcs_Dag)

last_task = EmptyOperator(task_id="last_task", dag=postgres_to_gcs_Dag)


first_task >> postgres_to_gcs_task >> load_csv >> last_task