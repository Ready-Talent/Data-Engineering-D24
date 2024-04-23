from airflow import DAG
from airflow.operators.postgres_operator import PostgresToGCSOperator
from airflow.operators.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta


dag = DAG(
    dag_id="postgres_to_bigquery_sedawy",
    description='A DAG to transfer data from PostgreSQL to Google Cloud Storage to BigQuery',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

with dag:
    # Task to execute SQL command in PostgreSQL to extract data and save to GCS
    extract_to_gcs_task = PostgresToGCSOperator(
        task_id="get_data",
        postgres_conn_id= 'sedawy_connections',
        sql='select *  from src01.order',
        bucket='postgres-to-gcs',
        filename='Sedawy/order.csv',
    )

    # Task to load data from GCS to BigQuery
    load_to_bigquery_task = GCSToBigQueryOperator(
        task_id='load_to_bigquery02',
        bucket='postgres-to-gcs',
        source_objects='Sedawy/order.csv',  # GCS source path
        destination_project_dataset_table='SRC_02.order',  # BigQuery table to load data into
        skip_leading_rows=1,  # If your CSV has a header row
        source_format='CSV',  # Source data format
    )

    # Set task dependencies
    extract_to_gcs_task >> load_to_bigquery_task
