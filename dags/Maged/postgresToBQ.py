import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator

dag = DAG(
    dag_id="PG_to_GCS_Maged",
    description="second trial",
    schedule_interval=None,
    start_date=datetime(2024, 4, 20),
    catchup=False,
) 
first_task = EmptyOperator(task_id="first_task", dag=dag)

last_task = EmptyOperator(task_id="last_task", dag=dag)


tables = ["customer", "date", "time","product"]

for table in tables:
    GCS_BUCKET = 'postgres-to-gcs'
    GCS_OBJECT_PATH = 'Maged'
    SOURCE_TABLE_NAME = f'src01.{table}'
    FILE_FORMAT = 'csv'

    postgres_to_gcs_task = PostgresToGCSOperator(
        task_id =f'postgres_to_gcs_{table}',
        postgres_conn_id="postgresConnection_Maged",
        bucket=GCS_BUCKET,
        sql = f'SELECT * FROM src01.{table}',
        filename=f'Maged/{table}.csv',
        export_format='csv',
        gzip=False,
        use_server_side_cursor=False,
        dag = dag,
    )   


    load_csv = GCSToBigQueryOperator(
        task_id=f"gcs_to_bigquery_Maged2_{table}",
        bucket="postgres-to-gcs",
        source_objects=[
            f"Maged/{table}.csv"
        ],
        source_format="CSV",
        destination_project_dataset_table=f"landing_Maged.{table}",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        autodetect=True,
        ignore_unknown_values=True,
        field_delimiter=",",
        dag = dag,
        skip_leading_rows=1,
    )

    first_task >> postgres_to_gcs_task >> load_csv >> last_task



#first_task >> postgres_to_gcs_task >> load_csv >> last_task