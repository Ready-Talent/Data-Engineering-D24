import logging
from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator



dag = DAG(
    dag_id="rowym_move_data_postgres_GCS",
    description="Moves data from Postgresto GCS",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

start_task = EmptyOperator(task_id="start_task", dag=dag)

tables = ['customer','product','payment','order','order_detail','channel','payment_type','address']
# postgres_to_gcs = []
# gcs_to_bigquery = []
for table in tables:
    postgres_to_gcs = PostgresToGCSOperator(
        task_id=f"get_data_from_postgres_to_GCS_{table}",
        postgres_conn_id="Postgres_Rowym_conn",
        sql=f"SELECT * FROM SRC_01.{table}",
        bucket="postgres-to-gcs",
        export_format = "CSV",
        filename=f"Rowym/{table}.csv",
        gzip=False,
        dag = dag
    )

    gcs_to_bigquery = GCSToBigQueryOperator(
    task_id=f"from_gcs_to_bigquery_{table}",
    bucket="postgres-to-gcs",
    source_objects=[f"Rowym/{table}.csv"],
    destination_project_dataset_table= f"Rowym_from_GCS.{table}",
    field_delimiter=',',
    max_bad_records = 1000000,
    skip_leading_rows = 1,
    ignore_unknown_values = True,
    source_format = "CSV",
    autodetect = True,
    write_disposition = "WRITE_TRUNCATE",
    dag = dag
    ) 

end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> postgres_to_gcs >> gcs_to_bigquery >> end_task
