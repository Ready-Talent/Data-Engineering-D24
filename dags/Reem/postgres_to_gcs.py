import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator



dag = DAG(
    dag_id="move_from_postgres_to_gcs_reemaa",
    description="postgres_to gcs DAG",
    schedule_interval=None,
    start_date=datetime(2024, 4, 24),
    catchup=False,
)

start_task = EmptyOperator(task_id="start_task", dag=dag)
end_task = EmptyOperator(task_id="end_task", dag=dag)

GCS_BUCKET = "postgres-to-gcs"

tables = ["product", "customer", "address", "order_detail", "payment", "payment_type", "channel"]


for table in tables:

    get_data = PostgresToGCSOperator(
        task_id=f"postgres_to_gcs{table}",
        postgres_conn_id="postgres_conn_reema",
        sql=f"SELECT * from src01.{table}",
        bucket=GCS_BUCKET,
        export_format="CSV",
        filename="Reemaa/order.csv",
        gzip=False,
        dag=dag
    )


    load_csv = GCSToBigQueryOperator(
        task_id=f"gcs_to_bigquery{table}",
        bucket=GCS_BUCKET,
        source_objects=[f"Reemaa/{table}.csv"],
        destination_project_dataset_table=f"Reema_AirFlow.{table}",
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        field_delimiter=',',
        skip_leading_rows=1,
        ignore_unknown_values=True,
        max_bad_records= 1000000,
        dag=dag
    )

    start_task >> get_data >> load_csv >>end_task
