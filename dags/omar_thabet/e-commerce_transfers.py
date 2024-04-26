from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.postgres_to_gcs import (
    PostgresToGCSOperator,
)

dag = DAG(
    dag_id="e-comerce_transfers",
    description="transfer e-commerce tables from postgres to BQ",
    schedule_interval=None,
    start_date=datetime(2024, 4, 20),
    catchup=False,
)

GCS_BUCKET = "postgres-to-gcs"

postgres_to_gcs_customer = PostgresToGCSOperator(
    task_id="customer_postgres_to_gcs",
    postgres_conn_id="postgres_connection",
    bucket=GCS_BUCKET,
    sql="SELECT * FROM src01.customer",
    filename="omar_thabet/customer.csv",
    export_format="csv",
    gzip=False,
    use_server_side_cursor=False,
)


gcs_to_bq_customer = GCSToBigQueryOperator(
    task_id="customer_gcs_to_bq",
    bucket=GCS_BUCKET,
    source_objects=["omar_thabet/*.csv"],
    source_format="CSV",
    destination_project_dataset_table="landing.customer",
    write_disposition="WRITE_TRUNCATE",
    create_disposition="CREATE_IF_NEEDED",
    autodetect=True,
    ignore_unknown_values=True,
    field_delimiter=",",
    dag=dag,
    skip_leading_rows=1,
)


start = EmptyOperator(task_id="start", dag=dag)

end = EmptyOperator(task_id="end", dag=dag)


start >> postgres_to_gcs_customer >> gcs_to_bq_customer >> end
