from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

gcs_bucket = "postgres-to-gcs"

address_schema = [
    {'name': 'customer_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'street', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'city', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'zipcode', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'created_by', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
    {'name': 'modified_by', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'modified_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
]

dag  = DAG(
        dag_id="abdelrahman_06_postgres_bq_dag", 
        schedule_interval=None, 
        start_date=datetime(2021, 1, 1), 
        catchup=False,
    )


start_task = EmptyOperator(task_id="start_task", dag=dag)

pg_to_gcs = PostgresToGCSOperator(
        task_id="pg_to_gcs",
        postgres_conn_id="abdelrahman_06_postgres_connection",
        bucket=gcs_bucket,
        filename="abdelrahman_06/address.csv",
        sql="SELECT * FROM src01.address;",
        export_format="csv",
        dag=dag
    )

move_data_to_bigquery_table = GCSToBigQueryOperator(
        task_id = "move_data_to_bigquery_table",
        bucket=gcs_bucket,
        source_objects="abdelrahman_06/address.csv",
        source_format="CSV",
        destination_project_dataset_table="SRC_06.address",
        schema_fields=address_schema,
        field_delimiter=',',
        max_bad_records=1000000,
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        encoding='UTF-8',
        dag=dag
    )

end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> pg_to_gcs >>move_data_to_bigquery_table >> end_task