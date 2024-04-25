import sys
import os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
from ETL_airflow import GCSEtL

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator


PG_SCHEMA = "src01"
BQ_BUCKET = "postgres-to-gcs"
FILENAME = "Ashraf/"
PG_CONN_ID = "postgres_01"
destination ='SRC_01'

dag = DAG(
    dag_id="transfer_src_from_PG_to_GCS",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

gcs_etl = GCSEtL()
start_task = EmptyOperator(task_id="start_task", dag=dag)

## extract from Postgres and load into GCS in CSV format
load_payment_table = gcs_etl.load_from_postgres(PG_SCHEMA, 'payment', BQ_BUCKET, FILENAME + 'payment', PG_CONN_ID, dag)
load_payment_type_table = gcs_etl.load_from_postgres(PG_SCHEMA, 'payment_type', BQ_BUCKET, FILENAME + 'payment_type', PG_CONN_ID, dag)
load_order_detail_table = gcs_etl.load_from_postgres(PG_SCHEMA, 'order_detail', BQ_BUCKET, FILENAME + 'order_detail', PG_CONN_ID, dag)
load_order_table = gcs_etl.load_from_postgres(PG_SCHEMA, 'order', BQ_BUCKET, FILENAME + 'order', PG_CONN_ID, dag)
load_product_table = gcs_etl.load_from_postgres(PG_SCHEMA, 'product', BQ_BUCKET, FILENAME + 'product', PG_CONN_ID, dag)
load_channel_table = gcs_etl.load_from_postgres(PG_SCHEMA, 'channel', BQ_BUCKET, FILENAME + 'channel', PG_CONN_ID, dag)
load_address_table = gcs_etl.load_from_postgres(PG_SCHEMA, 'address', BQ_BUCKET, FILENAME + 'address', PG_CONN_ID, dag)
load_customer_table = gcs_etl.load_from_postgres(PG_SCHEMA, 'customer', BQ_BUCKET, FILENAME + 'customer', PG_CONN_ID, dag)

temp_task = EmptyOperator(task_id="temp_task", dag=dag)

## extract from GCS and load into Bigquery in CSV format
extract_payment_table = gcs_etl.extract_to_bigquery(BQ_BUCKET, FILENAME + 'payment', destination, 'payment', dag)
extract_payment_type_table = gcs_etl.extract_to_bigquery(BQ_BUCKET, FILENAME + 'payment_type', destination, 'payment_type', dag)
extract_order_detail_table = gcs_etl.extract_to_bigquery(BQ_BUCKET, FILENAME + 'order_detail', destination, 'order_detail', dag)
extract_order_table = gcs_etl.extract_to_bigquery(BQ_BUCKET, FILENAME + 'order', destination, 'order', dag)
extract_product_table = gcs_etl.extract_to_bigquery(BQ_BUCKET, FILENAME + 'product', destination, 'product', dag)
extract_channel_table = gcs_etl.extract_to_bigquery(BQ_BUCKET, FILENAME + 'channel', destination, 'channel', dag)
extract_address_table = gcs_etl.extract_to_bigquery(BQ_BUCKET, FILENAME + 'address', destination, 'address', dag)
extract_customer_table = gcs_etl.extract_to_bigquery(BQ_BUCKET, FILENAME + 'customer', destination, 'customer', dag)

end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> [load_payment_table, load_payment_type_table, load_order_detail_table, load_order_table, load_product_table, load_channel_table, load_address_table, load_customer_table] >> temp_task
[extract_payment_table, extract_payment_type_table, extract_order_detail_table, extract_order_table, extract_product_table, extract_channel_table, extract_address_table, extract_customer_table] >>\ 
end_task






