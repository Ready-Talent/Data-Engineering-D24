from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta
bucket = 'postgres-to-gcs'

def create_postgres_to_bigquery_dag(task_id,dag_id, sql, gcs_filename, bq_table):
    dag = DAG(
        dag_id=dag_id,
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
    )

    with dag:
        # Task to execute SQL command in PostgreSQL to extract data and save to GCS
        extract_to_gcs_task = PostgresToGCSOperator(
            task_id=f"extract_data_from_{dag_id}",
            postgres_conn_id='sedawy_connections',
            sql=sql,
            bucket=bucket,
            filename=gcs_filename,
            export_format='CSV',
        )

        # Task to load data from GCS to BigQuery
        load_to_bigquery_task = GCSToBigQueryOperator(
            task_id=f"{task_id}",
            bucket=bucket,
            source_objects=[gcs_filename],  # GCS source path
            destination_project_dataset_table=bq_table,  # BigQuery table to load data into
            create_disposition='CREATE_IF_NEEDED',
            skip_leading_rows=1,  # If your CSV has a header row
            source_format='CSV',  # Source data format
        )

        # Set task dependencies
        extract_to_gcs_task >> load_to_bigquery_task

    return dag
dag1 = create_postgres_to_bigquery_dag(
    task_id="task_1",
    dag_id="postgres_to_bigquery_sedawy_1",
    sql='select * from src01.address',
    gcs_filename='Sedawy/address.csv',
    bq_table='Data_Platform_Sedawy.address',
)

dag2 = create_postgres_to_bigquery_dag(
    task_id="task_2",
    dag_id="postgres_to_bigquery_sedawy_2",
    sql='select * from src01.channel',
    gcs_filename='Sedawy/channel.csv',
    bq_table='Data_Platform_Sedawy.channel',
)

dag3 = create_postgres_to_bigquery_dag(
    task_id="task_3",
    dag_id="postgres_to_bigquery_sedawy_3",
    sql='select * from src01.customer',
    gcs_filename='Sedawy/customer.csv',
    bq_table='Data_Platform_Sedawy.customer',
)


dag4 = create_postgres_to_bigquery_dag(
    task_id="task_4",
    dag_id="postgres_to_bigquery_sedawy_4",
    sql='select * from src01.order',
    gcs_filename='Sedawy/order.csv',
    bq_table='Data_Platform_Sedawy.order',
)

dag5 = create_postgres_to_bigquery_dag(
    task_id="task_5",
    dag_id="postgres_to_bigquery_sedawy_5",
    sql='select * from src01.order_detail',
    gcs_filename='Sedawy/order_detail.csv',
    bq_table='Data_Platform_Sedawy.order_detail',
)
dag6 = create_postgres_to_bigquery_dag(
    task_id="task_6",
    dag_id="postgres_to_bigquery_sedawy_6",
    sql='select * from src01.payment',
    gcs_filename='Sedawy/payment.csv',
    bq_table='Data_Platform_Sedawy.payment',
)
dag7 = create_postgres_to_bigquery_dag(
    task_id="task_7",
    dag_id="postgres_to_bigquery_sedawy_7",
    sql='select * from src01.payment_type',
    gcs_filename='Sedawy/payment_type.csv',
    bq_table='Data_Platform_Sedawy.payment_type',
)
dag8 = create_postgres_to_bigquery_dag(
    task_id="task_8",   
    dag_id="postgres_to_bigquery_sedawy_8",
    sql='select * from src01.product',
    gcs_filename='Sedawy/product.csv',
    bq_table='Data_Platform_Sedawy.product',
)
