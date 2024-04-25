from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta
bucket = 'postgres-to-gcs'

dag1 = DAG(
    dag_id="postgres_to_bigquery_sedawy",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)
dag2 = DAG(
    dag_id="postgres_to_bigquery_sedawy",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)
dag3 = DAG(
    dag_id="postgres_to_bigquery_sedawy",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)
dag4 = DAG(
    dag_id="postgres_to_bigquery_sedawy",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)
dag5 = DAG(
    dag_id="postgres_to_bigquery_sedawy",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)
dag6 = DAG(
    dag_id="postgres_to_bigquery_sedawy",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)
dag7 = DAG(
    dag_id="postgres_to_bigquery_sedawy",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)
dag8 = DAG(
    dag_id="postgres_to_bigquery_sedawy",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)




    # Task to execute SQL command in PostgreSQL to extract data and save to GCS
extract_to_gcs_task1 = PostgresToGCSOperator(
    task_id="get_data",
    postgres_conn_id= 'sedawy_connections',
    sql='select *  from src01.address',
    bucket='postgres-to-gcs',
    filename='Sedawy/address.csv',
    export_format = 'CSV',
    dag=dag1
    )



    # Task to execute SQL command in PostgreSQL to extract data and save to GCS
extract_to_gcs_task2 = PostgresToGCSOperator(
    task_id="get_data",
    postgres_conn_id= 'sedawy_connections',
    sql='select *  from src01.channel',
    bucket='postgres-to-gcs',
    filename='Sedawy/channel.csv',
    export_format = 'CSV',
    dag=dag2
    )

extract_to_gcs_task3 = PostgresToGCSOperator(
    task_id="get_data",
    postgres_conn_id= 'sedawy_connections',
    sql='select *  from src01.customer',
    bucket='postgres-to-gcs',
    filename='Sedawy/customer.csv',
    export_format = 'CSV',
    dag=dag3
    )
extract_to_gcs_task4 = PostgresToGCSOperator(
    task_id="get_data",
    postgres_conn_id= 'sedawy_connections',
    sql='select *  from src01.order',
    bucket='postgres-to-gcs',
    filename='Sedawy/order.csv',
    export_format = 'CSV',
    dag=dag4
    )

extract_to_gcs_task5 = PostgresToGCSOperator(
    task_id="get_data",
    postgres_conn_id= 'sedawy_connections',
    sql='select *  from src01.order_detail',
    bucket='postgres-to-gcs',
    filename='Sedawy/order_detail.csv',
    export_format = 'CSV',
    dag=dag5
    )

extract_to_gcs_task6 = PostgresToGCSOperator(
    task_id="get_data",
    postgres_conn_id= 'sedawy_connections',
    sql='select *  from src01.payment',
    bucket='postgres-to-gcs',
    filename='Sedawy/payment.csv',
    export_format = 'CSV',
    dag=dag6
    )

extract_to_gcs_task7 = PostgresToGCSOperator(
    task_id="get_data",
    postgres_conn_id= 'sedawy_connections',
    sql='select *  from src01.payment_type',
    bucket='postgres-to-gcs',
    filename='Sedawy/payment_type.csv',
    export_format = 'CSV',
    dag=dag7
    )
extract_to_gcs_task8 = PostgresToGCSOperator(
    task_id="get_data",
    postgres_conn_id= 'sedawy_connections',
    sql='select *  from src01.product',
    bucket='postgres-to-gcs',
    filename='Sedawy/product.csv',
    export_format = 'CSV',
    dag=dag8
    )






    # Task to load data from GCS to BigQuery
load_to_bigquery_task1 = GCSToBigQueryOperator(
    task_id='load_to_bigquery02',
    bucket=bucket,
    source_objects="Sedawy/address.csv",  # GCS source path
    destination_project_dataset_table='Data_Platform_Sedawy.address',  # BigQuery table to load data into
    create_disposition='CREATE_IF_NEEDED',
    skip_leading_rows=1,  # If your CSV has a header row
    source_format='CSV',  # Source data format
    dag=dag1
    )
load_to_bigquery_task2 = GCSToBigQueryOperator(
    task_id='load_to_bigquery02',
    bucket=bucket,
    source_objects="Sedawy/chanel.csv",  # GCS source path
    destination_project_dataset_table='Data_Platform_channel.',  # BigQuery table to load data into
    create_disposition='CREATE_IF_NEEDED',
    skip_leading_rows=1,  # If your CSV has a header row
    source_format='CSV',  # Source data format
    dag=dag2
    )
load_to_bigquery_task3 = GCSToBigQueryOperator(
    task_id='load_to_bigquery02',
    bucket=bucket,
    source_objects="Sedawy/customer.csv",  # GCS source path
    destination_project_dataset_table='Data_Platform_Sedawy.customer',  # BigQuery table to load data into
    create_disposition='CREATE_IF_NEEDED',
    skip_leading_rows=1,  # If your CSV has a header row
    source_format='CSV',  # Source data format
    dag=dag3
    )
load_to_bigquery_task4 = GCSToBigQueryOperator(
    task_id='load_to_bigquery02',
    bucket=bucket,
    source_objects="Sedawy/order.csv",  # GCS source path
    destination_project_dataset_table='Data_Platform_Sedawy.order',  # BigQuery table to load data into
    create_disposition='CREATE_IF_NEEDED',
    skip_leading_rows=1,  # If your CSV has a header row
    source_format='CSV',  # Source data format
    dag=dag4
    )
load_to_bigquery_task5 = GCSToBigQueryOperator(
    task_id='load_to_bigquery02',
    bucket=bucket,
    source_objects="Sedawy/order_detail.csv",  # GCS source path
    destination_project_dataset_table='Data_Platform_Sedawy.order_detail',  # BigQuery table to load data into
    create_disposition='CREATE_IF_NEEDED',
    skip_leading_rows=1,  # If your CSV has a header row
    source_format='CSV',  # Source data format
    dag=dag5
    )
load_to_bigquery_task6 = GCSToBigQueryOperator(
    task_id='load_to_bigquery02',
    bucket=bucket,
    source_objects="Sedawy/payment.csv",  # GCS source path
    destination_project_dataset_table='Data_Platform_Sedawy.payment',  # BigQuery table to load data into
    create_disposition='CREATE_IF_NEEDED',
    skip_leading_rows=1,  # If your CSV has a header row
    source_format='CSV',  # Source data format
    dag=dag6
    )
load_to_bigquery_task7 = GCSToBigQueryOperator(
    task_id='load_to_bigquery02',
    bucket=bucket,
    source_objects="Sedawy/payment_type.csv",  # GCS source path
    destination_project_dataset_table='Data_Platform_Sedawy.payment_type',  # BigQuery table to load data into
    create_disposition='CREATE_IF_NEEDED',
    skip_leading_rows=1,  # If your CSV has a header row
    source_format='CSV',  # Source data format
    dag=dag7
    )
load_to_bigquery_task8 = GCSToBigQueryOperator(
    task_id='load_to_bigquery02',
    bucket=bucket,
    source_objects="Sedawy/product.csv",  # GCS source path
    destination_project_dataset_table='Data_Platform_Sedawy.product',  # BigQuery table to load data into
    create_disposition='CREATE_IF_NEEDED',
    skip_leading_rows=1,  # If your CSV has a header row
    source_format='CSV',  # Source data format
    dag=dag8
    )

    # Set task dependencies
[extract_to_gcs_task1,extract_to_gcs_task2,extract_to_gcs_task3,extract_to_gcs_task4,extract_to_gcs_task5,extract_to_gcs_task6,extract_to_gcs_task7,extract_to_gcs_task8] >> [load_to_bigquery_task1,load_to_bigquery_task2,load_to_bigquery_task3,load_to_bigquery_task4,load_to_bigquery_task5,load_to_bigquery_task6,load_to_bigquery_task7,load_to_bigquery_task8]
