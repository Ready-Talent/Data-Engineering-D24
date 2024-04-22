from airflow import DAG
from datetime import datetime
import logging
from google.cloud import storage
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

logging.basicConfig(level=logging.INFO)

def log_error(context):
    logging.error(f"Error Occurred: {context['exception']}")
    
    if 'ti' in context:
        ti = context['ti']
        errors = ti.task.get_task_errors()
        if errors:
            logging.error("Additional errors found in the errors collection:")
            for error in errors:
                logging.error("- %s", error)
                
# Define parameters
gcs_bucket = 'chicago-taxi-test-de24'
source_directory = 'data/'
file_prefix = 'large_file_'
batch_size = 50 

storage_client = storage.Client()
blobs_list = storage_client.list_blobs(bucket_or_name=gcs_bucket, prefix="data")
total_files = sum(1 for _ in blobs_list)
num_batches = total_files // batch_size + (1 if total_files % batch_size != 0 else 0)

dag = DAG (
        dag_id="abdelrahman_06_gcs_to_bq", 
        schedule_interval=None, 
        start_date=datetime(2021, 1, 1), 
        catchup=False,
        on_failure_callback=log_error
    )


start = EmptyOperator(task_id="start_task", dag=dag)

# Load each batch sequentially
for i in range(num_batches):
    task_id = f"load_batch_{i}"
    source_objects = [f"{source_directory}{file_prefix}{j}.csv" for j in range(i * batch_size + 1, (i + 1) * batch_size + 1)]

    load_data = GCSToBigQueryOperator(
        task_id=task_id,
        bucket=gcs_bucket,
        source_objects=source_objects,
        source_format="CSV",
        destination_project_dataset_table="your_project.your_dataset.your_table",
        autodetect=True,
        field_delimiter=',',
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        encoding='UTF-8',
        dag=dag
    )

# load_data = GCSToBigQueryOperator(
#     task_id = "chicago_taxi_gcs_to_bq",
#     bucket="chicago-taxi-test-de24",
#     source_objects="data/*.csv",
#     source_format="CSV",
#     destination_project_dataset_table="SRC_06.chicago_taxi",
#     autodetect=True,
#     field_delimiter=';',
#     skip_leading_rows=1,
#     write_disposition="WRITE_APPEND",
#     create_disposition="CREATE_IF_NEEDED",
#     encoding='UTF-8',
#     dag=dag
#     )

end = EmptyOperator(task_id="end_task", dag=dag)

start >> load_data >> end
