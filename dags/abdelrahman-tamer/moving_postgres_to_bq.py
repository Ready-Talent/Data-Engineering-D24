from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

gcs_bucket = "postgres-to-gcs"

tables_and_queries = [
    {"table": "address", "query": "SELECT * FROM src01.address;"},
    {"table": "customer", "query": "SELECT * FROM src01.customer;"},
    {"table": "channel", "query": "SELECT * FROM src01.channel;"},
    {"table": "order", "query": "SELECT * FROM src01.order;"},
    {"table": "order_detail", "query": "SELECT * FROM src01.order_detail;"},
    {"table": "payment", "query": "SELECT * FROM src01.payment;"},
    {"table": "payment_type", "query": "SELECT * FROM src01.payment_type;"},
    {"table": "product", "query": "SELECT * FROM src01.product;"},
]

dag  = DAG(
        dag_id="abdelrahman_06_postgres_bq_dag", 
        schedule_interval=None, 
        start_date=datetime(2021, 1, 1), 
        catchup=False,
    )

start_task = EmptyOperator(task_id="start_task", dag=dag)


gcs_tasks = []
bq_tasks = []
for index, item in enumerate(tables_and_queries): 

    pg_to_gcs = PostgresToGCSOperator(
            task_id=f"pg_to_gcs_{item['table']}",
            postgres_conn_id="abdelrahman_06_postgres_connection",
            bucket=gcs_bucket,
            filename=f"abdelrahman_06/{item['table']}.csv",
            sql=item["query"],
            export_format="csv",
            dag=dag
        )
    
    gcs_tasks.append(pg_to_gcs)
    
    move_data_to_bigquery_table = GCSToBigQueryOperator(
        task_id = f"move_{item['table']}_to_bigquery_table",
        bucket=gcs_bucket,
        source_objects=f"abdelrahman_06/{item['table']}.csv",
        source_format="CSV",
        destination_project_dataset_table=f"SRC_06.{item['table']}",
        field_delimiter=',',
        autodetect=True,
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        dag=dag
    )
    
    bq_tasks.append(move_data_to_bigquery_table)
    
end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> gcs_tasks >> bq_tasks >> end_task
    

    

