import logging
from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator



dag = DAG(
    dag_id="rowym_move_data_postgres_GCS",
    description="Moves data from Postgresto GCS",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

start_task = EmptyOperator(task_id="start_task", dag=dag)

get_data = PostgresToGCSOperator(
    task_id="get_data_from_postgres_to_GCS",
    postgres_conn_id="Postgres_Rowym_conn",
    sql="SELECT * from src01.order",
    bucket="postgres-to-gcs",
    source_format = "CSV",
    filename="Rowym/order.csv",
    gzip=False,
    dag = dag
)

end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> get_data >> end_task
