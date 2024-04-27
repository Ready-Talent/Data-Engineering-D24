import logging
from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


dag = DAG(
    dag_id="rowym_transfer_dim_to_bigquery",
    description="Transfers dim data to bigquery",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

move_data = BigQueryExecuteQueryOperator(
    task_id='move_customer_data',
    sql = 'sqlQueries.sql',
    destination_dataset_table='Data_Platform_Rowym.Customer',
    write_disposition='WRITE_EMPTY',
    use_legacy_sql = False,
    dag=dag
)

start_task = EmptyOperator(task_id="start_task", dag=dag)

end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> move_data >> end_task