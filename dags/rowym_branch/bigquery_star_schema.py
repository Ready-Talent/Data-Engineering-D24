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

create_table = BigQueryExecuteQueryOperator(
    task_id='create_dim_customer_table',
    sql = 'SQL/sqlQueries_create.sql',
    use_legacy_sql = False,
    dag=dag
)

insert_data = BigQueryExecuteQueryOperator(
    task_id='insert_dim_customer_data',
    sql = 'SQL/sqlQueries_insert.sql',
    use_legacy_sql = False,
    dag=dag
)

start_task = EmptyOperator(task_id="start_task", dag=dag)

end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> create_table >> insert_data >> end_task