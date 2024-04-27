from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

dag = DAG(
    dag_id="create_and_insert_dim_tables_01",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

start_task = EmptyOperator(task_id="start_task", dag=dag)
end_task = EmptyOperator(task_id="end_task", dag=dag)

tables = ['dim_customer', 'dim_product', 'dim_date', 'junk_dim', 'fact_sales']
for table in tables:
    create_query = BigQueryExecuteQueryOperator(
        task_id=f"create_{table}",
        sql= f'sql/create_{table}.sql',
        dag=dag,
        use_legacy_sql=False
    )
    insert_query = BigQueryExecuteQueryOperator(
        task_id=f"insert_into_{table}",
        sql= f'sql/{table}.sql',
        dag=dag,
        use_legacy_sql=False
    )
    if table == 'fact_sales':
        end_task >> create_query >> insert_query
    else:
        start_task >> create_query >> insert_query >> end_task




