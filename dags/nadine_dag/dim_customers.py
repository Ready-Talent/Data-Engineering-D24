from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow import DAG
from datetime import datetime

dag = DAG(
    dag_id="insert_customer_dim_bigquery_Nadine",
    description="Run create and insert queries to transfer dimensions from postgres to bigquery",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

create_table = BigQueryExecuteQueryOperator(task_id = "create_dim_customer_table",
                                            sql = 'dags\nadine_dag\sql_queries\create_customers.sql',
                                            uselegacySQL = False,
                                            dag = dag)

insert_table = BigQueryExecuteQueryOperator(task_id = "insert_into_dim_customer_table",
                                            sql = 'dags\nadine_dag\sql_queries\insert_customers.sql',
                                            uselegacySQL = False,
                                            dag = dag)

start_task = EmptyOperator(task_id="start_task", dag=dag)

end_task = EmptyOperator(task_id="end_task", dag=dag)


start_task >> create_table >> insert_table >> end_task