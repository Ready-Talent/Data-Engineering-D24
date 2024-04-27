
import logging
from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator



dag = DAG(
    dag_id="Sedawy_Dag",
    description="Simple tutorial DAG",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)


create_dims = BigQueryExecuteQueryOperator(
    task_id="create_dims",
    sql='sql/create_dims',
    use_legacy_sql=True,
    dag=dag
)

insert_dim_customer = BigQueryExecuteQueryOperator(
    task_id="insert_dim_customer",
    sql='sql/insert_dim_customer.sql',
    use_legacy_sql=False,
    dag=dag
)


insert_dim_product = BigQueryExecuteQueryOperator(
    task_id="insert_dim_product",
    sql='sql/insert_dim_product.sql',
    use_legacy_sql=False,
    dag=dag
)
create_dims>>[insert_dim_customer,insert_dim_product]
