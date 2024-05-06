import logging
from datetime import datetime
from pathlib import Path
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


def print_hello():
    logging.info("Abduallah")
    return "printed"

dag = DAG(
    dag_id="Abduallah_transfer_dims_Dag",
    description="Transfer",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)
table_list = ['dim_product','dim_date']
start_task = EmptyOperator(task_id="start_task", dag=dag)

#parent_path = str(Path(__file__).parent)
#path = os.path.join(parent_path, "product.json" )
for name in table_list:
    create_table = BigQueryExecuteQueryOperator(
        task_id="create_{name}_table",
        sql='{name}.sql',
        use_legacy_sql=False,
        dag=dag
    )

end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> create_table >> end_task
