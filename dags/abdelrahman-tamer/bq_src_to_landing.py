from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryInsertJobOperator

sql_file_path = 'sql/dim_customer.sql'
destination_dataset = "data_platform_abdelrahman_tamer"
destination_table_id = "customer"

with open(sql_file_path, 'r') as file:
    sql_query = file.read()


dag  = DAG(
        dag_id="abdelrahman_06_bq_src_to_landing", 
        schedule_interval=None, 
        start_date=datetime(2021, 1, 1), 
        catchup=False,
    )


start_task = EmptyOperator(task_id="start_task", dag=dag)

# excecute_sql_query = BigQueryExecuteQueryOperator(
#         task_id="excecute_sql_query",
#         sql=sql_query,
#         dag=dag
#     )

create_table_and_insert_data = BigQueryInsertJobOperator(
    task_id = "create_table_and_insert_data",
    configuration={
        "query": {
            "query": sql_query,
            "'createDisposition": "CREATE_IF_NEEDED",
            "writeDisposition": "WRITE_APPEND"
        }
        
    },
    dag=dag
)

end_task = EmptyOperator(task_id="end_task", dag=dag)


