from airflow import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryInsertJobOperator




dag  = DAG(
        dag_id="abdelrahman_06_bq_src_to_landing", 
        schedule_interval=None, 
        start_date=datetime(2021, 1, 1), 
        catchup=False,
        template_searchpath= "/dags/abdelrahman-tamer/sql"
    )


start_task = EmptyOperator(task_id="start_task", dag=dag)


create_table_and_insert_data = BigQueryInsertJobOperator(
    task_id = "create_table_and_insert_data",
    configuration={
        "query": {
            "query": "{% include 'dim_customer.sql' %}",
            "'createDisposition": "CREATE_IF_NEEDED",
            "writeDisposition": "WRITE_APPEND"
        }
        
    },
    dag=dag
)

end_task = EmptyOperator(task_id="end_task", dag=dag)


start_task >> create_table_and_insert_data  >> end_task
