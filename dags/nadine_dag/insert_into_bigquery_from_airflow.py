from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow import DAG
from datetime import datetime

dag = DAG(
    dag_id="insert_into_bigquery_from_airflow",
    description="Transfer data into bigquery from airflow",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

start_task = EmptyOperator(task_id="start_task", dag=dag)

insert_bigquery = mobile_push_stat = BigQueryExecuteQueryOperator(
        task_id="into_bigquery_from_airflow",
        sql="sql_queries\sql_customers.sql",
        destination_dataset_table=f"data_platform_Nadine",
        use_legacy_sql=False,
        #api_resource_configs={"jobTimeoutMs": "3600000"},
        gcp_conn_id="bigquery_work",
        write_disposition="WRITE_TRUNCATE",
        use_legacy_sql=False,
    )

end_task = EmptyOperator(task_id="end_task", dag=dag)


start_task >> insert_bigquery >> end_task