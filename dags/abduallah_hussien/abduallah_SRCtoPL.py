import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator

def print_hello():
    logging.info("Abduallah")
    return "printed"

dag = DAG(
    dag_id="Abduallah_Transfer_Dag",
    description="Transfer",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

start_task = EmptyOperator(task_id="start_task", dag=dag)

create_table = BigQueryCreateEmptyTableOperator(
    task_id="create_table",
    dataset_id='Data_Platform_Abduallah',
    table_id="Pl_customer",
    schema_fields=[
            {
                "name": "customer_key",
                "type": "INTEGER",
                "mode": "REQUIRED"
            },
            {
                "name": "customer_id",
                "type": "INTEGER"
            },
            {
                "name": "customer_name",
                "type": "STRING"
            },
            {
                "name": "email",
                "type": "STRING"
            },
            {
                "name": "phone",
                "type": "STRING"
            },
            {
                "name": "address_id",
                "type": "INTEGER"
            },
            {
                "name": "address_street",
                "type": "STRING"
            },
            {
                "name": "address_zipcode",
                "type": "STRING"
            },
            {
                "name": "city_id",
                "type": "INTEGER"
            },
            {
                "name": "city_name",
                "type": "STRING"
            },
            {
                "name": "state_id",
                "type": "INTEGER"
            },
            {
                "name": "state_name",
                "type": "STRING"
            },
            {
                "name": "country_id",
                "type": "INTEGER"
            },
            {
                "name": "country_name",
                "type": "STRING"
            },
            {
                "name": "created_by",
                "type": "STRING",
                "mode": "REQUIRED",
                "description": "The user who created the record"
            },
            {
                "name": "created_at",
                "type": "TIMESTAMP",
                "mode": "REQUIRED",
                "description": "The timestamp when the record was created"
            },
            {
                "name": "modified_by",
                "type": "STRING",
                "description": "The user who last modified the record"
            },
            {
                "name": "modified_at",
                "type": "TIMESTAMP",
                "description": "The timestamp when the record was last modified"
            }
        ]
)

insert_query_job = BigQueryInsertJobOperator(
    task_id="insert_query_job",
    configuration={
        "query": {
            "query": """
INSERT INTO Pl_customer (customer_id, customer_name, email, phone, address_id, address_street, address_zipcode, city_id, city_name, state_id, state_name, country_id, country_name)
SELECT c.customer_id, c."name" AS customer_name, c.email, c.phone, a.address_id, a.street AS address_street, a.zipcode AS address_zipcode, -1 AS city_id, a.city AS city_name, -1 AS state_id, a.state AS state_name, -1 AS country_id, 'US' AS country_name
FROM SRC_03.customer c
LEFT JOIN SRC_03.address a ON a.customer_id = c.customer_id;
""",
            "useLegacySql": False,
            "priority": "BATCH",
        }
    },
    deferrable=True,
)