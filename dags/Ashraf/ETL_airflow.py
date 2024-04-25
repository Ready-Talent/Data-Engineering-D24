from abc import ABC, abstractmethod
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

class GCSEtL:

    def extract_to_bigquery(self, BQ_BUCKET, FILENAME, destination, table_name, dag, fromat = 'CSV'):
        gcs_to_biqquery = GCSToBigQueryOperator(
            task_id=f"{table_name}_from_gcs_to_bigquery",
            bucket=BQ_BUCKET,
            source_objects=FILENAME,
            destination_project_dataset_table=f"{destination}.{table_name}",
            dag=dag,
            skip_leading_rows=1,
            source_format=fromat
        )
        return gcs_to_biqquery

    def load_from_postgres(self, PG_SCHEMA, PG_TABLE, BQ_BUCKET, FILENAME, PG_CONN_ID, dag, format = 'CSV'):
        postgres_to_gcs = PostgresToGCSOperator(
            task_id=f'{PG_TABLE}_from_postgres_to_gcs',
            sql=f'SELECT * FROM "{PG_SCHEMA}"."{PG_TABLE}";',
            bucket=BQ_BUCKET,
            filename=FILENAME,
            postgres_conn_id=PG_CONN_ID,
            export_format=format,  # You can change the export format as needed
            dag = dag,
            )
        return postgres_to_gcs

