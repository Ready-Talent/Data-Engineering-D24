import great_expectations as ge
from google.cloud import bigquery


def run_ge_check(validation_query: str):
    client = bigquery.Client()
    query_job = client.query(validation_query)
    results = query_job.result()

    df = results.to_dataframe()
    ge_df = ge.from_pandas(df)

    expectations_config = {
        "expectation_suite_name": "name",
        "expectations": [
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {
                    "min_value": 1,
                    "max_value": 1000,
                },
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {
                    "column": "address_id",
                },
            },
        ]
    }

    results = ge_df.validate(expectations_config)

    print(results["success"])

    if not results["success"]:
        raise ValueError("Data quality check failed")


sql = """SELECT * FROM `ready-data-engineering-p24.data_platform.dim_customer`"""

run_ge_check(sql)




