from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryExecuteQueryOperator,
)
from airflow.utils.dates import days_ago

# Define your DAG and its parameters
with DAG(
    dag_id="bigquery_native_table_sync",
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
    },
    start_date=days_ago(1),
    schedule_interval="@daily",
    tags=['bigquery', 'data-sync'],
) as dag:

    # Project and table variables
    PROJECT_ID = 'profound-surge-418508'  
    DATASET = 'composer_test'  
    EXTERNAL_TABLE = 'composerexternaltable'  
    NATIVE_TABLE = 'composernativetable'

    # Task 1 -  Check if Native Table Exists
    check_table_exists = BigQueryCheckOperator(
        task_id="check_native_table_exists",
        sql=f"SELECT 1 FROM `{PROJECT_ID}.{DATASET}.{NATIVE_TABLE}`",
        use_legacy_sql=False,
    )

    # Task 2 - Create Native Table (if it doesn't exist)
    create_native_table = BigQueryCreateEmptyTableOperator(
        task_id="create_native_table",
        dataset_id=DATASET,
        table_id=NATIVE_TABLE,
        schema_fields=[
            # Obtain schema from the external table (explained below)
        ],
        # Consider adding table partitioning or clustering options if needed
    )

    # Task 3 - Upsert Data 
    upsert_data = BigQueryExecuteQueryOperator(
        task_id="upsert_data",
        sql=f"""
            MERGE `{PROJECT_ID}.{DATASET}.{NATIVE_TABLE}` AS dest
            USING `{PROJECT_ID}.{DATASET}.{EXTERNAL_TABLE}` AS src
            ON dest.phone_number = src.phone_number

            WHEN MATCHED THEN
                UPDATE SET 
                    -- Update the columns you wish to sync from the external table
            WHEN NOT MATCHED THEN
                INSERT  
                    -- Insert all columns from the external table
            """,
        use_legacy_sql=False,
    )

    # Define the task dependencies
    check_table_exists >> create_native_table >> upsert_data
