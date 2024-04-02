import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator


# Custom Python logic
yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

# Default arguments
default_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# DAG definitions
with DAG(
    dag_id='GCS_to_BQ_and_AGG',
    catchup=False,
    schedule_interval=timedelta(days=1),
    default_args=default_args
) as dag:

    # Dummy start task
    start_task = DummyOperator(
        task_id='start',
        dag=dag
    )

    # GCS to BigQuery data load task
    load_data_task = GoogleCloudStorageToBigQueryOperator(
        task_id='load_data_to_bq',
        bucket='raw-data-for-composer',
        source_objects=['composer_test_data.csv'],
        destination_project_dataset_table='profound-surge-418508.composer_test.gcs_to_bq_table',
        schema_fields=[
            {'name': 'first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'age', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'phone_number', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'address', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'city', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'postal_code', 'type': 'STRING', 'mode': 'NULLABLE'}  
        ],
        skip_leading_rows=1,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        dag=dag
    )

    # BigQuery aggregation task
    create_aggr_table_task = BigQueryOperator(
        task_id='create_aggregation_table',
        use_legacy_sql=False,
        allow_large_results=True,
        sql="""  # Multi-line string for better SQL readability
            CREATE OR REPLACE TABLE profound-surge-418508.composer_test.bq_table_aggr AS 
            SELECT 
                first_name,
                SUM(age) as sum_age 
            FROM profound-surge-418508.composer_test.gcs_to_bq_table 
            GROUP BY 
                first_name
        """,
        dag=dag
    )

    # Dummy end task
    end_task = DummyOperator(
        task_id='end',
        dag=dag
    )

    # Task dependencies
    start_task >> load_data_task >> create_aggr_table_task >> end_task
