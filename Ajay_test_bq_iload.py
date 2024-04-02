from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

# Define default_args
yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())
default_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Create the DAG
with DAG(
    dag_id='Aj_GCS_to_BQ',
    catchup=False,
    schedule_interval=timedelta(days=1),
    default_args=default_args
) as dag:
    # Start task
    start_task = DummyOperator(
        task_id='start',
        dag=dag
    )
    
    # Check if file exists in tracking table
    check_file_task = BigQueryOperator(
        task_id='check_file_in_tracking',
        sql="SELECT filename FROM `composer_test.tracking_table` WHERE filename = '{{ source_object }}'",
        use_legacy_sql=False,
        dag=dag
    )
    
    # Load data from GCS to BigQuery for all CSV files
    load_data_task = GoogleCloudStorageToBigQueryOperator(
        task_id='load_data_to_bq',
        bucket='raw-data-for-composer',
        source_objects=['*.csv'], 
        destination_project_dataset_table='profound-surge-418508.composer_test.test_2',
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
        write_disposition='WRITE_APPEND',
        ignore_unknown_values=True,
        dag=dag
    )
    
    # Insert metadata into tracking table for each loaded file
    insert_metadata_task = BigQueryOperator(
        task_id='insert_metadata_to_tracking',
        sql="""
            INSERT INTO `composer_test.tracking_table`
            (filename, loaded_time, target_table)
            SELECT
                '{{ source_object }}' AS filename,
                CURRENT_TIMESTAMP() AS loaded_time,
                'test_2' AS target_table
        """,
        use_legacy_sql=False,
        dag=dag
    )
   
    # Dummy end task
    end_task = DummyOperator(
        task_id='end',
        dag=dag
    )
   
    # Set task dependencies
    start_task >> check_file_task >> load_data_task >> insert_metadata_task >> end_task
