from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.gcs_list_operator import GoogleCloudStorageListOperator
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

def check_and_load_file(ds, **kwargs):
    processed_files = kwargs['task_instance'].xcom_pull(task_ids='load_data_to_bq', key='processed_files')
    bucket = 'raw-data-for-composer'
    files_to_process = []

    # List all files in the bucket
    hook = GoogleCloudStorageHook()
    files = hook.list(bucket=bucket, prefix='*.csv')

    # Check which files are new or modified
    for file in files:
        if file not in processed_files:
            files_to_process.append(file)

    # Load new or modified files to BigQuery
    for file in files_to_process:
        load_data_task = GoogleCloudStorageToBigQueryOperator(
            task_id=f'load_data_to_bq_{file}',
            bucket=bucket,
            source_objects=[file],
            destination_project_dataset_table='profound-surge-418508.composer_test.gcs_test_data_load',
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
            dag=dag
        )

        load_data_task.execute(context=kwargs)

    # Update the list of processed files
    new_processed_files = processed_files + files_to_process
    return new_processed_files

# DAG definitions
with DAG(
    dag_id='GCS_to_BQ',
    catchup=False,
    schedule_interval=timedelta(days=1),
    default_args=default_args
) as dag:

    # Dummy start task
    start_task = DummyOperator(
        task_id='start',
        dag=dag
    )

    # List all files in the bucket
    list_files_task = GoogleCloudStorageListOperator(
        task_id='list_files',
        bucket='raw-data-for-composer',
        prefix='*.csv',
        dag=dag
    )

    # PythonOperator to check and load files
    check_and_load_task = PythonOperator(
        task_id='check_and_load_files',
        python_callable=check_and_load_file,
        provide_context=True,
        dag=dag
    )

    # Dummy end task
    end_task = DummyOperator(
        task_id='end',
        dag=dag
    )

    # Task dependencies
    start_task >> list_files_task >> check_and_load_task >> end_task
