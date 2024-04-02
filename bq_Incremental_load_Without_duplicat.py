import os
from datetime import datetime, timedelta
from google.cloud import storage 
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.python import PythonOperator


# Custom logic for incremental logic
def get_last_processed_filename(bucket_name, prefix):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    # Assumes a file to track last processed time (e.g., 'last_processed.txt')
    blob = bucket.blob(f'{prefix}/last_processed.txt') 
    if blob.exists():
        last_processed_timestamp = blob.download_as_string().decode('utf-8')  

        # Find files modified AFTER the last processed timestamp
        blobs = bucket.list_blobs(prefix=prefix)
        for blob in blobs:
            if blob.name != f'{prefix}/last_processed.txt' and blob.updated > datetime.strptime(last_processed_timestamp, '%Y-%m-%d %H:%M:%S.%f'): 
                return blob.name  

        return None  
    else:
        return None  

def update_last_processed(context, bucket_name, execution_date):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    blob = bucket.blob(f'{prefix}/last_processed.txt') 
    blob.upload_from_string(execution_date)  

# Custom logic
yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())
start_date = '{{ ds }}' 

# Default Arguments:
default_args = {
    'start_date': yesterday, 
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Parameterized paths
bucket = 'raw-data-for-composer'  
bq_dataset = 'composer_test' 

# DAG definition
with DAG(
    dag_id='GCS_to_BQ_Load_dynamic',
    catchup=False,
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    params={
        'project_id': 'profound-surge-418508',  
        'gcs_to_bq_table1': 'gcs_to_bq_table1',   
    }
) as dag:

    start_task = DummyOperator(task_id='start')

    load_data_task = GoogleCloudStorageToBigQueryOperator(
        task_id='load_data_to_bq',
        bucket=bucket,
        source_objects=['{{ ti.xcom_pull(task_ids="get_next_file") }}'],  
        source_format='CSV',
        destination_project_dataset_table=f"{dag.params['project_id']}.{bq_dataset}.{dag.params['gcs_to_bq_table1']}",
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
        on_success_callback=lambda context: update_last_processed(context, bucket, '{{ execution_date }}') 
    )

    get_next_file_task = PythonOperator(
        task_id='get_next_file',
        python_callable=get_last_processed_filename,
        op_kwargs={'bucket_name': bucket, 'prefix': ''},  
        provide_context=True,
    )

    end_task = DummyOperator(task_id='end') 

    start_task >> get_next_file_task >> load_data_task >> end_task 
