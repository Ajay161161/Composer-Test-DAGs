import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import storage

# ---------------------- CONFIGURATION ----------------------
PROJECT_ID = 'profound-surge-418508'  
DATASET = 'composer_test'
TABLE = 'test2'
BUCKET = 'raw-data-for-composer'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': datetime.datetime(2024, 4, 1)  # Adjust start date
}

with DAG(
    'gcs_to_bq_incremental',
    default_args=default_args,
    schedule_interval='0 0 * * *'  # Daily schedule
) as dag:

    def get_latest_loaded_timestamp():
        """Retrieves the timestamp of the last loaded file from the BigQuery metadata table."""
        query = f"""
            SELECT last_loaded_timestamp 
            FROM {DATASET}.metadata 
            ORDER BY last_loaded_timestamp DESC
            LIMIT 1
        """
        result = BigQueryHook(location='asia-south1').get_records(query)

        if not result:  # No previous loads
            return None
        else:
            return result[0][0]  # Extract timestamp

    def filter_new_files(**kwargs):
        """Filters GCS files based on the last processed timestamp."""
        ti = kwargs['ti']
        last_loaded_timestamp = ti.xcom_pull(task_ids='get_latest_loaded_timestamp')
    
        client = storage.Client()
        bucket = client.get_bucket(BUCKET)
        all_files = [blob.name for blob in bucket.list_blobs()]
    
        new_files = []
        for file in all_files:
            blob = bucket.get_blob(file)
            if blob is not None:
                updated_time = blob.updated
                if last_loaded_timestamp is None or updated_time > last_loaded_timestamp:
                    new_files.append(f'gs://{BUCKET}/{file}')
    
        ti.xcom_push(key='new_files', value=new_files)


    def load_to_bigquery():
        """Loads new files from GCS to BigQuery."""
        ti = kwargs['ti']
        new_files = ti.xcom_pull(task_ids='filter_new_files', key='new_files')

        for file in new_files:
            load_job = BigQueryInsertJobOperator(
                task_id=f'load_{file.split("/")[-1]}',
                configuration={
                    'load': {
                        'sourceUris': [file],
                        'destinationTable': {
                            'projectId': PROJECT_ID,
                            'datasetId': DATASET,
                            'tableId': TABLE,
                        },
                        'schema': [
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
                        'writeDisposition': 'WRITE_APPEND',
                    }
                }
            )
            load_job.execute(context=kwargs)

    def update_metadata():
        """Updates the metadata table in BigQuery with the latest timestamp."""
        ti = kwargs['ti']
        last_loaded_timestamp = ti.xcom_pull(task_ids='filter_new_files', key='new_files')

        if last_loaded_timestamp:
            query = f"""
                INSERT INTO {DATASET}.metadata (last_loaded_timestamp)
                VALUES ('{last_loaded_timestamp}')
            """
            BigQueryHook().run(query=query, project_id=PROJECT_ID, location='US')

    # Define task dependencies
    get_latest_loaded_timestamp_task = PythonOperator(
        task_id='get_latest_loaded_timestamp',
        python_callable=get_latest_loaded_timestamp,
        provide_context=True
    )

    filter_new_files_task = PythonOperator(
        task_id='filter_new_files',
        python_callable=filter_new_files,
        provide_context=True
    )

    load_to_bigquery_task = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_to_bigquery,
        provide_context=True
    )

    update_metadata_task = PythonOperator(
        task_id='update_metadata',
        python_callable=update_metadata,
        provide_context=True
    )

    get_latest_loaded_timestamp_task >> filter_new_files_task >> load_to_bigquery_task >> update_metadata_task
