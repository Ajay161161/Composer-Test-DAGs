import os
import json
import datetime
import uuid
from airflow import DAG
from typing import Any
from dateutil.relativedelta import relativedelta
import requests
import google.auth

from google.cloud import storage
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task_group
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from google.auth.transport.requests import AuthorizedSession

AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
BQ_API_ENDPOINT = "https://bigquery.googleapis.com/bigquery/v2"
CREDENTIALS, _ = google.auth.default(scopes=[AUTH_SCOPE])

today = datetime.datetime.today()
yesterday = today - datetime.timedelta(days=1)
# Default arguments
default_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5)
}
'''
    This function makes an API call to GCP rest API's with the required authentication token
'''
def make_api_request(url: str, method: str = "GET", **kwargs: Any) -> str:
    
    authed_session = AuthorizedSession(CREDENTIALS)
    # Set the default timeout, if missing
    if "timeout" not in kwargs:
        kwargs["timeout"] = 90
    response = authed_session.request(method, url, **kwargs)

    if response.status_code == 403:
          raise requests.HTTPError(
              "You do not have a permission to perform this operation. "
              "Please check the auth credentials"
              f"{response.headers} / {response.text}"
          )
    elif response.status_code not in [200, 202, 204]:
        response.raise_for_status()
    else:
        return response.text

'''
    This function reads the archive configuration from a GCS config bucket.
    The archive config contains necessary parameters like table to archive,
    retention days, etc., to run the archive job
'''
def read_archive_config():
    config_bucket = os.environ["config_bucket"]


    config_prefix = "composer/bigquery_archive"

    config_file = "bigquery_archive_config.json"

    client = storage.Client()

    print(f'Reading config from bucket {config_bucket} prefix {config_prefix}/{config_file}')

    bucket = client.get_bucket(config_bucket)
    params_blob = bucket.get_blob(config_prefix + "/" + config_file)
    config_dict = json.loads(params_blob.download_as_string(client=None))
   
    print(f'DAG Config params: {config_dict}')

    return config_dict 

'''
    This function determines the Bigquery Partition id to be archived
    Partition is determined based on the retention days. 
    Currently only supports monthly partitions but can be extended to support all partition types
'''   
def get_partition_to_archive(retention_days: int, today: datetime) -> str:
  months, days = divmod(retention_days, 30)
  archive_month = months + 1
  date4monthback = today - relativedelta(months=+archive_month)
  partition = f"{date4monthback.year:04d}{date4monthback.month:02d}"
  return partition

# Scheduled to run on the 1st of every month to archive data in Bigquery partition older than 90 days (3 months) to GCS bucket.
# Example if this job runs in October then this will archive the data in June Partition

# DAG definitions 
with DAG(dag_id='bigquery_export_gcs_dag',
      catchup=False,
      schedule_interval="0 0 1 * *",
      default_args=default_args
      ) as dag:
    
    dag_start = EmptyOperator(
      task_id='dag_start'
    ) 

    archive_config = read_archive_config()
    project_id = archive_config["project"]
    location = archive_config["location"]
    
    '''
        DAG task to copy partition data from BQ to GCS and then delete the partition after copy is completed
    '''
    @task_group(group_id="archive_bq_table", prefix_group_id=False)
    def archive_bq_table(table_archive_config):

        partition = get_partition_to_archive(table_archive_config["data_retention_in_days"], today)
        dataset = table_archive_config['dataset_name']
        table_name = table_archive_config['table_name']
        bucket_uri = f"gs://{table_archive_config['export_bucket']}/{project_id}/{dataset}/{table_name}/{partition}/bq-export-*{table_archive_config['export_file_extension']}"
        source_table = f"{project_id}.{dataset}.{table_name}${partition}"
        task_id_suffix = f"{dataset}_{table_name}"


        def delete_partition():
            request_url = f"{BQ_API_ENDPOINT}/projects/{project_id}/datasets/{dataset}/tables/{table_name}${partition}"
            response = make_api_request(url=request_url, method="DELETE")
            print(f"DELETE BQ Partition API request: {request_url} response: {response}")


        echo_task = BashOperator(
            task_id = f"echo_task_{task_id_suffix}",
            bash_command = 'echo "Archiving BigQuery Table ' + source_table + ' to GCS bucket ' + bucket_uri + '"'
        )

        copy_task = BigQueryToGCSOperator(
            source_project_dataset_table=source_table,
            destination_cloud_storage_uris= [bucket_uri],
            project_id = project_id,
            export_format = table_archive_config["export_file_format"],
            print_header = True,
            gcp_conn_id = "google_cloud_default",
            location = location,
            job_id = uuid.uuid4(),
            task_id = f"export_bq_to_gcs_{task_id_suffix}"
        )

        delete_task = PythonOperator(
            task_id = f"delete_bq_partiton_task_{task_id_suffix}",
            python_callable = delete_partition
        )
    
        echo_task >> copy_task >>  delete_task

    dag_end = EmptyOperator(  
      task_id="dag_end"
    )

    for config in archive_config["bq_archive_config"]:
        dag_start >> archive_bq_table(config) >> dag_end