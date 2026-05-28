import airflow

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.filesystem import FileSensor

from datetime import datetime, timedelta

import json
import os
import re
import io
import time
import glob
import sys
import socket
import logging
import requests
import fastavro
import base64

from google.cloud import storage
from google.cloud import bigquery
from google.cloud import secretmanager

from airflow.models.taskinstance import TaskInstance
from airflow.models import Variable
from airflow.models import DagRun

from airflow.utils.dates import days_ago
from airflow.utils.db import provide_session
from airflow.utils import timezone

import google.cloud.logging


# =============================================================================
# GLOBAL VARIABLE DECLARATIONS
# =============================================================================

global TABLE
global START_DATE_TIME
global DATASET
global BUCKET_NAME
global TGT_PATH
global GCP_NAME
global FEED_NAME
global RUN_ID
global PATTERN
global LOAD_PATTERN
global FILE_FORMAT
global ENV
global IMAGE_DATE
global FILE_NM
global IMAGE_FILE
global FILE_TYPE
global IMAGE_BATCH_DATE
global ODATE
global SQL_FILENAME
global STAGING_DATASET
global TARGET_DATASET
global BATCH_DATASET
global AUTH_VIEWS_DATASET
global STAGING_ARCHIVE_DATASET
global ODATECH
global GCSFILE
global PROJECT_ID
global STEP
global GCSARCHIVEFILE
global avro_count
global bq_count
global TAGGING_ALERT_CHECK
global NULL_ALERT_CHECK
global DUPLICATE_ALERT_CHECK
global EMPTY_FILE_ALERT_CHECK
global BIGQUERY_LABELS
global FILE_PREFIX
global LIST_OF_FILE_MFL
global LOF
global FILE
global DAG_NAME
global base_dir
global base_dag_file_name
global OWNER
global CONTROL_TABLE_UPDATE
global ORIGINAL_IMAGE_DATE
global ORIGINAL_IMAGE_FILE
global ORIGINAL_IMAGE_BATCH_DATE
global ID_CHK
global IF_CHK
global IBD_CHK
global RETRY
global RETRY_COUNT
global ALERT_CREATED
global EXIT
global TOTAL_RETRIES
global RETRY_INTERVAL
global FILES_ARCHIVED
global STEP6_COMPLETED

# --- NEW GLOBALS: Base64 decode support ---
global IS_BASE64_ENCODED
global DECODED_GCS_PATH
global ORIGINAL_ENCODED_FILE_PATH
global DECODED_FILE_CREATED
# --- END NEW GLOBALS ---


# =============================================================================
# GLOBAL VARIABLE INITIALISATIONS
# =============================================================================

FILES_ARCHIVED = False
STEP6_COMPLETED = False
EXIT = False
ALERT_CREATED = False
RETRY = False
RETRY_COUNT = 0

ID_CHK = True
IF_CHK = True
IBD_CHK = True

LIST_OF_FILE_MFL = []

STEP = 0
TOTAL_RETRIES = 4

# RETRY INTERVAL in seconds
RETRY_INTERVAL = 120

CONTROL_TABLE_UPDATE = True
EMPTY_FILE_ALERT_CHECK = True

# --- NEW INITIALISATIONS: Base64 decode support ---
IS_BASE64_ENCODED = False
DECODED_GCS_PATH = ""
ORIGINAL_ENCODED_FILE_PATH = ""
DECODED_FILE_CREATED = False
# --- END NEW INITIALISATIONS ---


# =============================================================================
# DIRECTORY SETUP
# =============================================================================

def update_directories(
    base_dag_file_name_p,
    base_dir_p
):

    global base_dir
    global base_dag_file_name

    base_dir = base_dir_p
    base_dag_file_name = base_dag_file_name_p


# =============================================================================
# VARIABLE UPDATE FUNCTION
# =============================================================================

def update_variables(

    dag_name_p,
    BUCKET_NAME_P,
    TGT_PATH_P,
    GCP_NAME_P,
    FEED_NAME_P,
    PATTERN_P,
    LOAD_PATTERN_P,
    FILE_FORMAT_P,
    ENV_P,
    IMAGE_DATE_P,
    IMAGE_FILE_P,
    FILE_TYPE_P,
    IMAGE_BATCH_DATE_P,
    FILE_P,
    DATASET_P,
    TABLE_P,
    ODATE_P,
    SQL_FILENAME_P,
    STAGING_DATASET_P,
    TARGET_DATASET_P,
    BATCH_DATASET_P,
    AUTH_VIEWS_DATASET_P,
    STAGING_ARCHIVE_DATASET_P,
    ODATECH_P,
    GCSFILE_P,
    PROJECT_ID_P,
    GCSARCHIVEFILE_P,
    TAGGING_ALERT_CHECK_P,
    NULL_ALERT_CHECK_P,
    DUPLICATE_ALERT_CHECK_P,
    BIGQUERY_LABELS_P,
    FILE_PREFIX_P,
    OWNER_P

):

    global TABLE
    global DAG_NAME
    global START_DATE_TIME
    global DATASET
    global BUCKET_NAME
    global TGT_PATH
    global GCP_NAME
    global FEED_NAME
    global RUN_ID
    global PATTERN
    global LOAD_PATTERN
    global FILE_FORMAT
    global ENV
    global IMAGE_DATE
    global FILE_NM
    global IMAGE_FILE
    global FILE_TYPE
    global IMAGE_BATCH_DATE
    global ODATE
    global SQL_FILENAME
    global STAGING_DATASET
    global TARGET_DATASET
    global BATCH_DATASET
    global AUTH_VIEWS_DATASET
    global STAGING_ARCHIVE_DATASET
    global ODATECH
    global GCSFILE
    global PROJECT_ID
    global STEP
    global GCSARCHIVEFILE
    global FILE
    global TAGGING_ALERT_CHECK
    global NULL_ALERT_CHECK
    global DUPLICATE_ALERT_CHECK
    global BIGQUERY_LABELS
    global FILE_PREFIX
    global OWNER
    global CONTROL_TABLE_UPDATE

    DAG_NAME = dag_name_p
    BUCKET_NAME = BUCKET_NAME_P
    TGT_PATH = TGT_PATH_P
    GCP_NAME = GCP_NAME_P
    FEED_NAME = FEED_NAME_P
    PATTERN = PATTERN_P
    LOAD_PATTERN = LOAD_PATTERN_P
    FILE_FORMAT = FILE_FORMAT_P
    ENV = ENV_P
    IMAGE_DATE = IMAGE_DATE_P
    IMAGE_FILE = IMAGE_FILE_P
    FILE_TYPE = FILE_TYPE_P
    IMAGE_BATCH_DATE = IMAGE_BATCH_DATE_P
    FILE = FILE_P
    DATASET = DATASET_P
    TABLE = TABLE_P
    ODATE = ODATE_P
    SQL_FILENAME = SQL_FILENAME_P
    STAGING_DATASET = STAGING_DATASET_P
    TARGET_DATASET = TARGET_DATASET_P
    BATCH_DATASET = BATCH_DATASET_P
    AUTH_VIEWS_DATASET = AUTH_VIEWS_DATASET_P
    STAGING_ARCHIVE_DATASET = STAGING_ARCHIVE_DATASET_P
    ODATECH = ODATECH_P
    GCSFILE = GCSFILE_P
    PROJECT_ID = PROJECT_ID_P
    GCSARCHIVEFILE = GCSARCHIVEFILE_P
    TAGGING_ALERT_CHECK = TAGGING_ALERT_CHECK_P
    NULL_ALERT_CHECK = NULL_ALERT_CHECK_P
    DUPLICATE_ALERT_CHECK = DUPLICATE_ALERT_CHECK_P
    BIGQUERY_LABELS = BIGQUERY_LABELS_P
    FILE_PREFIX = FILE_PREFIX_P
    OWNER = OWNER_P


# =============================================================================
# SENSOR / FILE DETECTION FUNCTIONS
# =============================================================================

def create_object_existence_sensor_task(
    task_id,
    file_prefix,
    BUCKET_NAME,
    TGT_PATH,
    FILE_SENSOR_TIMEOUT,
    FILE_SENSOR_POKE_INTERVAL,
    dag
):

    PythonOperator(
        task_id=task_id,
        python_callable=find_avro_files,
        provide_context=True,
        op_args=[
            file_prefix,
            BUCKET_NAME,
            TGT_PATH,
            FILE_SENSOR_TIMEOUT,
            FILE_SENSOR_POKE_INTERVAL
        ],
        dag=dag,
    )


def get_sa_key():

    client = secretmanager.SecretManagerServiceClient()

    secret_name = (
        "projects/hsbc-10138242-dfeurope-prod/"
        "secrets/hsbc-10138242-dfeurope-prod-airflowv2-1p-1/"
        "versions/latest"
    )

    response = client.access_secret_version(
        name=secret_name
    )

    return json.loads(
        response.payload.data.decode("UTF-8")
    )


def find_avro_files(
    file_prefix,
    BUCKET_NAME,
    TGT_PATH,
    FILE_SENSOR_TIMEOUT,
    FILE_SENSOR_POKE_INTERVAL
):

    start_time = time.time()

    start_time_utc = time.asctime(time.gmtime())

    if check_holiday():
        print("Happy Holiday, Feed Skipped!...")
        return True

    while True:

        list_of_files = list_files_with_prefix(
            file_prefix,
            BUCKET_NAME,
            TGT_PATH,
            FILE_FORMAT
        )

        if len(list_of_files) > 0:

            context = {
                'task_instance_key_str': (
                    f'Prefix Sensor Succeeded '
                    f'for file: {file_prefix}, '
                    f'bucket: {BUCKET_NAME}'
                )
            }

            task_success_alert(context)
            return True

        elapsed_time = time.time() - start_time

        if elapsed_time > FILE_SENSOR_TIMEOUT:

            print(
                f'Prefix Sensor Failure due '
                f'to timeout for file: '
                f'{file_prefix}, '
                f'bucket: {BUCKET_NAME}'
            )

            context = {
                'params': {
                    'msg_temp': (
                        f'Prefix Sensor Failure '
                        f'due to timeout for file: '
                        f'{file_prefix}, '
                        f'bucket: {BUCKET_NAME}'
                    )
                }
            }

            task_failure_alert(context)

            exit(
                f'Prefix Sensor Failure due '
                f'to timeout for file: '
                f'{file_prefix}, '
                f'bucket: {BUCKET_NAME}'
            )

        print(
            "File not found yet waiting "
            "for next trigger to search again"
        )

        time.sleep(FILE_SENSOR_POKE_INTERVAL)


# =============================================================================
# ALERT FUNCTIONS
# =============================================================================

def task_success_alert(context):

    print(
        f"Task succeeded, "
        f"task_instance_key_str: "
        f"{context['task_instance_key_str']}"
    )


def task_failure_alert(context):

    text = str(context['params'].get('msg_temp'))

    severity = 'WARNING'

    hostname = socket.gethostname()

    log_data = {
        "severity": "WARNING",
        "Message": "Task Failure",
        "Application_Name": DAG_NAME,
        "ERROR": text,
        "Host_Name": hostname,
        "Contact_Owner": OWNER,
        "Environment": ENV.upper(),
        "GCP_Project": GCP_NAME.upper()
    }

    json_log = json.dumps(log_data, indent=2)

    print(
        f"Message from Failure Alert:- "
        f"{json_log}"
    )

    api_key = Variable.get(
        "GDT_WPB_DF_DATA_INGESTION_UK_api_key",
        default_var=None
    )

    xmatter_url = Variable.get(
        "xmatter_url",
        default_var=None
    )

    try:

        data = {
            "@key": api_key,
            "@type": "ALERT",
            "object": "HSBCNET Airflow DAG alert",
            "@version": "alertapi-0.1",
            "severity": severity,
            "text": json_log
        }

        response = requests.post(
            xmatter_url,
            data,
            verify=False
        )

        print("Response code:", response.status_code)

        print(
            'send alert to xmatter done: '
            '{}'.format(response.text)
        )

        print("Send xMatters alert: COMPLETED")

    except Exception as e:

        print(f"Failed to send notification: {e}")

        print("Send xMatters alert: FAILED")


def dag_success_alert(context):

    print(
        f"DAG has succeeded, "
        f"run_id: {context['run_id']}"
    )


def dag_failure_alert(context):

    print(
        f"DAG has failed, "
        f"run_id: {context['run_id']}"
    )


# =============================================================================
# BIGQUERY UTILITY FUNCTIONS
# =============================================================================

def not_null_columns():

    client = bigquery.Client()

    table = client.get_table(
        f'{PROJECT_ID}.'
        f'{TARGET_DATASET}.'
        f'{TABLE}'
    )

    not_null_columns = [
        field.name
        for field in table.schema
        if field.mode == "REQUIRED"
    ]

    return not_null_columns


def tagging_fields_null_check():

    query = f"""
        SELECT COUNT(1) AS null_count
        FROM {PROJECT_ID}.{STAGING_DATASET}.{TABLE}
        WHERE PRM_LNE_OF_BUS_CDE_FILTER IS NULL
        OR CTRY_CDE_FILTER IS NULL
        OR ENTITY_CDE_FILTER IS NULL
        OR LAST_INGESTION_TIMESTAMP IS NULL
    """

    result = execute_query(query)

    for row in result:

        if row["null_count"] > 0:

            context = {
                'params': {
                    'msg_temp': (
                        f'Null values found '
                        f'in required columns of '
                        f'{PROJECT_ID}.'
                        f'{STAGING_DATASET}.'
                        f'{TABLE}'
                    )
                }
            }

            exit(
                f"Null Field found in "
                f"Tagging Columns in table "
                f"{PROJECT_ID}."
                f"{STAGING_DATASET}."
                f"{TABLE}"
            )

        else:

            print(
                f"No Null values found "
                f"in Tagging Columns in table "
                f"{PROJECT_ID}."
                f"{STAGING_DATASET}."
                f"{TABLE}"
            )


def find_null_row_count_in_staging():

    not_null_columns_list = not_null_columns()

    if len(not_null_columns_list) > 0:

        null_conditions = " OR ".join(
            [
                f"{col} IS NULL"
                for col in not_null_columns_list
            ]
        )

        query = f"""
            SELECT COUNT(1) AS null_count
            FROM {PROJECT_ID}.{STAGING_DATASET}.{TABLE}
            WHERE {null_conditions}
        """

        result = execute_query(query)

        for row in result:

            if row["null_count"] > 0:

                context = {
                    'params': {
                        'msg_temp': (
                            f'Null values found '
                            f'in required columns of '
                            f'{PROJECT_ID}.'
                            f'{STAGING_DATASET}.'
                            f'{TABLE}'
                        )
                    }
                }

                exit(
                    f"Null Field found in table "
                    f"{PROJECT_ID}."
                    f"{STAGING_DATASET}."
                    f"{TABLE}"
                )

            else:

                print(
                    f"No Null values found in "
                    f"{PROJECT_ID}."
                    f"{STAGING_DATASET}."
                    f"{TABLE}"
                )

    else:

        print(
            "No Required Columns Found, "
            "Null Check Cancelled"
        )


def duplicate_check():

    query = f"""
        SELECT IF(
            (
                SELECT COUNT(*)
                FROM {PROJECT_ID}.{STAGING_DATASET}.{TABLE}
            )
            =
            (
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT *
                    FROM {PROJECT_ID}.{STAGING_DATASET}.{TABLE}
                )
            ),
            TRUE,
            FALSE
        ) AS result
    """

    result = execute_query(query)

    print(f'result 1 {result}')

    result = next(result)

    print(f'result 2 {result}')

    result = result['result']

    print(f'result 3 {result}')

    if result is True:

        print(
            f"No Duplicate found in "
            f"{PROJECT_ID}."
            f"{STAGING_DATASET}."
            f"{TABLE}"
        )

    else:

        print(
            f"Duplicate Found in "
            f"{PROJECT_ID}."
            f"{STAGING_DATASET}."
            f"{TABLE}"
        )

        context = {
            'params': {
                'msg_temp': (
                    f'Duplicate Found in '
                    f'{PROJECT_ID}.'
                    f'{STAGING_DATASET}.'
                    f'{TABLE}'
                )
            }
        }

        exit(
            f'Duplicate Found in '
            f'{PROJECT_ID}.'
            f'{STAGING_DATASET}.'
            f'{TABLE}'
        )


# =============================================================================
# SCHEDULING UTILITY
# =============================================================================

def get_cron_schedule(schedule, run_time):

    if (
        schedule == 'None'
        or
        run_time == 'None'
    ):
        return None

    cron_days = {
        "sunday": 0,
        "monday": 1,
        "tuesday": 2,
        "wednesday": 3,
        "thursday": 4,
        "friday": 5,
        "saturday": 6,
    }

    run_hour, run_minute = map(
        int,
        run_time.split(':')
    )

    if schedule.split('/')[0] == '@weekly':

        try:

            schedule_week = schedule.split('/')

            days_of_run = (
                schedule_week[1].split(',')
            )

            cron_days_of_week = [
                str(cron_days[day.lower()])
                for day in days_of_run
            ]

            run_day = ",".join(cron_days_of_week)

        except Exception as e:

            print(
                "error in creating CRON "
                "for week day"
            )

            raise Exception(
                f"error in creating CRON "
                f"for week day with error:- {e}"
            )

    if schedule.split('/')[0] == '@monthly':

        try:

            day_list = schedule.split('/')[1]

            day_list = day_list.split(',')

            if all(day.isdigit() for day in day_list):

                if all(
                    1 <= int(day) <= 31
                    for day in day_list
                ):

                    run_day = ",".join(day_list)

                else:

                    raise Exception(
                        "Month day not between 1 and 31"
                    )

            else:

                raise Exception(
                    "Month days are not digits"
                )

        except Exception as e:

            raise Exception(
                f"error in creating "
                f"CRON for month day:- {e}"
            )

    if schedule == '@daily':

        return (
            f"{run_minute} "
            f"{run_hour} "
            f"* * *"
        )

    elif schedule.split('/')[0] == '@weekly':

        return (
            f"{run_minute} "
            f"{run_hour} "
            f"* * {run_day}"
        )

    elif schedule.split('/')[0] == '@monthly':

        return (
            f"{run_minute} "
            f"{run_hour} "
            f"{run_day} * *"
        )


# =============================================================================
# CONF FILE READER
# =============================================================================

def read_CONF_Prefix():

    global FEED_NAME
    global base_dir

    print(
        f'CONF File to read is here:- '
        f'{base_dir}/{FEED_NAME}.CONF'
    )

    with open(
        f'{base_dir}/{FEED_NAME}.CONF',
        'r'
    ) as f:
        lines = f.readlines()

    file_prefixes = [
        line.strip().split('|')[1]
        for line in lines
        if line.strip()
    ]

    return file_prefixes


# =============================================================================
# HOLIDAY CALENDAR CHECK
# =============================================================================

def check_holiday():

    global base_dir

    file_path = os.path.join(
        base_dir,
        "HOLIDAY_CALENDAR.txt"
    )

    if not os.path.isfile(file_path):
        return False

    today = datetime.now().strftime("%d/%m/%Y")

    with open(file_path, 'r') as file:

        for line in file:

            line = line.strip()

            if line == today:
                return True

    return False


# =============================================================================
# GCS UTILITY FUNCTIONS
# =============================================================================

def list_files_with_prefix(
    file_prefix,
    BUCKET_NAME,
    TGT_PATH,
    FILE_FORMAT
):

    client = ''

    if 'liveproving' in BUCKET_NAME:
        sa_key = get_sa_key()
        client = (
            storage.Client
            .from_service_account_info(sa_key)
        )
    else:
        client = storage.Client()

    print(
        f'Bucket Name from LOF:- '
        f'{BUCKET_NAME}'
    )

    bucket = client.get_bucket(BUCKET_NAME)

    blobs = list(
        bucket.list_blobs(
            prefix=f'{TGT_PATH}/{file_prefix}'
        )
    )

    print("List of files before Filter")
    print("printing TGT_PATH below")
    print(TGT_PATH)

    ready_blobs = []

    print(blobs)

    blobs = sorted(
        [
            blob
            for blob in blobs
            if blob.name.lower().endswith(
                f'.{FILE_FORMAT.lower()}'
            )
        ],
        key=lambda x: x.updated
    )

    file_prefixes = read_CONF_Prefix()

    sorted_prefixes = sorted(
        file_prefixes,
        key=lambda x: -len(x)
    )

    for blob in blobs:

        for prefix in sorted_prefixes:

            if blob.name.startswith(
                TGT_PATH + "/" + prefix
            ):

                if prefix == file_prefix:
                    ready_blobs.append(blob)
                    break

    print("List of files after Filter")
    print(ready_blobs)

    return ready_blobs


def exit(*args):

    global RETRY
    global RETRY_COUNT
    global DATASET
    global TABLE
    global FILE_PREFIX
    global EXIT
    global TOTAL_RETRIES
    global RETRY_INTERVAL
    global CONTROL_TABLE_UPDATE
    global STEP

    if RETRY and RETRY_COUNT < TOTAL_RETRIES:

        RETRY_COUNT = RETRY_COUNT + 1

        print(
            f"Retrying in 2 Minutes. "
            f"Retry Count: {RETRY_COUNT}"
        )

        time.sleep(RETRY_INTERVAL)

        main(DATASET, TABLE, FILE_PREFIX)

    elif (
        not RETRY
        or
        (
            RETRY_COUNT >= TOTAL_RETRIES
            and
            CONTROL_TABLE_UPDATE == False
        )
    ):

        msg_temp = ''.join(args)

        print(
            f"Execution failed with error "
            f"{msg_temp}. "
            f"Updating Table Status as failed."
        )

        if STEP != '0':

            sql_to_run = f"""

            UPDATE
            {BATCH_DATASET}.STAGING_TABLE_LOAD_HISTORY

            SET
                BATCH_STATUS =
                'ABORTED AT STEP {STEP}',

                BATCH_END_TIME =
                CURRENT_TIMESTAMP

            WHERE
                BATCH_PROD_DATE = '{ODATE}'
                AND DATASET_NAME = '{STAGING_DATASET}'
                AND TABLE_NAME = '{TABLE}'
                AND BATCH_STATUS = 'RUNNING'

            """

            return_value = execute_query(sql_to_run)

        EXIT = True

        raise Exception(msg_temp)

    sys.exit(2)

    EXIT = True

    return 1


def create_gcs_file(
    TABLE,
    BUCKET_NAME,
    TGT_PATH,
    FILE,
    LOAD_PATTERN,
    PATTERN,
    FILE_FORMAT
):

    print(
        f'Table for global check '
        f'{TABLE}-------'
    )

    GCSFILE = (
        f"gs://{BUCKET_NAME}/"
        f"{TGT_PATH}/{FILE}"
    )

    return GCSFILE


def move_gcs_to_archive(
    GCSFILE,
    GCSARCHIVEFILE,
    bucket_name
):

    try:

        storage_client = ''

        if 'liveproving' in bucket_name:
            sa_key = get_sa_key()
            storage_client = (
                storage.Client
                .from_service_account_info(sa_key)
            )
        else:
            storage_client = storage.Client()

        bucket = storage_client.bucket(bucket_name)

        source_blob = bucket.blob(GCSFILE)

        destination_blob = bucket.blob(GCSARCHIVEFILE)

        destination_blob.rewrite(source_blob)

        source_blob.delete()

    except Exception as e:

        print(
            f"Error occured while moving "
            f"{GCSFILE} to "
            f"{GCSARCHIVEFILE} "
            f"Error: {e}"
        )

        return -1


def count_avro_rows(bucket_name, avro_file_path):

    try:

        storage_client = storage.Client()

        bucket = storage_client.bucket(bucket_name)

        blob = bucket.blob(avro_file_path)

        avro_bytes = blob.download_as_bytes()

        with io.BytesIO(avro_bytes) as avro_file:
            reader = fastavro.reader(avro_file)
            row_count = sum(1 for _ in reader)

        return row_count

    except Exception as e:

        print(
            f"Error occured while counting "
            f"rows from avro file. "
            f"Error: {e}"
        )

        return -1


def execute_query(query):

    global BIGQUERY_LABELS

    job_config = bigquery.QueryJobConfig(
        labels=BIGQUERY_LABELS
    )

    try:

        bq_client = ''

        if 'liveproving' in BUCKET_NAME:
            sa_key = get_sa_key()
            bq_client = (
                bigquery.Client
                .from_service_account_info(sa_key)
            )
        else:
            bq_client = bigquery.Client()

        query_job = bq_client.query(
            query,
            job_config=job_config
        )

        rows = query_job.result()

        print(rows)

        return rows

    except Exception as e:

        print(
            f"An error occured while "
            f"executing query :- "
            f"{query} "
            f"Error: {e}"
        )

        return -1


def bq_load_from_gcs(jobConfig, gcsfile, table_id):

    try:

        client = ''

        if 'liveproving' in BUCKET_NAME:
            sa_key = get_sa_key()
            client = (
                bigquery.Client
                .from_service_account_info(sa_key)
            )
        else:
            client = bigquery.Client()

        load_job = client.load_table_from_uri(
            gcsfile,
            table_id,
            job_config=jobConfig
        )

        load_job.result()

        print(
            f"Loaded {gcsfile} "
            f"to {table_id} :- "
            f"{load_job.output_rows}"
        )

        return 20

    except Exception as e:

        print(
            f"load Job Failed with Error:-{e}"
        )

        return -1


# =============================================================================
# NEW FUNCTION: Base64 Decode from GCS
# =============================================================================

def decode_base64_gcs_file(
    bucket_name,
    source_gcs_path,
    decoded_folder
):
    """
    Reads a Base64-encoded file from GCS,
    decodes it, and writes the decoded CSV
    content to the decoded_folder path
    within the same bucket.

    Supports:
    - Standard Base64 (RFC 4648)
    - URL-safe Base64 fallback (altchars -_)
    - Large files via streaming bytes

    Args:
        bucket_name     : GCS bucket name
        source_gcs_path : blob path of encoded file
                          (no gs:// prefix)
        decoded_folder  : GCS folder path for output
                          (no gs:// prefix)

    Returns:
        decoded_blob_path (str) on success
        -1 on failure
    """

    global DECODED_FILE_CREATED

    try:

        # Build storage client (liveproving-aware)
        if 'liveproving' in bucket_name:
            sa_key = get_sa_key()
            storage_client = (
                storage.Client
                .from_service_account_info(sa_key)
            )
        else:
            storage_client = storage.Client()

        bucket = storage_client.bucket(bucket_name)

        source_blob = bucket.blob(source_gcs_path)

        # Verify source file exists before proceeding
        if not source_blob.exists():
            print(
                f"Source encoded file not found "
                f"in GCS: {source_gcs_path}"
            )
            return -1

        print(
            f"Reading Base64 encoded file "
            f"from: gs://{bucket_name}/{source_gcs_path}"
        )

        # Download encoded bytes
        encoded_bytes = source_blob.download_as_bytes()

        # Strip surrounding whitespace or newlines
        # that may exist in file-level Base64 content
        encoded_bytes = encoded_bytes.strip()

        # Attempt standard Base64 decode first
        try:
            decoded_bytes = base64.b64decode(
                encoded_bytes,
                validate=True
            )
            print("Base64 standard decode successful.")

        except Exception as decode_err:
            # Fallback: URL-safe Base64 (altchars -_)
            print(
                f"Standard Base64 decode failed: "
                f"{decode_err}. "
                f"Attempting URL-safe Base64..."
            )
            try:
                decoded_bytes = base64.b64decode(
                    encoded_bytes,
                    altchars=b'-_'
                )
                print(
                    "URL-safe Base64 decode successful."
                )
            except Exception as fallback_err:
                print(
                    f"URL-safe Base64 decode also failed: "
                    f"{fallback_err}"
                )
                return -1

        # Derive decoded output blob path
        # e.g. input/path/file.csv
        #   -> TGT_PATH/decoded/file.csv
        file_name_only = os.path.basename(
            source_gcs_path
        )

        decoded_blob_path = (
            f"{decoded_folder}/{file_name_only}"
        )

        print(
            f"Writing decoded file to: "
            f"gs://{bucket_name}/{decoded_blob_path}"
        )

        # Upload decoded CSV content to GCS
        destination_blob = bucket.blob(decoded_blob_path)

        destination_blob.upload_from_string(
            decoded_bytes,
            content_type='text/csv'
        )

        DECODED_FILE_CREATED = True

        print(
            f"Base64 decode step completed. "
            f"Decoded file stored at: "
            f"gs://{bucket_name}/{decoded_blob_path}"
        )

        return decoded_blob_path

    except Exception as e:

        print(
            f"Unexpected error in "
            f"decode_base64_gcs_file: {e}"
        )

        return -1


# =============================================================================
# VALIDATION STEPS
# =============================================================================

def validation_1():

    global STEP

    STEP = 'V1'

    print('Validation 1 Running...')

    print(f'BATCH_DATASET={BATCH_DATASET}')

    validation_query = f"""

    SELECT MASTER_BATCH_CONDITION

    FROM {BATCH_DATASET}.BATCH_RUN_CONTROL

    """

    print("Master Batch SQL running")

    master_batch = execute_query(validation_query)

    first_row = next(master_batch)

    print(
        f"Result from validation step: "
        f"{first_row['MASTER_BATCH_CONDITION']}"
    )

    if master_batch == -1:

        print(
            f"Aborting script as "
            f"master batch query run fails"
        )

        exit(
            f"Aborting script as "
            f"master batch query run fails"
        )

    else:

        print(
            f"bq query executed successfully.. "
            f"master_batch_position: "
            f"{first_row['MASTER_BATCH_CONDITION']}"
        )

        if first_row['MASTER_BATCH_CONDITION'] == 'Y':

            print(
                "Individual table batch "
                "condition is Y continuing"
            )

            return "success"

        else:

            print(
                "Aborting script as "
                "individual table batch "
                "condition is currently disabled"
            )

            exit(
                "Error Code:- 111 \n",
                "Aborting script as "
                "individual table batch "
                "condition is currently disabled"
            )


def validation_2():

    global STEP

    STEP = 'V2'

    print('Validation 2 Running...')

    validation_query = f"""

    SELECT TABLE_BATCH_CONDITION

    FROM {BATCH_DATASET}.STAGING_TABLE_CONTROL

    WHERE
        DATASET_NAME = '{STAGING_DATASET}'
        AND TABLE_NAME = '{TABLE}'

    """

    print("Table Batch SQL running")

    print(f"SQL_TO_RUN:'{validation_query}'")

    table_batch = execute_query(validation_query)

    first_row = next(table_batch)

    print(f"Result from validation step: {table_batch}")

    if table_batch == -1:

        print(
            f"Aborting script as "
            f"table batch query run fails"
        )

        exit(
            f"Aborting script as "
            f"table batch query run fails"
        )

    else:

        print(
            f"bq query executed successfully.. "
            f"table_batch_position: "
            f"{first_row['TABLE_BATCH_CONDITION']}"
        )

        if first_row['TABLE_BATCH_CONDITION'] == 'Y':

            print(
                "Individual table batch "
                "condition is Y continuing"
            )

            return "success"

        else:

            print(
                "Aborting script as "
                "individual table batch "
                "condition is currently disabled"
            )

            exit(
                "Error Code:- 111 \n",
                "Aborting script as "
                "individual table batch "
                "condition is currently disabled"
            )


# =============================================================================
# PROCESSING STEPS 0 - 12
# =============================================================================

def step_0():

    global STEP

    STEP = '0'

    print("STEP 0 Running... ")

    update_staging_table_load_history_step_0 = f"""

    UPDATE {BATCH_DATASET}.STAGING_TABLE_LOAD_HISTORY

    SET BATCH_STATUS = 'INCOMPLETE'

    WHERE
        DATASET_NAME = '{STAGING_DATASET}'
        AND TABLE_NAME = '{TABLE}'
        AND BATCH_STATUS = 'RUNNING'

    """

    data = execute_query(
        update_staging_table_load_history_step_0
    )

    if data == -1:

        print(
            "Failed to execute Step 0 "
            "Aborting job"
        )

        exit(
            "Failed to execute Step 0..."
            "Aborting Job"
        )

    else:

        print("Successfully executed Step 0")


def step_1():

    global STEP

    STEP = '1'

    print("STEP 1 Running... ")

    insert_staing_table_load_history_step_1 = f"""

    INSERT INTO
    {BATCH_DATASET}.STAGING_TABLE_LOAD_HISTORY
    (
        BATCH_PROD_DATE,
        BATCH_PROD_DATE_CH,
        DATASET_NAME,
        TABLE_NAME,
        BATCH_STATUS,
        BATCH_START_TIME
    )

    VALUES
    (
        '{ODATE}',
        '{ODATECH[2:8]}',
        '{STAGING_DATASET}',
        '{TABLE}',
        'RUNNING',
        CURRENT_TIMESTAMP
    )

    """

    data = execute_query(
        insert_staing_table_load_history_step_1
    )

    if data == -1:

        print(
            "Failed to execute Step 1 "
            "Aborting job"
        )

        exit(
            "Failed to execute Step 1..."
            "Aborting Job"
        )

    else:

        print("Successfully executed Step 1")


# =============================================================================
# NEW STEP 1B: Base64 Decode
# Inserted between step_1 and step_2.
# Only executes when IS_BASE64_ENCODED = True.
# On success: redirects GCSFILE to decoded path.
# On failure: calls exit() to trigger existing
#             retry / abort framework unchanged.
# =============================================================================

def step_1b_decode_base64():
    """
    Base64 decode step inserted between
    step_1 (audit insert) and step_2 (truncate staging).

    Responsibilities:
    - Preserve original encoded file path for
      safe archiving in step_9
    - Decode Base64 content from source GCS path
    - Write decoded CSV to TGT_PATH/decoded/ folder
    - Redirect global GCSFILE to decoded path so
      step_3 loads the correct file to BigQuery

    This step is retry-safe:
    - Re-running decode overwrites the decoded file
      in GCS (idempotent)
    - Original encoded file remains untouched
      until step_9 (after step_12 completes)
    """

    global STEP
    global GCSFILE
    global ORIGINAL_ENCODED_FILE_PATH
    global DECODED_GCS_PATH

    STEP = '1B'

    print(
        "STEP 1B Running... "
        "Base64 Decode starting."
    )

    # Preserve original encoded file GCS path
    # (blob path without gs:// prefix)
    # This is used by step_9 to archive
    # the correct original encoded file
    ORIGINAL_ENCODED_FILE_PATH = (
        f"{TGT_PATH}/{FILE}"
    )

    print(
        f"Original encoded file path preserved: "
        f"{ORIGINAL_ENCODED_FILE_PATH}"
    )

    # Decoded output folder:
    # Same bucket, sibling folder to TGT_PATH
    # e.g. input/feeds/myfeed
    #   -> input/feeds/myfeed/decoded
    decoded_folder = f"{TGT_PATH}/decoded"

    print(
        f"Decoded output folder: "
        f"gs://{BUCKET_NAME}/{decoded_folder}"
    )

    # Execute the decode
    decoded_blob_path = decode_base64_gcs_file(
        bucket_name=BUCKET_NAME,
        source_gcs_path=ORIGINAL_ENCODED_FILE_PATH,
        decoded_folder=decoded_folder
    )

    # Abort via existing exit() framework on failure
    if decoded_blob_path == -1:

        print(
            "Failed to execute Step 1B "
            "(Base64 Decode). Aborting job."
        )

        exit(
            "Failed to execute Step 1B "
            "(Base64 Decode). Aborting job."
        )

    # Redirect GCSFILE so all downstream steps
    # (step_3 load, step_4 reconciliation, etc.)
    # operate against the decoded CSV file
    GCSFILE = (
        f"gs://{BUCKET_NAME}/{decoded_blob_path}"
    )

    DECODED_GCS_PATH = decoded_blob_path

    print(
        f"GCSFILE redirected to decoded path: "
        f"{GCSFILE}"
    )

    print("Successfully executed Step 1B")


def step_2():

    global RETRY
    global STEP

    RETRY = True

    STEP = '2'

    print("STEP 2 Running...")

    delete_staging_step_2 = f"""

    DELETE FROM
    {PROJECT_ID}.{STAGING_DATASET}.{TABLE}

    WHERE TRUE

    """

    data = execute_query(delete_staging_step_2)

    if data == -1:

        print(
            "Failed to execute Step 2.. "
            "Aborting job"
        )

        exit(
            "Failed to execute Step 2..."
            "Aborting Job"
        )

    else:

        print("Successfully executed Step 2")


def step_3():

    global STEP
    global IMAGE_DATE
    global IMAGE_FILE
    global LIST_OF_FILE_MFL
    global LOF
    global RETRY

    GCSFILE_NM = []

    LIST_OF_FILE_MFL = []

    print(f'GCS FILE IS HERE:- {GCSFILE}')

    STEP = '3'

    print("Step 3 running")

    if FILE_FORMAT == "AVRO":

        jobConfig = bigquery.LoadJobConfig(
            source_format=(
                bigquery.SourceFormat.AVRO
            ),
            use_avro_logical_types=True,
            schema_update_options=[
                bigquery.SchemaUpdateOption
                .ALLOW_FIELD_RELAXATION
            ],
        )

    elif FILE_FORMAT == "CSV":

        jobConfig = bigquery.LoadJobConfig(
            source_format=(
                bigquery.SourceFormat.CSV
            ),
            skip_leading_rows=1,
            allow_jagged_rows=True,
            schema_update_options=[
                bigquery.SchemaUpdateOption
                .ALLOW_FIELD_RELAXATION
            ],
        )

    elif FILE_FORMAT == "JSON":

        jobConfig = bigquery.LoadJobConfig(
            source_format=(
                bigquery.SourceFormat
                .NEWLINE_DELIMITED_JSON
            ),
            schema_update_options=[
                bigquery.SchemaUpdateOption
                .ALLOW_FIELD_RELAXATION
            ],
        )

    if LOAD_PATTERN == 'SFL':

        # GCSFILE already points to decoded path
        # if IS_BASE64_ENCODED = True (set in step_1b)
        run_data = bq_load_from_gcs(
            jobConfig,
            GCSFILE,
            f"{STAGING_DATASET}.{TABLE}"
        )

        if run_data == -1:

            print(
                "Failed to execute Step 3..."
                " Aborting Job"
            )

            exit(
                "Failed to execute Step 3..."
                " Aborting Job"
            )

        else:

            print("Successfully Executed Step 3..")

    if LOAD_PATTERN == 'MFL':

        LOF = list_files_with_prefix(
            FILE_PREFIX,
            BUCKET_NAME,
            TGT_PATH,
            FILE_FORMAT
        )

        for fl in LOF:

            fl_nm = fl.name

            fl_nm = fl_nm.split('/')

            fl_nm = fl_nm[len(fl_nm)-1]

            GCSFILE_NM.append(
                create_gcs_file(
                    TABLE,
                    BUCKET_NAME,
                    TGT_PATH,
                    fl_nm,
                    LOAD_PATTERN,
                    PATTERN,
                    FILE_FORMAT
                )
            )

            LIST_OF_FILE_MFL.append(fl_nm)

        # For MFL + Base64: GCSFILE_NM entries
        # point to TGT_PATH (original location).
        # step_1b has already redirected GCSFILE
        # for MFL first-file reference.
        # Full MFL Base64 support: each file in
        # LOF would need individual decode;
        # GCSFILE_NM is rebuilt here from TGT_PATH.
        # If MFL files are ALL Base64 encoded,
        # extend step_1b to loop and rebuild
        # GCSFILE_NM with decoded paths.
        print(
            f"List of Files to be uploaded "
            f"to bigquery is here:- "
            f"{GCSFILE_NM}"
        )

        run_data = bq_load_from_gcs(
            jobConfig,
            GCSFILE_NM,
            f"{STAGING_DATASET}.{TABLE}"
        )

        if run_data == -1:

            print(
                "Failed to execute Step 3..."
                " Aborting Job"
            )

            exit(
                "Failed to execute Step 3..."
                " Aborting Job"
            )

        else:

            print(
                f"Successfully Executed "
                f"MFL Step 3.."
            )

    print(f"image_date = {IMAGE_DATE}")

    if IMAGE_DATE != "NA":

        if IMAGE_DATE == "IMAGEDATE":
            IMAGE_DATE = (
                os.path.basename(GCSFILE)
                .split('_')[-1]
                .split('.')[0]
            )

        print(f"image_date = {IMAGE_DATE}")

        update_staging_image_date_step_3 = f"""

        UPDATE {STAGING_DATASET}.{TABLE}

        SET Image_Date =
        CAST('{IMAGE_DATE}' AS STRING)

        WHERE TRUE

        """

        print(
            f"STEP=3 Running... "
            f"SQL_TO_RUN: "
            f"{update_staging_image_date_step_3}"
        )

        execute_data = execute_query(
            update_staging_image_date_step_3
        )

        if execute_data == -1:

            print(
                "Failed to execute "
                "Image date step.. "
                "Aborting job"
            )

            exit(
                "Failed to execute "
                "Image date step.. "
                "Aborting job"
            )

        else:

            print(
                "Successfully executed "
                "Image date Step.."
            )

    if IMAGE_FILE != "NA":

        if IMAGE_FILE == "IMAGEFILE":
            IMAGE_FILE = FILE.split('.')[0]

        print(f"image_file = {IMAGE_FILE}")

        update_staging_image_file_step_3 = f"""

        UPDATE {STAGING_DATASET}.{TABLE}

        SET Image_File =
        CAST('{IMAGE_FILE}' AS STRING)

        WHERE TRUE

        """

        print(
            f"STEP=3 Running... "
            f"SQL_TO_RUN: "
            f"{update_staging_image_file_step_3}"
        )

        execute_data = execute_query(
            update_staging_image_file_step_3
        )

        if execute_data == -1:

            print(
                "Failed to execute "
                "Image file step.. "
                "Aborting job"
            )

            exit(
                "Failed to execute "
                "Image file step.. "
                "Aborting job"
            )

        else:

            print(
                "Successfully executed "
                "Image file Step.."
            )

    print(
        f'NULL_ALERT_CHECK = '
        f'{NULL_ALERT_CHECK}'
    )

    RETRY = False

    if NULL_ALERT_CHECK:

        print('NULL alert running')

        find_null_row_count_in_staging()

    print(
        f'DUPLICATE_ALERT_CHECK = '
        f'{DUPLICATE_ALERT_CHECK}'
    )

    if DUPLICATE_ALERT_CHECK:

        print('DUPLICATE alert running')

        duplicate_check()

    RETRY = True


def step_4():

    global avro_count
    global bq_count
    global STEP
    global RETRY
    global base_dir
    global base_dag_file_name
    global EMPTY_FILE_ALERT_CHECK

    RETRY = True

    STEP = '4'

    avro_count = 0

    print('STEP 4 Running...')

    if FILE_TYPE == "OTHERS":

        print(
            "Loading files as raw, "
            "hence skipping reconciliation"
        )

        print("Successfully executed Step 4..")

    else:

        print("Doing BQ Reconciliation")

        bq_count = execute_query(
            f"""
            SELECT COUNT(*)
            FROM {STAGING_DATASET}.{TABLE}
            """
        )

        bq_count = next(bq_count)

        bq_count = bq_count[0]

        print(f"Staging Table Count: {bq_count}")

        avro_count = bq_count

        if avro_count == 0:

            context = {
                'params': {
                    'msg_temp': (
                        f'Zero row count found '
                        f'in file {GCSFILE}'
                    )
                }
            }

            config_name = (
                base_dag_file_name
                .split('.py')[0]
                .split('_TIGER')[0]
            )

            config_name = (
                f'{config_name}_config.json'
            )

            message_outpt = (
                f'Zero row count found '
                f'in file {GCSFILE}'
            )

            log_data = {
                "severity": "INFO",
                "Message": message_outpt,
                "task_name": f'{FILE}_POST_PROCESSING',
                "task_type": "airflow"
            }

            json_log = json.dumps(log_data, indent=2)

            gcp_log_client = (
                google.cloud.logging.Client()
            )

            logger = gcp_log_client.logger(
                "Generic_Alert"
            )

            with open(
                f'{base_dir}/{config_name}'
            ) as f:
                config = json.load(f)

            EMPTY_FILE_ALERT_CHECK = config.get(
                "empty_file_alert",
                True
            )

            if EMPTY_FILE_ALERT_CHECK:

                logger.log_struct(log_data)

                print(message_outpt)

                task_failure_alert(context)

            else:

                print(message_outpt)

                logger.log_struct(log_data)

        if avro_count == -1:

            print(
                "Error while counting "
                "rows of avro file"
            )

            exit(
                "Error while counting "
                "rows of avro file"
            )

        if avro_count == bq_count:

            print(
                f"Extraction-BQ "
                f"Reconciliation successful. "
                f"BQ count: {bq_count}, "
                f"AVRO count: {avro_count}"
            )

            print("Successfully executed Step 4..")

        else:

            print(
                f"BQ Reconciliation failed, "
                f"Staging BQ count: "
                f"{bq_count}, "
                f"Extraction Avro count: "
                f"{avro_count}"
            )

            exit(
                'Error Code: 101 \n',
                f"BQ Reconciliation failed, "
                f"Staging BQ count: "
                f"{bq_count}, "
                f"Extraction Avro count: "
                f"{avro_count}"
            )


def step_5():

    global STEP

    STEP = '5'

    print("Step 5 running")

    step_5_query = ''

    ret_data = ''

    if IMAGE_BATCH_DATE == 'IMAGEBATCHDATE':

        print(
            "Updating Image batch date "
            "in batch control table"
        )

        step_5_query = f"""

        UPDATE
        {BATCH_DATASET}.STAGING_TABLE_LOAD_HISTORY

        SET

            EXPECTED_STAGING_ROW_COUNT =
            {avro_count},

            ACTUAL_STAGING_ROW_COUNT =
            (
                SELECT SUM(1)
                FROM {STAGING_DATASET}.{TABLE}
            ),

            PROJECT_ROW_COUNT_PRE_UPDATE =
            (
                SELECT SUM(1)
                FROM {TARGET_DATASET}.{TABLE}
            ),

            IMAGE_DATE =
            CAST(
                CAST('{IMAGE_DATE}' AS STRING)
                AS DATE FORMAT 'YYYYMMDD'
            )

        WHERE
            BATCH_PROD_DATE = '{ODATE}'
            AND DATASET_NAME = '{STAGING_DATASET}'
            AND TABLE_NAME = '{TABLE}'
            AND BATCH_STATUS = 'RUNNING'

        """

        execute_query(step_5_query)

    else:

        step_5_query = f"""

        UPDATE
        {BATCH_DATASET}.STAGING_TABLE_LOAD_HISTORY

        SET

            EXPECTED_STAGING_ROW_COUNT =
            {avro_count},

            ACTUAL_STAGING_ROW_COUNT =
            (
                SELECT SUM(1)
                FROM {STAGING_DATASET}.{TABLE}
            ),

            PROJECT_ROW_COUNT_PRE_UPDATE =
            (
                SELECT SUM(1)
                FROM {TARGET_DATASET}.{TABLE}
            )

        WHERE
            BATCH_PROD_DATE = '{ODATE}'
            AND DATASET_NAME = '{STAGING_DATASET}'
            AND TABLE_NAME = '{TABLE}'
            AND BATCH_STATUS = 'RUNNING'

        """

        ret_data = execute_query(step_5_query)

    if ret_data == -1:

        print(
            f"Failed to execute "
            f"Step 5.. Aborting job"
        )

        exit(
            f"Failed to execute "
            f"Step 5.. Aborting job"
        )

    else:

        print(
            f"Successfully executed "
            f"Step 5.."
        )


def step_6():

    global STEP
    global RETRY
    global STEP6_COMPLETED

    STEP = '6'

    if not os.path.isfile(SQL_FILENAME):

        print(
            f"SQL file does not exists, "
            f"please check and place "
            f"it again.. {SQL_FILENAME}"
        )

        exit(
            'Error Code:- 111 \n',
            f"SQL file does not exists, "
            f"please check and place "
            f"it again.. {SQL_FILENAME}"
        )

    else:

        print(f"Found SQL File {SQL_FILENAME}")

    print("step 6 running")
    print(TARGET_DATASET)
    print(STAGING_DATASET)
    print(BATCH_DATASET)
    print(STAGING_ARCHIVE_DATASET)
    print(AUTH_VIEWS_DATASET)

    SQL_COMMAND = ''

    with open(SQL_FILENAME, 'r') as file:
        SQL_COMMAND = file.read()

    SQL_COMMAND = SQL_COMMAND.replace(
        '${TARGET_DATASET}',
        TARGET_DATASET
    )

    SQL_COMMAND = SQL_COMMAND.replace(
        '${STAGING_DATASET}',
        STAGING_DATASET
    )

    SQL_COMMAND = SQL_COMMAND.replace(
        '${BATCH_DATASET}',
        BATCH_DATASET
    )

    SQL_COMMAND = SQL_COMMAND.replace(
        '${STAGING_ARCHIVE_DATASET}',
        STAGING_ARCHIVE_DATASET
    )

    SQL_COMMAND = SQL_COMMAND.replace(
        '${AUTH_VIEWS_DATASET}',
        AUTH_VIEWS_DATASET
    )

    print(
        f"STEP=6 Running... "
        f"Final SQL_TO_RUN: "
        f"{SQL_COMMAND}"
    )

    step_6_result = execute_query(SQL_COMMAND)

    if step_6_result == -1:

        print(
            f"Failed to execute "
            f"Step 6.. Aborting job"
        )

        exit(
            f"Failed to execute "
            f"Step 6.. Aborting job"
        )

    else:

        print(
            f'TAGGING_ALERT_CHECK = '
            f'{TAGGING_ALERT_CHECK}'
        )

        RETRY = False

        if TAGGING_ALERT_CHECK:

            print('Tagging alert running')

            tagging_fields_null_check()

        RETRY = True

        STEP6_COMPLETED = True

        print("Successfully executed Step 6..")


def step_7():

    global STEP

    STEP = '7'

    print("STEP 7 Running... ")

    update_staing_table_step_7 = f"""

    UPDATE
    {BATCH_DATASET}.STAGING_TABLE_LOAD_HISTORY

    SET

        STAGING_RECORD_DIFFERENCE =
        EXPECTED_STAGING_ROW_COUNT
        -
        ACTUAL_STAGING_ROW_COUNT,

        PROJECT_ROW_COUNT_POST_UPDATE =
        (
            SELECT SUM(1)
            FROM {TARGET_DATASET}.{TABLE}
        )

    WHERE
        BATCH_PROD_DATE = '{ODATE}'
        AND DATASET_NAME = '{STAGING_DATASET}'
        AND TABLE_NAME = '{TABLE}'
        AND BATCH_STATUS = 'RUNNING'

    """

    data = execute_query(update_staing_table_step_7)

    if data == -1:

        print(
            "Failed to execute "
            "Step 7.. Aborting job"
        )

        exit(
            "Failed to execute "
            "Step 7 Aborting job"
        )

    else:

        print("Successfully executed Step 7")


def step_8():

    global STEP

    STEP = '8'

    print("STEP 8 Running... ")

    update_staing_table_step_8 = f"""

    UPDATE
    {BATCH_DATASET}.STAGING_TABLE_LOAD_HISTORY

    SET

        PROJECT_ROW_COUNT_DIFFERENCE =
        PROJECT_ROW_COUNT_POST_UPDATE
        -
        PROJECT_ROW_COUNT_PRE_UPDATE

    WHERE
        BATCH_PROD_DATE = '{ODATE}'
        AND DATASET_NAME = '{STAGING_DATASET}'
        AND TABLE_NAME = '{TABLE}'
        AND BATCH_STATUS = 'RUNNING'

    """

    data = execute_query(update_staing_table_step_8)

    if data == -1:

        print(
            "Failed to execute "
            "Step 8... Aborting job"
        )

        exit(
            "Failed to execute "
            "Step 8 Aborting job"
        )

    else:

        print("Successfully executed Step 8")


def step_9():
    """
    Archives processed file(s) to GCS archive folder.

    CHANGE (Base64 support):
    - SFL mode: when IS_BASE64_ENCODED = True,
      archives the ORIGINAL encoded file
      (ORIGINAL_ENCODED_FILE_PATH) instead of
      the decoded file (GCSFILE).
      The decoded file is an intermediate artifact
      and is intentionally NOT archived.
    - MFL mode: always archives from TGT_PATH/fl
      (original location). Decoded files if any
      are intermediate and not archived.
    - All other behaviour unchanged.
    """

    global STEP
    global FILES_ARCHIVED
    global LIST_OF_FILE_MFL

    STEP = '9'

    if LOAD_PATTERN == 'SFL':

        # ---------------------------------------------------------
        # CHANGE: Route archive source based on IS_BASE64_ENCODED
        # If Base64 mode is active, the original encoded file must
        # be archived (not the decoded intermediate file).
        # ORIGINAL_ENCODED_FILE_PATH was captured in step_1b
        # before GCSFILE was redirected to the decoded path.
        # ---------------------------------------------------------
        if IS_BASE64_ENCODED and ORIGINAL_ENCODED_FILE_PATH:

            source_path_to_archive = (
                ORIGINAL_ENCODED_FILE_PATH
            )

            print(
                f"STEP 9 Running (Base64 mode)... "
                f"Archiving original encoded file: "
                f"{source_path_to_archive}"
            )

        else:

            source_path_to_archive = (
                f'{TGT_PATH}/{FILE}'
            )

            print(
                f"STEP 9 Running... "
                f"Archiving file: "
                f"{source_path_to_archive} started.."
            )
        # ---------------------------------------------------------
        # END CHANGE
        # ---------------------------------------------------------

        mv_res = move_gcs_to_archive(
            source_path_to_archive,
            GCSARCHIVEFILE,
            BUCKET_NAME
        )

        if mv_res == -1:

            print(
                f"Failed to execute "
                f"Step {STEP}.. "
                f"Aborting job"
            )

            exit(
                f"Failed to execute "
                f"Step {STEP}.. "
                f"Aborting job"
            )

        else:

            FILES_ARCHIVED = True

            print("Successfully executed Step 9..")

    if LOAD_PATTERN == 'MFL':

        for fl in LIST_OF_FILE_MFL:

            print(
                f"STEP 9 Running... "
                f"Archiving file.. "
                f"{fl} started.."
            )

            GCSARCHIVEFILE_MFL = (
                f'{TGT_PATH}/archive/'
                f'{ODATECH}/{fl}'
            )

            # MFL always archives from TGT_PATH/fl
            # (original source location).
            # Decoded files are intermediate only.
            mv_res = move_gcs_to_archive(
                f'{TGT_PATH}/{fl}',
                GCSARCHIVEFILE_MFL,
                BUCKET_NAME
            )

            if mv_res == -1:

                print(
                    f"Failed to execute "
                    f"Step {STEP}.. "
                    f"Aborting job"
                )

                exit(
                    f"Failed to execute "
                    f"Step {STEP}.. "
                    f"Aborting job"
                )

            else:

                FILES_ARCHIVED = True

                print(
                    "Successfully executed "
                    "Step 9.."
                )


def step_10():

    global STEP

    STEP = '10'

    print("STEP 10 Running... ")

    update_staing_table_step_10 = f"""

    UPDATE
    {BATCH_DATASET}.STAGING_TABLE_LOAD_HISTORY

    SET

        BATCH_END_TIME =
        CURRENT_TIMESTAMP,

        BATCH_SECONDS_ELAPSED =
        TIMESTAMP_DIFF(
            CURRENT_TIMESTAMP,
            BATCH_START_TIME,
            SECOND
        )

    WHERE
        BATCH_PROD_DATE = '{ODATE}'
        AND DATASET_NAME = '{STAGING_DATASET}'
        AND TABLE_NAME = '{TABLE}'
        AND BATCH_STATUS = 'RUNNING'

    """

    data = execute_query(update_staing_table_step_10)

    if data == -1:

        print(
            "Failed to execute "
            "Step 10 Aborting job"
        )

        exit(
            "Failed to execute "
            "Step 10 Aborting job"
        )

    else:

        print("Successfully executed Step 10")


def step_11():

    global STEP
    global CONTROL_TABLE_UPDATE
    global FILE_PREFIX
    global LOAD_PATTERN

    STEP = '11'

    lof = list_files_with_prefix(
        FILE_PREFIX,
        BUCKET_NAME,
        TGT_PATH,
        FILE_FORMAT
    )

    nof = len(lof)

    print("STEP 11 Running... ")

    update_staing_table_step_11 = f"""

    UPDATE
    {BATCH_DATASET}.STAGING_TABLE_CONTROL c

    SET

        LAST_BATCH_DATE =
        BATCH_PROD_DATE,

        LAST_BATCH_START_TIME =
        BATCH_START_TIME,

        LAST_BATCH_END_TIME =
        BATCH_END_TIME,

        NEXT_BATCH_DATE =
        CASE BATCH_FREQUENCY

            WHEN 'DLY' THEN PROD_DLY
            WHEN 'DLW' THEN PROD_DLW
            WHEN 'LCD' THEN PROD_LCD
            WHEN 'LWD' THEN PROD_LWD
            WHEN 'FCD' THEN PROD_FCD
            WHEN 'FWD' THEN PROD_FWD
            WHEN 'D16' THEN PROD_D16
            WHEN 'MON' THEN PROD_MON
            WHEN 'TUE' THEN PROD_TUE
            WHEN 'WED' THEN PROD_WED
            WHEN 'THU' THEN PROD_THU
            WHEN 'FRI' THEN PROD_FRI
            WHEN 'SAT' THEN PROD_SAT
            WHEN 'SUN' THEN PROD_SUN

        END

    FROM
    {BATCH_DATASET}.STAGING_TABLE_LOAD_HISTORY h

    INNER JOIN
    {BATCH_DATASET}.BATCH_DAY_CONTROL b

    ON h.BATCH_PROD_DATE = b.PROD_DATE

    WHERE

        h.BATCH_PROD_DATE = '{ODATE}'

        AND h.DATASET_NAME =
        '{STAGING_DATASET}'

        AND h.TABLE_NAME =
        '{TABLE}'

        AND h.BATCH_STATUS =
        'RUNNING'

        AND c.DATASET_NAME =
        '{STAGING_DATASET}'

        AND c.TABLE_NAME =
        '{TABLE}'

    """

    if nof > 0 and LOAD_PATTERN == 'MFL':

        print(
            f'Control Table update skipped '
            f'as {nof} more files found '
            f'to upload and load pattern '
            f'is MFL'
        )

    else:

        data = execute_query(
            update_staing_table_step_11
        )

        if data == -1:

            print(
                "Failed to execute "
                "Step 11 Aborting job"
            )

            exit(
                "Failed to execute "
                "Step 11.. Aborting job"
            )

        else:

            print("Successfully executed Step 11")


def step_12():

    global STEP
    global RETRY
    global CONTROL_TABLE_UPDATE

    STEP = '12'

    print("STEP 12 Running... ")

    update_staging_table_step_12 = f"""

    UPDATE
    {BATCH_DATASET}.STAGING_TABLE_LOAD_HISTORY

    SET
        BATCH_STATUS = 'COMPLETE'

    WHERE
        BATCH_PROD_DATE = '{ODATE}'
        AND DATASET_NAME = '{STAGING_DATASET}'
        AND TABLE_NAME = '{TABLE}'
        AND BATCH_STATUS = 'RUNNING'

    """

    data = execute_query(update_staging_table_step_12)

    if data == -1:

        print(
            "Failed to execute "
            "Step 12 Aborting job"
        )

        exit(
            "Failed to execute "
            "Step 12 Aborting job"
        )

    else:

        CONTROL_TABLE_UPDATE = True

        RETRY = False

        print("Successfully executed Step 12")


# =============================================================================
# HELPER: Inline EXIT guard block
# Reused identically after every step call in main()
# =============================================================================

def _check_exit_and_return():
    """
    Internal helper that mirrors the inline EXIT guard
    pattern used throughout main(). Not called directly
    — guard blocks are kept inline in main() to preserve
    the original structural pattern.
    """
    pass


# =============================================================================
# MAIN ORCHESTRATION FUNCTION
# =============================================================================

def main(DATASET, TABLE, file_p):

    if check_holiday():
        print("Happy Holiday, Feed Skipped!...")
        return True

    global ALERT_CREATED
    global EXIT
    global TOTAL_RETRIES
    global RETRY_INTERVAL

    # --- NEW: Base64 globals used in main ---
    global IS_BASE64_ENCODED
    global DECODED_GCS_PATH
    global ORIGINAL_ENCODED_FILE_PATH
    global DECODED_FILE_CREATED
    # --- END NEW ---

    try:

        global ODATE
        global SQL_FILENAME
        global STAGING_DATASET
        global TARGET_DATASET
        global BATCH_DATASET
        global AUTH_VIEWS_DATASET
        global STAGING_ARCHIVE_DATASET
        global ODATECH
        global GCSFILE
        global PROJECT_ID
        global STEP
        global GCSARCHIVEFILE
        global FILE
        global CONTROL_TABLE_UPDATE
        global FILE_PREFIX
        global ORIGINAL_IMAGE_DATE
        global ORIGINAL_IMAGE_FILE
        global ORIGINAL_IMAGE_BATCH_DATE
        global ID_CHK
        global IF_CHK
        global IBD_CHK
        global RETRY
        global RETRY_COUNT
        global STEP6_COMPLETED
        global FILES_ARCHIVED

        file_prefix = file_p

        FILE_PREFIX = file_p

        # -----------------------------------------------------------------
        # NEW: Read IS_BASE64_ENCODED flag from config JSON.
        # Reads once per main() invocation.
        # Defaults to False if key absent or config not found,
        # ensuring zero impact on feeds that do not set the flag.
        # -----------------------------------------------------------------
        config_name = (
            base_dag_file_name
            .split('.py')[0]
            .split('_TIGER')[0]
        )

        config_name = f'{config_name}_config.json'

        config_path = f'{base_dir}/{config_name}'

        if os.path.isfile(config_path):

            with open(config_path) as cfg_f:
                feed_config = json.load(cfg_f)

            IS_BASE64_ENCODED = feed_config.get(
                "is_base64_encoded",
                False
            )

        else:

            IS_BASE64_ENCODED = False

        print(
            f"IS_BASE64_ENCODED = "
            f"{IS_BASE64_ENCODED}"
        )

        # Reset decoded path state for this run
        ORIGINAL_ENCODED_FILE_PATH = ""
        DECODED_GCS_PATH = ""
        DECODED_FILE_CREATED = False
        # -----------------------------------------------------------------
        # END NEW
        # -----------------------------------------------------------------

        lof = list_files_with_prefix(
            file_prefix,
            BUCKET_NAME,
            TGT_PATH,
            FILE_FORMAT
        )

        print(f'No. of files found = {len(lof)}')

        nof = len(lof)

        if nof == 0:

            if (
                CONTROL_TABLE_UPDATE == False
                and
                FILES_ARCHIVED == True
            ):

                if EXIT:
                    if (
                        RETRY_COUNT >= TOTAL_RETRIES
                        and
                        CONTROL_TABLE_UPDATE == False
                    ):
                        sys.exit(2)
                    else:
                        return True

                step_10()

                if EXIT:
                    if (
                        RETRY_COUNT >= TOTAL_RETRIES
                        and
                        CONTROL_TABLE_UPDATE == False
                    ):
                        sys.exit(2)
                    else:
                        return True

                step_11()

                if EXIT:
                    if (
                        RETRY_COUNT >= TOTAL_RETRIES
                        and
                        CONTROL_TABLE_UPDATE == False
                    ):
                        sys.exit(2)
                    else:
                        return True

                step_12()

                if EXIT:
                    if (
                        RETRY_COUNT >= TOTAL_RETRIES
                        and
                        CONTROL_TABLE_UPDATE == False
                    ):
                        sys.exit(2)
                    else:
                        return True

                return True

            else:

                print("No files Found")
                return True

        else:

            if (
                CONTROL_TABLE_UPDATE == False
                and
                FILES_ARCHIVED == True
            ):

                if EXIT:
                    if (
                        RETRY_COUNT >= TOTAL_RETRIES
                        and
                        CONTROL_TABLE_UPDATE == False
                    ):
                        sys.exit(2)
                    else:
                        return True

                step_10()

                if EXIT:
                    if (
                        RETRY_COUNT >= TOTAL_RETRIES
                        and
                        CONTROL_TABLE_UPDATE == False
                    ):
                        sys.exit(2)
                    else:
                        return True

                step_11()

                if EXIT:
                    if (
                        RETRY_COUNT >= TOTAL_RETRIES
                        and
                        CONTROL_TABLE_UPDATE == False
                    ):
                        sys.exit(2)
                    else:
                        return True

                step_12()

                if EXIT:
                    if (
                        RETRY_COUNT >= TOTAL_RETRIES
                        and
                        CONTROL_TABLE_UPDATE == False
                    ):
                        sys.exit(2)
                    else:
                        return True

            elif (
                CONTROL_TABLE_UPDATE == False
                and
                FILES_ARCHIVED == False
                and
                STEP6_COMPLETED == True
            ):

                if EXIT:
                    if (
                        RETRY_COUNT >= TOTAL_RETRIES
                        and
                        CONTROL_TABLE_UPDATE == False
                    ):
                        sys.exit(2)
                    else:
                        return True

                step_7()

                if EXIT:
                    if (
                        RETRY_COUNT >= TOTAL_RETRIES
                        and
                        CONTROL_TABLE_UPDATE == False
                    ):
                        sys.exit(2)
                    else:
                        return True

                step_8()

                if EXIT:
                    if (
                        RETRY_COUNT >= TOTAL_RETRIES
                        and
                        CONTROL_TABLE_UPDATE == False
                    ):
                        sys.exit(2)
                    else:
                        return True

                step_9()

                if EXIT:
                    if (
                        RETRY_COUNT >= TOTAL_RETRIES
                        and
                        CONTROL_TABLE_UPDATE == False
                    ):
                        sys.exit(2)
                    else:
                        return True

                step_10()

                if EXIT:
                    if (
                        RETRY_COUNT >= TOTAL_RETRIES
                        and
                        CONTROL_TABLE_UPDATE == False
                    ):
                        sys.exit(2)
                    else:
                        return True

                step_11()

                if EXIT:
                    if (
                        RETRY_COUNT >= TOTAL_RETRIES
                        and
                        CONTROL_TABLE_UPDATE == False
                    ):
                        sys.exit(2)
                    else:
                        return True

                step_12()

                if EXIT:
                    if (
                        RETRY_COUNT >= TOTAL_RETRIES
                        and
                        CONTROL_TABLE_UPDATE == False
                    ):
                        sys.exit(2)
                    else:
                        return True

            while True:

                # =============================================================
                # SFL: Single File Load
                # =============================================================

                if LOAD_PATTERN == 'SFL':

                    print('LOAD Pattern = SFL')

                    lof = list_files_with_prefix(
                        file_prefix,
                        BUCKET_NAME,
                        TGT_PATH,
                        FILE_FORMAT
                    )

                    print(
                        f'No. of files found = '
                        f'{len(lof)}'
                    )

                    nof = len(lof)

                    for fl in lof:

                        print(
                            f'file list file is '
                            f'here:- {fl}'
                        )

                        print(
                            f'file list file name '
                            f'is here:- {fl.name}'
                        )

                        CONTROL_TABLE_UPDATE = False
                        FILES_ARCHIVED = False
                        STEP6_COMPLETED = False

                        # Reset Base64 state per file
                        ORIGINAL_ENCODED_FILE_PATH = ""
                        DECODED_GCS_PATH = ""
                        DECODED_FILE_CREATED = False

                        FILE = fl.name
                        FILE = FILE.split('/')
                        FILE = FILE[len(FILE)-1]

                        print(
                            f'Dataset = {DATASET}, '
                            f'TABLE = {TABLE}, '
                            f'FILE = {FILE}'
                        )

                        DATASET = DATASET
                        TABLE = TABLE

                        ODATE = datetime.now().strftime(
                            '%Y-%m-%d'
                        )

                        SQL_FILENAME = glob.glob(
                            f'{base_dir}/*_'
                            f'{TABLE.lower()}_load.sql'
                        )[0]

                        STAGING_DATASET = (
                            f'{DATASET}_STAGING_{ENV}'
                        )

                        TARGET_DATASET = (
                            f'{DATASET}_{ENV}'
                        )

                        BATCH_DATASET = (
                            f'{DATASET}_BATCH_CONTROL_{ENV}'
                        )

                        AUTH_VIEWS_DATASET = (
                            f'{DATASET}_AUTH_VIEWS_{ENV}'
                        )

                        STAGING_ARCHIVE_DATASET = (
                            f'{DATASET}_STAGING_ARCHIVE_{ENV}'
                        )

                        ODATECH = (
                            ODATE[:4]
                            + ODATE[5:7]
                            + ODATE[8:10]
                        )

                        GCSFILE = ''

                        PROJECT_ID = GCP_NAME

                        STEP = 0

                        GCSARCHIVEFILE = (
                            f'{TGT_PATH}/archive/'
                            f'{ODATECH}/{FILE}'
                        )

                        print(f"running for file {FILE}")

                        print(
                            f'BATCH_DATASET='
                            f'{BATCH_DATASET}'
                        )

                        GCSFILE = create_gcs_file(
                            TABLE,
                            BUCKET_NAME,
                            TGT_PATH,
                            FILE,
                            LOAD_PATTERN,
                            PATTERN,
                            FILE_FORMAT
                        )

                        if ID_CHK:
                            ORIGINAL_IMAGE_DATE = IMAGE_DATE
                            ID_CHK = False

                        if IF_CHK:
                            ORIGINAL_IMAGE_FILE = IMAGE_FILE
                            IF_CHK = False

                        if IBD_CHK:
                            ORIGINAL_IMAGE_BATCH_DATE = (
                                IMAGE_BATCH_DATE
                            )
                            IBD_CHK = False

                        update_variables(
                            DAG_NAME,
                            BUCKET_NAME,
                            TGT_PATH,
                            GCP_NAME,
                            FEED_NAME,
                            PATTERN,
                            LOAD_PATTERN,
                            FILE_FORMAT,
                            ENV,
                            ORIGINAL_IMAGE_DATE,
                            ORIGINAL_IMAGE_FILE,
                            FILE_TYPE,
                            ORIGINAL_IMAGE_BATCH_DATE,
                            FILE,
                            DATASET,
                            TABLE,
                            ODATE,
                            SQL_FILENAME,
                            STAGING_DATASET,
                            TARGET_DATASET,
                            BATCH_DATASET,
                            AUTH_VIEWS_DATASET,
                            STAGING_ARCHIVE_DATASET,
                            ODATECH,
                            GCSFILE,
                            PROJECT_ID,
                            GCSARCHIVEFILE,
                            TAGGING_ALERT_CHECK,
                            NULL_ALERT_CHECK,
                            DUPLICATE_ALERT_CHECK,
                            BIGQUERY_LABELS,
                            file_prefix,
                            OWNER
                        )

                        print(f" RETRY: {RETRY}")

                        if not RETRY:

                            if EXIT:
                                if (
                                    RETRY_COUNT >= TOTAL_RETRIES
                                    and
                                    CONTROL_TABLE_UPDATE == False
                                ):
                                    sys.exit(2)
                                else:
                                    return True

                            validation_1()

                            if EXIT:
                                if (
                                    RETRY_COUNT >= TOTAL_RETRIES
                                    and
                                    CONTROL_TABLE_UPDATE == False
                                ):
                                    sys.exit(2)
                                else:
                                    return True

                            validation_2()

                            if EXIT:
                                if (
                                    RETRY_COUNT >= TOTAL_RETRIES
                                    and
                                    CONTROL_TABLE_UPDATE == False
                                ):
                                    sys.exit(2)
                                else:
                                    return True

                            step_0()

                            if EXIT:
                                if (
                                    RETRY_COUNT >= TOTAL_RETRIES
                                    and
                                    CONTROL_TABLE_UPDATE == False
                                ):
                                    sys.exit(2)
                                else:
                                    return True

                            step_1()

                            if EXIT:
                                if (
                                    RETRY_COUNT >= TOTAL_RETRIES
                                    and
                                    CONTROL_TABLE_UPDATE == False
                                ):
                                    sys.exit(2)
                                else:
                                    return True

                            # -----------------------------------------
                            # NEW: Step 1B — Base64 Decode (SFL)
                            # Only executes when IS_BASE64_ENCODED=True.
                            # Redirects GCSFILE to decoded path before
                            # step_2 truncates staging and step_3 loads.
                            # -----------------------------------------
                            if IS_BASE64_ENCODED:

                                step_1b_decode_base64()

                                if EXIT:
                                    if (
                                        RETRY_COUNT >= TOTAL_RETRIES
                                        and
                                        CONTROL_TABLE_UPDATE == False
                                    ):
                                        sys.exit(2)
                                    else:
                                        return True
                            # -----------------------------------------
                            # END NEW: Step 1B (SFL)
                            # -----------------------------------------

                            step_2()

                            if EXIT:
                                if (
                                    RETRY_COUNT >= TOTAL_RETRIES
                                    and
                                    CONTROL_TABLE_UPDATE == False
                                ):
                                    sys.exit(2)
                                else:
                                    return True

                            step_3()

                            if EXIT:
                                if (
                                    RETRY_COUNT >= TOTAL_RETRIES
                                    and
                                    CONTROL_TABLE_UPDATE == False
                                ):
                                    sys.exit(2)
                                else:
                                    return True

                            step_4()

                            if EXIT:
                                if (
                                    RETRY_COUNT >= TOTAL_RETRIES
                                    and
                                    CONTROL_TABLE_UPDATE == False
                                ):
                                    sys.exit(2)
                                else:
                                    return True

                            step_5()

                            if EXIT:
                                if (
                                    RETRY_COUNT >= TOTAL_RETRIES
                                    and
                                    CONTROL_TABLE_UPDATE == False
                                ):
                                    sys.exit(2)
                                else:
                                    return True

                            step_6()

                            if EXIT:
                                if (
                                    RETRY_COUNT >= TOTAL_RETRIES
                                    and
                                    CONTROL_TABLE_UPDATE == False
                                ):
                                    sys.exit(2)
                                else:
                                    return True

                            step_7()

                            if EXIT:
                                if (
                                    RETRY_COUNT >= TOTAL_RETRIES
                                    and
                                    CONTROL_TABLE_UPDATE == False
                                ):
                                    sys.exit(2)
                                else:
                                    return True

                            step_8()

                            if EXIT:
                                if (
                                    RETRY_COUNT >= TOTAL_RETRIES
                                    and
                                    CONTROL_TABLE_UPDATE == False
                                ):
                                    sys.exit(2)
                                else:
                                    return True

                            step_9()

                            if EXIT:
                                if (
                                    RETRY_COUNT >= TOTAL_RETRIES
                                    and
                                    CONTROL_TABLE_UPDATE == False
                                ):
                                    sys.exit(2)
                                else:
                                    return True

                            step_10()

                            if EXIT:
                                if (
                                    RETRY_COUNT >= TOTAL_RETRIES
                                    and
                                    CONTROL_TABLE_UPDATE == False
                                ):
                                    sys.exit(2)
                                else:
                                    return True

                            step_11()

                            if EXIT:
                                if (
                                    RETRY_COUNT >= TOTAL_RETRIES
                                    and
                                    CONTROL_TABLE_UPDATE == False
                                ):
                                    sys.exit(2)
                                else:
                                    return True

                            step_12()

                            if EXIT:
                                if (
                                    RETRY_COUNT >= TOTAL_RETRIES
                                    and
                                    CONTROL_TABLE_UPDATE == False
                                ):
                                    sys.exit(2)
                                else:
                                    return True

                # =============================================================
                # MFL: Multiple File Load
                # =============================================================

                if LOAD_PATTERN == 'MFL':

                    print('LOAD Pattern = MFL')

                    lof = list_files_with_prefix(
                        file_prefix,
                        BUCKET_NAME,
                        TGT_PATH,
                        FILE_FORMAT
                    )

                    print(
                        f'No. of files found = '
                        f'{len(lof)}'
                    )

                    CONTROL_TABLE_UPDATE = False
                    FILES_ARCHIVED = False
                    STEP6_COMPLETED = False

                    # Reset Base64 state per cycle
                    ORIGINAL_ENCODED_FILE_PATH = ""
                    DECODED_GCS_PATH = ""
                    DECODED_FILE_CREATED = False

                    nof = len(lof)

                    DATASET = DATASET
                    TABLE = TABLE

                    FILE = lof[0].name

                    print(
                        f'Dataset = {DATASET}, '
                        f'TABLE = {TABLE}, '
                        f'FILE = {FILE}'
                    )

                    ODATE = datetime.now().strftime(
                        '%Y-%m-%d'
                    )

                    SQL_FILENAME = glob.glob(
                        f'{base_dir}/*_'
                        f'{TABLE.lower()}_load.sql'
                    )[0]

                    STAGING_DATASET = (
                        f'{DATASET}_STAGING_{ENV}'
                    )

                    TARGET_DATASET = (
                        f'{DATASET}_{ENV}'
                    )

                    BATCH_DATASET = (
                        f'{DATASET}_BATCH_CONTROL_{ENV}'
                    )

                    AUTH_VIEWS_DATASET = (
                        f'{DATASET}_AUTH_VIEWS_{ENV}'
                    )

                    STAGING_ARCHIVE_DATASET = (
                        f'{DATASET}_STAGING_ARCHIVE_{ENV}'
                    )

                    ODATECH = (
                        ODATE[:4]
                        + ODATE[5:7]
                        + ODATE[8:10]
                    )

                    GCSFILE = ''

                    PROJECT_ID = GCP_NAME

                    STEP = 0

                    GCSARCHIVEFILE = (
                        f'{TGT_PATH}/archive/'
                        f'{ODATECH}/{FILE}'
                    )

                    print(f"running for file {FILE}")

                    print(
                        f'BATCH_DATASET='
                        f'{BATCH_DATASET}'
                    )

                    GCSFILE = create_gcs_file(
                        TABLE,
                        BUCKET_NAME,
                        TGT_PATH,
                        FILE,
                        LOAD_PATTERN,
                        PATTERN,
                        FILE_FORMAT
                    )

                    update_variables(
                        DAG_NAME,
                        BUCKET_NAME,
                        TGT_PATH,
                        GCP_NAME,
                        FEED_NAME,
                        PATTERN,
                        LOAD_PATTERN,
                        FILE_FORMAT,
                        ENV,
                        IMAGE_DATE,
                        IMAGE_FILE,
                        FILE_TYPE,
                        IMAGE_BATCH_DATE,
                        FILE,
                        DATASET,
                        TABLE,
                        ODATE,
                        SQL_FILENAME,
                        STAGING_DATASET,
                        TARGET_DATASET,
                        BATCH_DATASET,
                        AUTH_VIEWS_DATASET,
                        STAGING_ARCHIVE_DATASET,
                        ODATECH,
                        GCSFILE,
                        PROJECT_ID,
                        GCSARCHIVEFILE,
                        TAGGING_ALERT_CHECK,
                        NULL_ALERT_CHECK,
                        DUPLICATE_ALERT_CHECK,
                        BIGQUERY_LABELS,
                        file_prefix,
                        OWNER
                    )

                    if not RETRY:

                        if EXIT:
                            if (
                                RETRY_COUNT >= TOTAL_RETRIES
                                and
                                CONTROL_TABLE_UPDATE == False
                            ):
                                sys.exit(2)
                            else:
                                return True

                        validation_1()

                        if EXIT:
                            if (
                                RETRY_COUNT >= TOTAL_RETRIES
                                and
                                CONTROL_TABLE_UPDATE == False
                            ):
                                sys.exit(2)
                            else:
                                return True

                        validation_2()

                        if EXIT:
                            if (
                                RETRY_COUNT >= TOTAL_RETRIES
                                and
                                CONTROL_TABLE_UPDATE == False
                            ):
                                sys.exit(2)
                            else:
                                return True

                        step_0()

                        if EXIT:
                            if (
                                RETRY_COUNT >= TOTAL_RETRIES
                                and
                                CONTROL_TABLE_UPDATE == False
                            ):
                                sys.exit(2)
                            else:
                                return True

                        step_1()

                        if EXIT:
                            if (
                                RETRY_COUNT >= TOTAL_RETRIES
                                and
                                CONTROL_TABLE_UPDATE == False
                            ):
                                sys.exit(2)
                            else:
                                return True

                        # -----------------------------------------
                        # NEW: Step 1B — Base64 Decode (MFL)
                        # For MFL, step_1b decodes the first file
                        # reference. step_3 MFL path rebuilds the
                        # full GCSFILE_NM list from TGT_PATH.
                        # If ALL MFL files are Base64 encoded,
                        # the decode loop in step_3 should be
                        # extended to decode each file individually
                        # before building GCSFILE_NM.
                        # -----------------------------------------
                        if IS_BASE64_ENCODED:

                            step_1b_decode_base64()

                            if EXIT:
                                if (
                                    RETRY_COUNT >= TOTAL_RETRIES
                                    and
                                    CONTROL_TABLE_UPDATE == False
                                ):
                                    sys.exit(2)
                                else:
                                    return True
                        # -----------------------------------------
                        # END NEW: Step 1B (MFL)
                        # -----------------------------------------

                        step_2()

                        if EXIT:
                            if (
                                RETRY_COUNT >= TOTAL_RETRIES
                                and
                                CONTROL_TABLE_UPDATE == False
                            ):
                                sys.exit(2)
                            else:
                                return True

                        step_3()

                        if EXIT:
                            if (
                                RETRY_COUNT >= TOTAL_RETRIES
                                and
                                CONTROL_TABLE_UPDATE == False
                            ):
                                sys.exit(2)
                            else:
                                return True

                        step_4()

                        if EXIT:
                            if (
                                RETRY_COUNT >= TOTAL_RETRIES
                                and
                                CONTROL_TABLE_UPDATE == False
                            ):
                                sys.exit(2)
                            else:
                                return True

                        step_5()

                        if EXIT:
                            if (
                                RETRY_COUNT >= TOTAL_RETRIES
                                and
                                CONTROL_TABLE_UPDATE == False
                            ):
                                sys.exit(2)
                            else:
                                return True

                        step_6()

                        if EXIT:
                            if (
                                RETRY_COUNT >= TOTAL_RETRIES
                                and
                                CONTROL_TABLE_UPDATE == False
                            ):
                                sys.exit(2)
                            else:
                                return True

                        step_7()

                        if EXIT:
                            if (
                                RETRY_COUNT >= TOTAL_RETRIES
                                and
                                CONTROL_TABLE_UPDATE == False
                            ):
                                sys.exit(2)
                            else:
                                return True

                        step_8()

                        if EXIT:
                            if (
                                RETRY_COUNT >= TOTAL_RETRIES
                                and
                                CONTROL_TABLE_UPDATE == False
                            ):
                                sys.exit(2)
                            else:
                                return True

                        step_9()

                        if EXIT:
                            if (
                                RETRY_COUNT >= TOTAL_RETRIES
                                and
                                CONTROL_TABLE_UPDATE == False
                            ):
                                sys.exit(2)
                            else:
                                return True

                        step_10()

                        if EXIT:
                            if (
                                RETRY_COUNT >= TOTAL_RETRIES
                                and
                                CONTROL_TABLE_UPDATE == False
                            ):
                                sys.exit(2)
                            else:
                                return True

                        step_11()

                        if EXIT:
                            if (
                                RETRY_COUNT >= TOTAL_RETRIES
                                and
                                CONTROL_TABLE_UPDATE == False
                            ):
                                sys.exit(2)
                            else:
                                return True

                        step_12()

                        if EXIT:
                            if (
                                RETRY_COUNT >= TOTAL_RETRIES
                                and
                                CONTROL_TABLE_UPDATE == False
                            ):
                                sys.exit(2)
                            else:
                                return True

                    print(
                        f'CONTROL_TABLE_UPDATE = '
                        f'{CONTROL_TABLE_UPDATE}'
                    )

                    lof = list_files_with_prefix(
                        file_prefix,
                        BUCKET_NAME,
                        TGT_PATH,
                        FILE_FORMAT
                    )

                    print(
                        f'No. of files found = '
                        f'{len(lof)} '
                        f'After Completion of a cycle'
                    )

                    nof = len(lof)

                    if nof == 0:

                        if CONTROL_TABLE_UPDATE == True:
                            break
                        else:
                            continue

    except Exception as e:

        if not ALERT_CREATED:

            context = {
                'params': {
                    'msg_temp': (
                        f'Error: {e}, '
                        f'task name: '
                        f'{file_prefix}_POST_PROCESSING, '
                        f'STEP = {STEP}'
                    )
                }
            }

            print(f"Error from Main Function:- {e}")

            task_failure_alert(context)

            ALERT_CREATED = True

        raise Exception(context)

    sys.exit(2)


# =============================================================================
# AIRFLOW TASK WRAPPER
# =============================================================================

def call_main(task_id, DATASET, TABLE, file_p, dag):

    PythonOperator(
        task_id=task_id,
        python_callable=main,
        provide_context=True,
        op_args=[DATASET, TABLE, file_p],
        dag=dag,
    )
