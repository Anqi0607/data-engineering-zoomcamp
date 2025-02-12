import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

# Get configuration from environment variables.
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

# Define dataset file names and the URL to download.
# NOTE: The file is in .gz format.
dataset_file = "yellow_tripdata_2021-01.csv.gz"
dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
# After decompression, the CSV file name is as follows.
decompressed_file = dataset_file.replace('.gz', '')  # "yellow_tripdata_2021-01.csv"
# After conversion, the Parquet file name is:
parquet_file = decompressed_file.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


def format_to_parquet(src_file):
    """
    Reads a CSV file and converts it to Parquet format.
    """
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


# NOTE: This task takes around 20 minutes at an upload speed of 800kbps. 
# It will be faster if your internet connection is faster.
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    
    :param bucket: GCS bucket name
    :param object_name: target path & file name in the bucket
    :param local_file: local file path & file name to upload
    :return: None
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way).
with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    # Task 1: Download the compressed dataset file.
    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file}"
    )

    # Task 2: Decompress the downloaded file.
    decompress_task = BashOperator(
        task_id="decompress_csv_task",
        bash_command="gunzip -f {{ params.path }}/{{ params.dataset_file }}",
        params={
            "path": path_to_local_home,
            "dataset_file": dataset_file,
        },
    )

    # Task 3: Convert the decompressed CSV file to Parquet format.
    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{decompressed_file}",
        },
    )

    # TODO: Homework - research and try XCOM to communicate output values between two tasks/operators.
    # Task 4: Upload the Parquet file to Google Cloud Storage.
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )

    # Task 5: Create an external table in BigQuery that references the Parquet file in GCS.
    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
            },
        },
    )

    # Set the task dependencies:
    # 1. Download dataset -> 2. Decompress file -> 3. Convert to Parquet ->
    # 4. Upload Parquet file to GCS -> 5. Create external table in BigQuery.
    download_dataset_task >> decompress_task >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task
