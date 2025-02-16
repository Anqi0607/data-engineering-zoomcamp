import os
import logging

from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
#from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

# Get configuration from environment variables.
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

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
    "start_date": datetime(2019, 1, 1),
    "depends_on_past": False,
    "retries": 1,
}

def download_parquetize_load_gcs(
        dag,
        url_template,
        output_gz_template,
        output_csv_template,
        output_parquet_template,
        output_gcs_template
    ):
    with dag:

        # Task 1: Download the compressed dataset file.
        download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            bash_command=f"curl -sSLf {url_template} > {output_gz_template}"
        )

        # Task 2: Decompress the downloaded file.
        decompress_task = BashOperator(
            task_id="decompress_csv_task",
            bash_command = f"gunzip -f {output_gz_template}"
        )

        # Task 3: Convert the decompressed CSV file to Parquet format.
        format_to_parquet_task = PythonOperator(
            task_id="format_to_parquet_task",
            python_callable=format_to_parquet,
            op_kwargs={
                "src_file": f"{output_csv_template}",
            },
        )

        # Task 4: Upload the Parquet file to Google Cloud Storage.
        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": f"raw/{output_gcs_template}",
                "local_file": f"{output_parquet_template}",
            },
        )

        # Task 5: Remove temp files from docker
        rm_temp_files_from_Docker_task = BashOperator(
            task_id="rm_temp_files_from_Docker_task",
            bash_command=f"rm {output_gz_template} {output_csv_template} {output_parquet_template}"
        )

        # Set the task dependencies:
        download_dataset_task >> decompress_task >> format_to_parquet_task >> local_to_gcs_task >> rm_temp_files_from_Docker_task


# Create dags for:
# 1) Yellow and green taxi data: Jan 2019 - July 2021
# 2) FHV Data: 2019
# 3) Zone Data: only one file

# Define dataset file names and the URL to download.
# Note: The file is in .gz format.
URL_PREFIX = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download'

# yellow taxi trip data
YELLOW_URL_TEMPLATE = URL_PREFIX + '/yellow/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
YELLOW_OUTPUT_GZ_TEMPLATE = AIRFLOW_HOME + '/yellow_trip_data_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
YELLOW_OUTPUT_CSV_TEMPLATE = YELLOW_OUTPUT_GZ_TEMPLATE.replace('.csv.gz', '.csv')
YELLOW_OUTPUT_PARQUET_TEMPLATE = YELLOW_OUTPUT_CSV_TEMPLATE.replace('.csv', '.parquet')
YELLOW_OUTPUT_GCS_TEMPLATE = 'yellow_trip_data/{{ execution_date.strftime(\'%Y-%m\') }}.parquet'


yellow_taxi_data_dag = DAG(
    dag_id="yellow_taxi_data_dag",
    schedule_interval="0 6 2 * *",
    end_date=datetime(2021, 8, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
)

download_parquetize_load_gcs(
    dag=yellow_taxi_data_dag,
    url_template=YELLOW_URL_TEMPLATE,
    output_gz_template=YELLOW_OUTPUT_GZ_TEMPLATE,
    output_csv_template=YELLOW_OUTPUT_CSV_TEMPLATE,
    output_parquet_template=YELLOW_OUTPUT_PARQUET_TEMPLATE,
    output_gcs_template=YELLOW_OUTPUT_GCS_TEMPLATE
)

# green taxi trip data
GREEN_URL_TEMPLATE = URL_PREFIX + '/green/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
GREEN_OUTPUT_GZ_TEMPLATE = AIRFLOW_HOME + '/green_trip_data_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
GREEN_OUTPUT_CSV_TEMPLATE = GREEN_OUTPUT_GZ_TEMPLATE.replace('.csv.gz', '.csv')
GREEN_OUTPUT_PARQUET_TEMPLATE = GREEN_OUTPUT_CSV_TEMPLATE.replace('.csv', '.parquet')
GREEN_OUTPUT_GCS_TEMPLATE = 'green_trip_data/{{ execution_date.strftime(\'%Y-%m\') }}.parquet'


green_taxi_data_dag = DAG(
    dag_id="green_taxi_data_dag",
    schedule_interval="0 6 2 * *",
    end_date=datetime(2021, 8, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
)

download_parquetize_load_gcs(
    dag=green_taxi_data_dag,
    url_template=GREEN_URL_TEMPLATE,
    output_gz_template=GREEN_OUTPUT_GZ_TEMPLATE,
    output_csv_template=GREEN_OUTPUT_CSV_TEMPLATE,
    output_parquet_template=GREEN_OUTPUT_PARQUET_TEMPLATE,
    output_gcs_template=GREEN_OUTPUT_GCS_TEMPLATE
)

# FHV data
FHV_URL_TEMPLATE = URL_PREFIX + '/fhv/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
FHV_OUTPUT_GZ_TEMPLATE = AIRFLOW_HOME + '/fhv_trip_data_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz'
FHV_OUTPUT_CSV_TEMPLATE = FHV_OUTPUT_GZ_TEMPLATE.replace('.csv.gz', '.csv')
FHV_OUTPUT_PARQUET_TEMPLATE = FHV_OUTPUT_CSV_TEMPLATE.replace('.csv', '.parquet')
FHV_OUTPUT_GCS_TEMPLATE = 'fhv_trip_data/{{ execution_date.strftime(\'%Y-%m\') }}.parquet'


fhv_taxi_data_dag = DAG(
    dag_id="fhv_taxi_data_dag",
    schedule_interval="0 6 2 * *",
    end_date=datetime(2020, 1, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
)

download_parquetize_load_gcs(
    dag=fhv_taxi_data_dag,
    url_template=FHV_URL_TEMPLATE,
    output_gz_template=FHV_OUTPUT_GZ_TEMPLATE,
    output_csv_template=FHV_OUTPUT_CSV_TEMPLATE,
    output_parquet_template=FHV_OUTPUT_PARQUET_TEMPLATE,
    output_gcs_template=FHV_OUTPUT_GCS_TEMPLATE
)

