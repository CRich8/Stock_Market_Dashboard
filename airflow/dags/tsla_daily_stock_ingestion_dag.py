import os
import requests
import pandas as pd

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from datetime import datetime, timedelta

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'stock_data_all')

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)





url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=TSLA&apikey=W1JGK0HSLX19RW0I&outputsize=full"

def get_stock_data(url):
    r = requests.get(url)
    data = r.json()
    data = data['Time Series (Daily)']
    df_key = pd.DataFrame.from_dict(data.keys())
    df_key = df_key.rename(columns={0:'Date'})
    df_values = pd.DataFrame.from_dict(data.values())
    df_values = df_values.rename(columns={'1. open':'Open','2. high': 'High','3. low': 'Low','4. close' : 'Close', '5. adjusted close' : 'Adjusted_Close','6. volume' : 'Volume','7. dividend amount' : 'Dividend_Amount','8. split coefficient' : 'Split_coefficient'})
    frames = [df_key,df_values]
    df_final = pd.concat(frames, axis=1)
    df_final = df_final.to_csv(index=False)
    with open('tsla_output_daily_stock_adjusted.csv', 'w') as file:
        file.write(df_final)

OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + f"/tsla_output_daily_stock_adjusted.csv"
CSV_FILE = f"tsla_daily_stock_adjusted.csv"
DATASET = "tsla_daily_stock"
INPUT_FILETYPE = "csv"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(0),
    "retries": 1,
}

with DAG(
    dag_id="tsla_daily_stock_ingestion_dag",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
    schedule_interval="@daily",

) as dag:



    downlod_dataset_python_task = PythonOperator(
        task_id=f"download_daily_stock_adjusted_dataset_task",
        python_callable=get_stock_data,
         op_kwargs={
             "url": url
        },
    )
    local_to_gcs_task = PythonOperator(
        task_id=f"local_daily_stock_adjusted_dataset_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"daily_stock_adjusted/{CSV_FILE}",
            "local_file": f"{OUTPUT_FILE_TEMPLATE}",
        },
    )

    rm_task = BashOperator(
        task_id=f"rm_daily_stock_adjusted_dataset_task",
        bash_command=f"rm {OUTPUT_FILE_TEMPLATE}"
    )


    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id=f"bq_daily_stock_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": f"{DATASET}_external_table",
            },
            "externalDataConfiguration": {
                "autodetect": True,
                "sourceFormat": f"{INPUT_FILETYPE.upper()}",
                "sourceUris": [f"gs://{BUCKET}/daily_stock_adjusted/{CSV_FILE}"],
            },
        },
    )

    CREATE_BQ_TBL_QUERY = (
        f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{DATASET} \
        AS \
        SELECT * FROM {BIGQUERY_DATASET}.{DATASET}_external_table;"
    )

    # bq_create_partitioned_table_job = BigQueryInsertJobOperator(
    #     task_id=f"bq_create_{DATASET}_table_task",
    #     configuration={
    #         "query": {
    #             "query": CREATE_BQ_TBL_QUERY,
    #             "useLegacySql": False,
    #         }
    #     }
    # )
    
    downlod_dataset_python_task >> local_to_gcs_task >> rm_task >> bigquery_external_table_task #>> bq_create_partitioned_table_job

    
