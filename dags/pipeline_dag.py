from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))
from dotenv import load_dotenv
load_dotenv()
from fetch_and_upload import fetch_and_store_to_s3

default_args = {
    'owner': 'you',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='crypto_pipeline_s3',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    catchup=False,
    tags=['crypto', 's3']
) as dag:

    fetch_and_upload = PythonOperator(
        task_id='fetch_and_upload_to_s3',
        python_callable=fetch_and_store_to_s3,
        op_kwargs={
            'bucket': 'your-bucket-name',
            'aws_access_key_id': 'YOUR_ACCESS_KEY',
            'aws_secret_access_key': 'YOUR_SECRET_KEY'
        }
    )
