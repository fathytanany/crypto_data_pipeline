from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))
from dotenv import load_dotenv
load_dotenv()
# Todo: Think about how to handle secrets in Airflow connections/ Airflow variables
from fetch_and_upload import fetch_and_store_to_s3, transform_and_store_to_redshift

default_args = {
    'owner': 'you',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    # TODO: retry_delay will be removed when retires set 0

}

with DAG(
    dag_id='crypto_pipeline_s3',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    catchup=False,
    tags=['crypto', 's3']
) as dag:
    ts_template = "{{ ts }}"

    fetch_and_upload = PythonOperator(
        task_id='fetch_and_upload_to_s3',
        python_callable=fetch_and_store_to_s3,
        op_kwargs={
            'timestamp': ts_template,

        }
        
    )
    transform_and_store = PythonOperator(
        task_id="transform_and_store_to_redshift",
        python_callable=transform_and_store_to_redshift,
        op_kwargs={
            'timestamp': ts_template,

        }
    )
    # TODO: SUE SQLExecuteQueryOperator to execute SQL queries in Airflow with template files.
    # execute_query = SQLExecuteQueryOperator(
    #     task_id="execute_query",
    #     sql=f"SELECT 1; SELECT * FROM {AIRFLOW_DB_METADATA_TABLE} LIMIT 1;",
    #     split_statements=True,
    #     return_last=False,
    # )
    fetch_and_upload >> transform_and_store
