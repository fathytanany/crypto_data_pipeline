from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', ))
from dotenv import load_dotenv
load_dotenv()
# Todo: Think about how to handle secrets in Airflow connections/ Airflow variables
from scripts.operations import fetch_data, store_data_to_s3, transform_data, store_transformed_to_s3,run_sql_file_on_redshift

def pass_timestamp(**kwargs):
    return kwargs["ts"]

with DAG(
    dag_id="crypto_pipeline",
    # schedule_interval="*/5 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    # default_args={"retries": 2, "retry_delay": timedelta(minutes=3)}
    tags=['crypto', 's3', 'redshift']
) as dag:

    get_ts = PythonOperator(
        task_id="get_timestamp",
        python_callable=pass_timestamp,
        provide_context=True
    )

    fetch = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_data,
        op_kwargs={"ts": "{{ ti.xcom_pull(task_ids='get_timestamp') }}",
                   "API_url":"https://api.coingecko.com/api/v3/coins/markets",
                   "API_params":{"vs_currency": "usd", "ids": "bitcoin,ethereum"}
                   },
    )

    store = PythonOperator(
        task_id="store_to_s3",
        python_callable=store_data_to_s3,
        op_kwargs={"ts": "{{ ti.xcom_pull(task_ids='get_timestamp') }}"},
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        op_kwargs={"ts": "{{ ti.xcom_pull(task_ids='get_timestamp') }}"},
    )

    store_transformed = PythonOperator(
        task_id="store_transformed_to_s3",
        python_callable=store_transformed_to_s3,
        op_kwargs={"ts": "{{ ti.xcom_pull(task_ids='get_timestamp') }}"},
    )

    create_table = PythonOperator(
        task_id="create_redshift_table",
        python_callable=run_sql_file_on_redshift,
        op_kwargs={"sql_path": "utils/redshift_sql/create_table.sql"},
    )

    copy_to_redshift = PythonOperator(
        task_id="copy_data_to_redshift",
        python_callable=run_sql_file_on_redshift,
        op_kwargs={
            "sql_path": "utils/redshift_sql/copy_to_table.sql",
            "params": {
                "bucket": os.getenv("S3_BUCKET"),
                "region": os.getenv("AWS_REGION"),
                "timestamp": "{{ ti.xcom_pull(task_ids='get_timestamp') }}",
                "iam_role": os.getenv("REDSHIFT_IAM_ROLE"),
            },
        },
    )

    get_ts >> fetch >> store >> transform >> store_transformed >> create_table >> copy_to_redshift