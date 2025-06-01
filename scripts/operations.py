from datetime import datetime, timedelta
import requests, boto3, json
import pandas as pd
from io import BytesIO
import os
import redshift_connector
import logging
import sys
# TODO: Remove top level code because it get executed every time the module is imported

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(CURRENT_DIR))

from utils.operation_utils import boto_client


def fetch_data(ti, **kwargs):
    url = kwargs['API_url']
    params = kwargs['API_params']
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    ti.xcom_push(key='fetched_data', value=data)

def store_data_to_s3(**kwargs):
    timestamp = kwargs['ts']
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_data', key='fetched_data')
    raw_key = f"{os.getenv('RAW_FOLDER')}/{timestamp}/raw_data.json"

    s3 = boto_client('s3')
    s3.put_object(Bucket=os.getenv("S3_BUCKET"), Key=raw_key, Body=json.dumps(data))
    ti.xcom_push(key='raw_key', value=raw_key)

def transform_data(**kwargs):
    ti = kwargs['ti']
    raw_key = ti.xcom_pull(task_ids='store_to_s3', key='raw_key')

    s3 = boto_client('s3')
    obj = s3.get_object(Bucket=os.getenv("S3_BUCKET"), Key=raw_key)
    raw_data = json.loads(obj['Body'].read())

    df = pd.json_normalize(raw_data)
    df_clean = df[['id', 'symbol', 'name', 'current_price', 'market_cap', 'last_updated']]
    ti.xcom_push(key='cleaned_data', value=df_clean.to_json(orient='records'))

def store_transformed_to_s3(**kwargs):
    timestamp = kwargs['ts']
    ti = kwargs['ti']
    data = json.loads(ti.xcom_pull(task_ids='transform_data', key='cleaned_data'))
    df_clean = pd.DataFrame(data)
    transformed_key = f"{os.getenv('TRANSFORMED_FOLDER')}/{timestamp}/data.csv"

    csv_buffer = BytesIO()
    df_clean.to_csv(csv_buffer, index=False)

    s3 = boto_client('s3')
    s3.put_object(Bucket=os.getenv("S3_BUCKET"), Key=transformed_key, Body=csv_buffer.getvalue())
    ti.xcom_push(key='transformed_key', value=transformed_key)

def run_sql_file_on_redshift(sql_path, params=None, **kwargs):
    with open(sql_path, 'r') as file:
        sql = file.read()

    if params:
        for key, val in params.items():
            sql = sql.replace(f"{{{{ {key} }}}}", val)

    conn = redshift_connector.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=5439,
        ssl=True,
    )
    print(sql)

    cursor = conn.cursor()
    cursor.execute(sql)
    cursor.close()
    conn.commit()
    conn.close()





if __name__ == '__main__':
    
    op_kwargs={
            "sql_path": "utils/redshift_sql/copy_to_table.sql",
            "params": {
                "s3_path": f"s3://destudybucket/crypto-data/processed/crypto_data_cleaned_2025-05-31T21:38:56.198663+00:00.csv",
                "region": os.getenv("AWS_REGION"),
                "timestamp": "{{ ti.xcom_pull(task_ids='get_timestamp') }}",
                "iam_role": os.getenv("REDSHIFT_IAM_ROLE"),
            },
        }
    run_sql_file_on_redshift(op_kwargs['sql_path'],op_kwargs['params'])