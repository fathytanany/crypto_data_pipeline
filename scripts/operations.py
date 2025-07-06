import os
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(CURRENT_DIR))

from utils.operation_utils import boto_client


def fetch_data(ti, **kwargs):
    import requests
    url = kwargs['API_url']
    params = kwargs['API_params']
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    ti.xcom_push(key='fetched_data', value=data)

def store_data_to_s3(**kwargs):
    import os
    import json
    timestamp = kwargs['ts']
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_data', key='fetched_data')
    raw_key = f"{os.getenv('RAW_FOLDER')}/{timestamp}/raw_data.json"

    s3 = boto_client('s3')
    s3.put_object(Bucket=os.getenv("S3_BUCKET"), Key=raw_key, Body=json.dumps(data))
    ti.xcom_push(key='raw_key', value=raw_key)

def transform_data(**kwargs):
    import json
    import pandas as pd
    ti = kwargs['ti']
    raw_key = ti.xcom_pull(task_ids='store_to_s3', key='raw_key')

    s3 = boto_client('s3')
    obj = s3.get_object(Bucket=os.getenv("S3_BUCKET"), Key=raw_key)
    raw_data = json.loads(obj['Body'].read())

    df = pd.json_normalize(raw_data)
    # df_clean = df[['id', 'symbol', 'name', 'current_price', 'market_cap','total_volume', 'last_updated']]
    df_clean = df[['id', 'symbol', 'name', 'current_price', 'market_cap', 'total_volume', 'last_updated' ,'price_change_percentage_24h','ath','atl']]
    df_clean['last_updated'] = pd.to_datetime(df_clean['last_updated']).dt.strftime('%Y-%m-%d %H:%M:%S')
    ti.xcom_push(key='cleaned_data', value=df_clean.to_json(orient='records'))

def store_transformed_to_s3(**kwargs):
    import json
    import pandas as pd
    from io import BytesIO
    import os
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