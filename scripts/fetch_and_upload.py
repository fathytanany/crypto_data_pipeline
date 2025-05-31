from datetime import datetime, timedelta
import requests, boto3, json
import pandas as pd
from io import BytesIO
import os
import redshift_connector
import logging
# TODO: Remove top level code because it get executed every time the module is imported
S3_BUCKET = os.getenv("S3_BUCKET")
RAW_FOLDER = "crypto-data/raw"
TRANSFORMED_FOLDER = "crypto-data/processed"

REGION = os.getenv("AWS_REGION")
IAM_ROLE = os.getenv("REDSHIFT_IAM_ROLE")
REDSHIFT_ENDPOINT = os.getenv("DB_HOST")
REDSHIFT_PORT = os.getenv("DB_PORT", "5439")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
AWS_SECRET=os.getenv("aws_access_key_id")
AWS_ACCESS=os.getenv("aws_secret_access_key")

s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_SECRET,
        aws_secret_access_key=AWS_ACCESS,
        region_name=REGION
    )

def fetch_and_store_to_s3(**kwargs):
    # TODO: Add Prameters to the function to make it more generic
    # URL, Timestamp, etc.
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency": "usd", "ids": "bitcoin,ethereum"}
    response = requests.get(url, params=params)
    data = response.json()
    # TODO: This function should end with a return data
    # TODO: Use XCOM to pass data between tasks in Airflow
    file_key = f"{RAW_FOLDER}/data_{kwargs['timestamp']}.json"
    print("file_key",file_key)
    # TODO: Remove Print statements, Replace with logging.
    s3.put_object(Bucket=S3_BUCKET, Key=file_key, Body=json.dumps(data))

    print("Successfully uploaded to S3.")

def transform_and_store_to_redshift(**kwargs):
    # TODO: Add Parameters to the function to make it more generic
    # TODO: split the function into smaller functions
    raw_file = f"{RAW_FOLDER}/data_{kwargs['timestamp']}.json"
    print("raw_file",raw_file)
    obj = s3.get_object(Bucket=S3_BUCKET, Key=raw_file)
    raw_data = json.loads(obj['Body'].read())

    df = pd.json_normalize(raw_data)
    df_clean = df[['id', 'symbol', 'name', 'current_price', 'market_cap', 'last_updated']]

    # Save cleaned CSV to S3
    csv_buffer = BytesIO()
    df_clean.to_csv(csv_buffer, index=False)
    file_key = f"{TRANSFORMED_FOLDER}/data_{kwargs['timestamp']}.json" 
    print("file_key",file_key)
    s3.put_object(Bucket=S3_BUCKET, Key=file_key, Body=csv_buffer.getvalue())
    print("pushed transformed data")
    # Load into Redshift
    conn = redshift_connector.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    cursor = conn.cursor()
    # TODO: ADD the sql script into utils folder
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS crypto_prices (
            id VARCHAR(50),
            symbol VARCHAR(10),
            name VARCHAR(50),
            current_price FLOAT,
            market_cap BIGINT,
            last_updated TIMESTAMP
        )
    """)
    conn.commit()

    # COPY from S3 into Redshift
    copy_command = f"""
        COPY crypto_prices
        FROM 's3://{S3_BUCKET}/{file_key}'
        IAM_ROLE '{os.getenv("REDSHIFT_IAM_ROLE")}'
        FORMAT AS CSV
        IGNOREHEADER 1
        TIMEFORMAT 'auto';
    """
    cursor.execute(copy_command)
    conn.commit()
    cursor.close()
    conn.close()

    print("commit to redshift")


if __name__ == '__main__':
    
    timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%SZ")
    op_kwargs={
            'timestamp': timestamp,

        }
    fetch_and_store_to_s3(**op_kwargs)
    transform_and_store_to_redshift(**op_kwargs)