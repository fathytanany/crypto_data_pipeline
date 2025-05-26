from datetime import datetime, timedelta
import requests, boto3, json
import pandas as pd
from io import BytesIO
import os
import redshift_connector
import logging

S3_BUCKET = os.getenv("S3_BUCKET")
RAW_KEY = "crypto/raw/crypto_data.json"
TRANSFORMED_KEY = "crypto/processed/crypto_data_cleaned.csv"
s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv("aws_access_key_id"),
        aws_secret_access_key=os.getenv("aws_secret_access_key"),
        region_name=os.getenv("s3_region")
    )

print(S3_BUCKET)
def fetch_and_store_to_s3():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency": "usd", "ids": "bitcoin,ethereum"}
    response = requests.get(url, params=params)
    data = response.json()

    timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%SZ")
    file_key = f"crypto-data/raw/data_{timestamp}.json"
    logging.info(f"bucket: {S3_BUCKET}")
    s3.put_object(Bucket=S3_BUCKET, Key=file_key, Body=json.dumps(data))

    logging.info("Successfully uploaded to S3.")

def transform_and_store_to_redshift():
    obj = s3.get_object(Bucket=S3_BUCKET, Key=RAW_KEY)
    raw_data = json.loads(obj['Body'].read())

    df = pd.json_normalize(raw_data)
    df_clean = df[['id', 'symbol', 'name', 'current_price', 'market_cap', 'last_updated']]

    # Save cleaned CSV to S3
    csv_buffer = BytesIO()
    df_clean.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=S3_BUCKET, Key=TRANSFORMED_KEY, Body=csv_buffer.getvalue())

    # Load into Redshift
    conn = redshift_connector.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    cursor = conn.cursor()

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
        FROM 's3://{S3_BUCKET}/{TRANSFORMED_KEY}'
        IAM_ROLE '{os.getenv("REDSHIFT_IAM_ROLE")}'
        FORMAT AS CSV
        IGNOREHEADER 1
        TIMEFORMAT 'auto';
    """
    cursor.execute(copy_command)
    conn.commit()
    cursor.close()
    conn.close()


