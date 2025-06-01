import boto3
import os

def boto_client(service):
    AWS_SECRET=os.getenv("aws_access_key_id")
    AWS_ACCESS=os.getenv("aws_secret_access_key")
    REGION = os.getenv("AWS_REGION")

    return boto3.client(
        service,
        aws_access_key_id=AWS_SECRET,
        aws_secret_access_key=AWS_ACCESS,
        region_name=REGION
    )