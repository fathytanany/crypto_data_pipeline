COPY crypto_data
FROM '{s3_path}'
IAM_ROLE '{iam_role}'
REGION '{region}'
FORMAT AS CSV
IGNOREHEADER 1
TIMEFORMAT 'auto';
