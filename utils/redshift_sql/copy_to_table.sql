COPY crypto_prices
FROM '{{ s3_path }}'
IAM_ROLE '{{ iam_role }}'
FORMAT AS CSV
IGNOREHEADER 1
TIMEFORMAT 'auto';