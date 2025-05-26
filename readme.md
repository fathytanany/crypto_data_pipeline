# Crypto Data Pipeline with Apache Airflow

This project builds a crypto data pipeline using Airflow, AWS S3, and Amazon Redshift.

## Architecture
- Fetch data from CoinGecko API
- Store raw JSON in S3
- Transform with Pandas
- Save CSV to S3
- Load into Redshift using `COPY` command
- Visualize using AWS QuickSight

## Setup
1. Create `.env` file with your credentials
2. Run with Docker Compose:
```bash
docker-compose up airflow airflow_scheduler
```
3. Open Airflow UI at `http://localhost:8080`

## DAG: `crypto_pipeline_s3_redshift`
- five minutes schedule
- Monitors and logs each step

---