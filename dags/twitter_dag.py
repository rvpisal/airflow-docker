from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from twitter_etl import run_twitter_etl
import os
import logging
from airflow.utils.log.logging_mixin import LoggingMixin

# from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2023,8,6),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes = 1)
}

final_dag = DAG(
    'twitter_dag',
    default_args=default_args,
    description='My first ETL airflow dag'
)

run_etl = PythonOperator (
    task_id = 'complete_twitter_etl',
    python_callable=run_twitter_etl,
    dag = final_dag
)

# logging.error ('This is debug',os.getenv("AIRFLOW__CORE__SQL_ALCHEMY_CONN"))

# LoggingMixin().log.error("Hello",os.getenv("AWS_BUCKET"))

# print (os.getenv("AIRFLOW__CORE__SQL_ALCHEMY_CONN"))

run_etl