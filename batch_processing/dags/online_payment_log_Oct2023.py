import os

from airflow import models
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator,BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

DATASET_NAME = os.environ.get("GCP_DATASET_NAME", 'online_payment_log_Oct2023')
FRAUD_TABLE_NAME = os.environ.get("GCP_TABLE_NAME", 'fraud_transaction')
TRANSACTION_TABLE_NAME = os.environ.get("GCP_TABLE_NAME", 'transaction')

dag = models.DAG(
    dag_id='online_payment_log',
    schedule_interval="@monthly",
    start_date=datetime(2023, 10, 7),
    tags=['Oct2023'],
)
'''
load_fraud_json_gcs_to_bq = GCSToBigQueryOperator(
                    task_id="load_fraud_json_gcs_to_bq",
                    bucket="df-11-group-1",
                    source_objects=["fraud_confirmation/*.json"],
                    source_format="NEWLINE_DELIMITED_JSON",
                    destination_project_dataset_table=f"{DATASET_NAME}.{FRAUD_TABLE_NAME}",
                    write_disposition="WRITE_TRUNCATE",
                    dag=dag
                )
'''

load_transaction_json_gcs_to_bq = GCSToBigQueryOperator(
                    task_id="load_transaction_json_gcs_to_bq",
                    bucket="df-11-group-1",
                    source_objects=["transaction_1/*.json"],
                    source_format="NEWLINE_DELIMITED_JSON",
                    destination_project_dataset_table=f"{DATASET_NAME}.{TRANSACTION_TABLE_NAME}",
                    write_disposition="WRITE_TRUNCATE",
                    dag=dag
                )

#load_fraud_json_gcs_to_bq
load_transaction_json_gcs_to_bq
