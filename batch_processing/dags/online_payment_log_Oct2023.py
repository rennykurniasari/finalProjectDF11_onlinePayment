import os

from airflow import models
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

dag = models.DAG(
    dag_id='online_payment_log',
    schedule_interval="@monthly",
    start_date=datetime(2023, 10, 1),
    tags=['Oct2023'],
)
load_transaction = SimpleHttpOperator(
                        task_id="load_transaction_gcs_to_bq",
                        http_conn_id='load_transaction',
                        method="GET",
                        dag=dag
                    )
load_email = SimpleHttpOperator(
                        task_id="load_email_gcs_to_bq",
                        http_conn_id='load_email',
                        method="GET",
                        dag=dag
                    )

load_fraud = SimpleHttpOperator(
                        task_id="load_fraud_gcs_to_bq",
                        http_conn_id='load_fraud',
                        method="GET",
                        dag=dag
                    )

load_transaction >> load_email >> load_fraud
