from airflow import DAG
from airflow.operators.python_operator import PythonOperator # type: ignore
from datetime import datetime, timedelta
import boto3 # type: ignore
import smtplib

# Placeholder functions
def process_orders():
    # Simulate pulling order data from S3
    s3 = boto3.client('s3')
    print("Processing orders from S3...")

def sync_inventory():
    print("Syncing inventory...")

def send_confirmation_emails():
    print("Sending email confirmations...")

default_args = {
    'owner': 'ecommerce-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'order_cycle_pipeline',
    default_args=default_args,
    description='Order processing and email confirmation DAG',
    schedule_interval=timedelta(days=1),
    catchup=False
)

t1 = PythonOperator(
    task_id='process_orders',
    python_callable=process_orders,
    dag=dag,
)

t2 = PythonOperator(
    task_id='sync_inventory',
    python_callable=sync_inventory,
    dag=dag,
)

t3 = PythonOperator(
    task_id='send_confirmation_emails',
    python_callable=send_confirmation_emails,
    dag=dag,
)

t1 >> t2 >> t3
