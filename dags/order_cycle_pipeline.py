from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import random
import time
import json
import boto3
import os
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Retrieve S3 configurations from environment variables
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_KEY_PREFIX = os.getenv("S3_KEY_PREFIX", "orders/")  # Default prefix if not set

# Initialize S3 client
s3_client = boto3.client('s3')

# Mock function to simulate order placement
def place_order(**kwargs):
    order_data = {
        "order_id": 123,
        "customer_name": "John Doe",
        "product": "Laptop",
        "quantity": 1,
        "price": 1500
    }
    print(f"Order placed: {order_data}")

    # Convert to JSON string
    order_data_json = json.dumps(order_data)

    # Define S3 object key
    s3_key = f"{S3_KEY_PREFIX}{order_data['order_id']}.json"

    try:
        # Upload order data to S3
        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=order_data_json)
        print(f"Order data saved to S3 bucket {S3_BUCKET_NAME} with key {s3_key}")
    except (NoCredentialsError, PartialCredentialsError) as e:
        print("S3 upload failed due to missing or incomplete credentials:", e)

    return order_data_json

# Function to process the order
def process_order(**kwargs):
    order_data = json.loads(kwargs['ti'].xcom_pull(task_ids='place_order'))
    delay = random.uniform(0, 3)
    time.sleep(delay)

    if random.random() < 0.1:
        raise Exception(f"Random failure occurred while processing order: {order_data['order_id']}")

    print(f"Processing order {order_data['order_id']} after {delay:.2f} seconds")

# Function to update inventory
def update_inventory(**kwargs):
    order_data = json.loads(kwargs['ti'].xcom_pull(task_ids='place_order'))
    delay = random.uniform(0, 3)
    time.sleep(delay)
    print(f"Updating inventory for product: {order_data['product']} after {delay:.2f} seconds")

# Function to generate invoice
def generate_invoice(**kwargs):
    order_data = json.loads(kwargs['ti'].xcom_pull(task_ids='place_order'))
    delay = random.uniform(0, 3)
    time.sleep(delay)
    invoice = f"Invoice for Order ID: {order_data['order_id']} - Total: ${order_data['price'] * order_data['quantity']}"
    print(f"{invoice} after {delay:.2f} seconds")

# Function to send confirmation email
def send_confirmation_email(**kwargs):
    order_data = json.loads(kwargs['ti'].xcom_pull(task_ids='place_order'))
    delay = random.uniform(0, 3)
    time.sleep(delay)
    print(f"Sending confirmation email for Order ID: {order_data['order_id']} to {order_data['customer_name']} after {delay:.2f} seconds")

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'ecommerce_order_processing_pipeline',
    default_args=default_args,
    description='A simple e-commerce order processing pipeline',
    schedule_interval='*/1 * * * *',
    catchup=False,
    is_paused_upon_creation=False
)

# Define tasks
place_order_task = PythonOperator(
    task_id='place_order',
    python_callable=place_order,
    provide_context=True,
    dag=dag
)

process_order_task = PythonOperator(
    task_id='process_order',
    python_callable=process_order,
    provide_context=True,
    dag=dag
)

update_inventory_task = PythonOperator(
    task_id='update_inventory',
    python_callable=update_inventory,
    provide_context=True,
    dag=dag
)

generate_invoice_task = PythonOperator(
    task_id='generate_invoice',
    python_callable=generate_invoice,
    provide_context=True,
    dag=dag
)

send_email_task = PythonOperator(
    task_id='send_confirmation_email',
    python_callable=send_confirmation_email,
    provide_context=True,
    dag=dag
)

# Set task dependencies
place_order_task >> process_order_task >> update_inventory_task >> generate_invoice_task >> send_email_task
