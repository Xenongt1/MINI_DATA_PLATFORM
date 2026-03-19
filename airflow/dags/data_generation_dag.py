import csv
import random
import uuid
import os
import logging
import io
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import boto3
from botocore.exceptions import ClientError

# Configuration
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000') # Docker internal network
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')
BUCKET_NAME = 'raw-sales-data'

# Master Data
PRODUCTS = [
    {"product_id": "P-1001", "name": "Laptop Pro", "price": 1299.99, "category": "Electronics"},
    {"product_id": "P-1002", "name": "Smartphone X", "price": 899.50, "category": "Electronics"},
    {"product_id": "P-1003", "name": "Wireless Headphones", "price": 199.99, "category": "Accessories"},
    {"product_id": "P-1004", "name": "4K Monitor 27inch", "price": 349.00, "category": "Electronics"},
    {"product_id": "P-1005", "name": "Mechanical Keyboard", "price": 129.99, "category": "Accessories"}
]

CUSTOMERS = [
    {"cust_id": "C-501", "name": "Alice Smith"},
    {"cust_id": "C-502", "name": "Bob Jones"},
    {"cust_id": "C-503", "name": "Charlie Brown"},
    {"cust_id": "C-504", "name": "Diana Prince"},
    {"cust_id": "C-505", "name": "Evan Wright"}
]

SALESPERSONS = [
    {"emp_id": "E-001", "name": "John Doe"},
    {"emp_id": "E-002", "name": "Jane Roe"},
    {"emp_id": "E-003", "name": "Jim Beam"}
]

STORE_LOCATIONS = ["New York", "San Francisco", "London", "Tokyo", "Berlin", "Toronto", "Sydney"]

def generate_sale(base_date=None):
    product = random.choice(PRODUCTS)
    customer = random.choice(CUSTOMERS)
    salesperson = random.choice(SALESPERSONS)
    
    quantity = random.randint(1, 5)
    total_amount = round(product["price"] * quantity, 2)
    
    # 10% chance to mess up the data format for data cleaning exercises
    is_dirty = random.random() < 0.10
    
    if is_dirty:
        dirty_type = random.choice(['null_name', 'string_qty', 'missing_location', 'negative_price'])
        if dirty_type == 'null_name':
            customer_name = ""  # Missing value
            store_location = random.choice(STORE_LOCATIONS)
        elif dirty_type == 'string_qty':
            quantity = str(quantity) + " units"  # String instead of int
            customer_name = customer["name"]
            store_location = random.choice(STORE_LOCATIONS)
        elif dirty_type == 'missing_location':
            store_location = None # Missing value
            customer_name = customer["name"]
        elif dirty_type == 'negative_price':
            total_amount = -total_amount  # Negative amount anomaly
            customer_name = customer["name"]
            store_location = random.choice(STORE_LOCATIONS)
    else:
        customer_name = customer["name"]
        store_location = random.choice(STORE_LOCATIONS)
    
    if not base_date:
        base_date = datetime.now()
        
    tx_time = base_date - timedelta(
        days=random.randint(0, 28),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )
    
    return [
        str(uuid.uuid4()),                    # transaction_id
        tx_time.isoformat(),                  # transaction_date
        customer["cust_id"],                  # customer_id
        customer_name,                        # customer_name (sometimes empty)
        salesperson["emp_id"],                # salesperson_id
        salesperson["name"],                  # salesperson_name
        product["product_id"],                # product_id
        product["name"],                      # product_name
        product["category"],                  # category
        quantity,                             # quantity (sometimes string)
        product["price"],                     # unit_price
        total_amount,                         # total_amount (sometimes negative)
        store_location                        # store_location (sometimes empty)
    ]

def generate_month(num_records=200):
    base_date = datetime.now()
    records = []
    
    for _ in range(num_records):
        record = generate_sale(base_date)
        records.append(record)
        
        # 5% chance to inject an exact identical duplicate row
        if random.random() < 0.05:
            records.append(record)
            
    return records

def generate_and_upload_data(**kwargs):
    s3_client = boto3.client(
        's3', endpoint_url=MINIO_ENDPOINT, aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY, region_name='us-east-1'
    )
    
    # Ensure bucket exists
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            s3_client.create_bucket(Bucket=BUCKET_NAME)
            logging.info(f"Created MinIO bucket '{BUCKET_NAME}'.")
        else:
            raise
            
    # Generate records
    num_records = 200
    records = generate_month(num_records)
    logging.info(f"Generated {len(records)} fake sales records in memory.")
    
    # Write to memory object (StringIO)
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    headers = [
        'transaction_id', 'transaction_date', 'customer_id', 'customer_name', 
        'salesperson_id', 'salesperson_name', 'product_id', 'product_name', 
        'category', 'quantity', 'unit_price', 'total_amount', 'store_location'
    ]
    writer.writerow(headers)
    writer.writerows(records)
    
    # Upload directly to MinIO
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"sales_data_{timestamp_str}.csv"
    
    s3_client.put_object(
        Bucket=BUCKET_NAME, 
        Key=filename, 
        Body=csv_buffer.getvalue().encode('utf-8')
    )
    
    logging.info(f"✅ Successfully uploaded {filename} directly into MinIO bucket {BUCKET_NAME}.")
    return filename


# Define the Data Generation DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'data_generation_dag',
    default_args=default_args,
    description='Generates fake sales data and dumps it into MinIO',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sales', 'generator', 'minio'],
) as dag:

    # Task 1: Generate Data and push to MinIO
    t1_generate_data = PythonOperator(
        task_id='generate_and_upload_data',
        python_callable=generate_and_upload_data
    )

    # Task 2: Trigger the ETL pipeline immediately after generation
    t2_trigger_etl = TriggerDagRunOperator(
        task_id='trigger_sales_etl_pipeline',
        trigger_dag_id='sales_data_pipeline',  # Matches your existing DAG ID
        wait_for_completion=False,
        reset_dag_run=True,
    )

    t1_generate_data >> t2_trigger_etl
