import csv
import random
import uuid
import os
import time
import logging
from datetime import datetime, timedelta

# Configure standard logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    logging.error("The 'boto3' library is not installed.")
    logging.info("Please install it by running: pip install boto3")
    exit(1)

# Configuration
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')
BUCKET_NAME = 'raw-sales-data'
RAW_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'raw')

# Ensure the local raw data directory exists
os.makedirs(RAW_DATA_DIR, exist_ok=True)

# Master data for fake generation
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

def get_s3_client():
    """
    Initializes and returns a boto3 client configured for the local MinIO instance.
    Utilizes environment variables for precise configuration.
    
    Returns:
        botocore.client.S3: A configured S3 client object.
    """
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name='us-east-1'
    )

def ensure_bucket_exists(s3_client, bucket_name):
    """
    Ensures that the target bucket exists in the MinIO storage.
    If it does not exist, it creates it automatically.
    
    Args:
        s3_client (botocore.client.S3): The active S3 client.
        bucket_name (str): The name of the bucket to verify/create.
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            s3_client.create_bucket(Bucket=bucket_name)
            logging.info(f"Bucket '{bucket_name}' created successfully.")
        else:
            raise

def generate_sale(base_date=None):
    """
    Creates one single simulated sale record with random choices from master data.
    Intentionally injects 'dirty' data formats (nulls, string counts, duplicates) 
    roughly 10% of the time to simulate messy real-world reporting that requires cleaning.
    
    Args:
        base_date (datetime, optional): The anchor date for the sale. Defaults to datetime.now().
        
    Returns:
        list: A single row representing the transaction record.
    """
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
        
    # Give it a random timestamp within the month
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
    """
    Generates a bulk collection of sales spread out roughly across a single month.
    Also has a 5% chance to duplicate an exact row to test duplication cleaning.
    
    Args:
        num_records (int, optional): The target number of entries to generate. Defaults to 200.
        
    Returns:
        list of list: A list where each item is a transaction record array.
    """
    base_date = datetime.now()
    records = []
    
    for _ in range(num_records):
        record = generate_sale(base_date)
        records.append(record)
        
        # 5% chance to inject an exact identical duplicate row
        if random.random() < 0.05:
            records.append(record)
            
    return records

def save_csv(records, filename_prefix="sales_data"):
    """
    Takes raw list records and writes them out safely to a physical CSV file inside
    the local data/raw/ directory with a unique standard timestamp.
    
    Args:
        records (list of list): The generated transaction rows.
        filename_prefix (str, optional): The string prefix for the filename. Defaults to "sales_data".
        
    Returns:
        tuple: (filepath string, filename string) to track the final destination.
    """
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{filename_prefix}_{timestamp_str}.csv"
    filepath = os.path.join(RAW_DATA_DIR, filename)
    
    headers = [
        'transaction_id', 'transaction_date', 'customer_id', 'customer_name', 
        'salesperson_id', 'salesperson_name', 'product_id', 'product_name', 
        'category', 'quantity', 'unit_price', 'total_amount', 'store_location'
    ]
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(records)
        
    logging.info(f"Saved {len(records)} records to {filepath}")
    return filepath, filename

def upload_to_minio(s3_client, bucket_name, filepath, file_name):
    """
    Securely pushes the physical CSV file from the local file system
    straight into the target 'raw' object storage bucket on MinIO.
    
    Args:
        s3_client (botocore.client.S3): The active S3 connection client.
        bucket_name (str): The destination bucket string name.
        filepath (str): The absolute local filepath to the source file.
        file_name (str): The desired destination key/name within the bucket.
    """
    try:
        logging.info(f"Uploading {file_name} to MinIO bucket '{bucket_name}'...")
        s3_client.upload_file(filepath, bucket_name, file_name)
        logging.info("Upload complete!")
    except ClientError as e:
        logging.error(f"Upload failed: {e}")
        raise

def main():
    """
    Main orchestration function for the generator simulation.
    Ensures bucket availability, generates roughly a month's worth of messy 
    sales, saves it physically as a CSV, and then executes the upload command.
    """
    logging.info("Starting Data Generation Simulator")
    
    try:
        s3_client = get_s3_client()
        ensure_bucket_exists(s3_client, BUCKET_NAME)
        
        # 1 & 2: Generate 200 records using generate_month and generate_sale
        records = generate_month(200)
        
        # 3: Save to local data/raw directory
        filepath, filename = save_csv(records)
        
        # 4: Upload the local file to MinIO
        upload_to_minio(s3_client, BUCKET_NAME, filepath, filename)
        logging.info("Successfully finished generation and upload.")
        
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    main()
