import csv
import random
import uuid
import os
import time
from datetime import datetime, timedelta

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    print("Error: The 'boto3' library is not installed.")
    print("Please install it by running: pip install boto3")
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
    """Initializes and returns a boto3 client configured for MinIO."""
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name='us-east-1'
    )

def ensure_bucket_exists(s3_client, bucket_name):
    """Ensures that the target bucket exists in MinIO."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            raise

def generate_sale(base_date=None):
    """Creates one sale record with random values."""
    product = random.choice(PRODUCTS)
    customer = random.choice(CUSTOMERS)
    salesperson = random.choice(SALESPERSONS)
    
    quantity = random.randint(1, 5)
    total_amount = round(product["price"] * quantity, 2)
    
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
        customer["name"],                     # customer_name
        salesperson["emp_id"],                # salesperson_id
        salesperson["name"],                  # salesperson_name
        product["product_id"],                # product_id
        product["name"],                      # product_name
        product["category"],                  # category
        quantity,                             # quantity
        product["price"],                     # unit_price
        total_amount,                         # total_amount
        random.choice(STORE_LOCATIONS)        # store_location
    ]

def generate_month(num_records=200):
    """Generates sales spread across a month."""
    base_date = datetime.now()
    return [generate_sale(base_date) for _ in range(num_records)]

def save_csv(records, filename_prefix="sales_data"):
    """Writes the records to a CSV file in data/raw/."""
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
        
    print(f"📁 Saved {len(records)} records to {filepath}")
    return filepath, filename

def upload_to_minio(s3_client, bucket_name, filepath, file_name):
    """Pushes the CSV file to our MinIO bucket."""
    try:
        print(f"📤 Uploading {file_name} to MinIO bucket '{bucket_name}'...")
        s3_client.upload_file(filepath, bucket_name, file_name)
        print("✅ Upload complete!")
    except ClientError as e:
        print(f"❌ Upload failed: {e}")
        raise

def main():
    print("=" * 50)
    print("🚀 Starting Data Generation Simulator")
    print("=" * 50)
    
    try:
        s3_client = get_s3_client()
        ensure_bucket_exists(s3_client, BUCKET_NAME)
        
        # 1 & 2: Generate 200 records using generate_month and generate_sale
        records = generate_month(200)
        
        # 3: Save to local data/raw directory
        filepath, filename = save_csv(records)
        
        # 4: Upload the local file to MinIO
        upload_to_minio(s3_client, BUCKET_NAME, filepath, filename)
        
    except Exception as e:
        print(f"🚨 An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
