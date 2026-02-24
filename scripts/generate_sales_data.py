import csv
import random
import uuid
import os
import io
import time
from datetime import datetime, timedelta

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    print("Error: The 'boto3' library is not installed.")
    print("Please install it by running: pip install boto3")
    exit(1)

# Configuration - using MinIO credentials from our docker-compose
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')
BUCKET_NAME = 'raw-sales-data'

# Master data for fake generation
PRODUCTS = [
    {"product_id": "P-1001", "name": "Laptop Pro", "price": 1299.99, "category": "Electronics"},
    {"product_id": "P-1002", "name": "Smartphone X", "price": 899.50, "category": "Electronics"},
    {"product_id": "P-1003", "name": "Wireless Headphones", "price": 199.99, "category": "Accessories"},
    {"product_id": "P-1004", "name": "4K Monitor 27inch", "price": 349.00, "category": "Electronics"},
    {"product_id": "P-1005", "name": "Mechanical Keyboard", "price": 129.99, "category": "Accessories"},
    {"product_id": "P-1006", "name": "USB-C Hub", "price": 45.00, "category": "Accessories"}
]

CUSTOMERS = [
    {"cust_id": "C-501", "name": "Alice Smith"},
    {"cust_id": "C-502", "name": "Bob Jones"},
    {"cust_id": "C-503", "name": "Charlie Brown"},
    {"cust_id": "C-504", "name": "Diana Prince"},
    {"cust_id": "C-505", "name": "Evan Wright"},
    {"cust_id": "C-506", "name": "Fiona Gallagher"}
]

STORE_LOCATIONS = ["New York", "San Francisco", "London", "Tokyo", "Berlin", "Toronto", "Sydney"]

def get_s3_client():
    """Initializes and returns a boto3 client configured for our local MinIO instance."""
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name='us-east-1' # Boto3 requires a region, even for MinIO
    )

def ensure_bucket_exists(s3_client, bucket_name):
    """Ensures that the target bucket exists in MinIO."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"✅ Bucket '{bucket_name}' already exists.")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            print(f"🔨 Creating bucket '{bucket_name}'...")
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"✅ Bucket '{bucket_name}' created successfully.")
        else:
            print(f"❌ Error checking bucket: {e}")
            raise

def generate_sales_data(num_records):
    """Generates fake sales data and returns it as a CSV-formatted string."""
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Write the CSV header
    writer.writerow([
        'transaction_id', 
        'transaction_date', 
        'customer_id', 
        'customer_name', 
        'product_id', 
        'product_name', 
        'category', 
        'quantity', 
        'unit_price', 
        'total_amount', 
        'store_location'
    ])
    
    now = datetime.now()
    
    for _ in range(num_records):
        product = random.choice(PRODUCTS)
        customer = random.choice(CUSTOMERS)
        quantity = random.randint(1, 5)
        total_amount = round(product["price"] * quantity, 2)
        
        # Give it a random timestamp within the last 24 hours
        tx_time = now - timedelta(minutes=random.randint(0, 1440))
        
        writer.writerow([
            str(uuid.uuid4()),                    # transaction_id
            tx_time.isoformat(),                  # transaction_date
            customer["cust_id"],                  # customer_id
            customer["name"],                     # customer_name
            product["product_id"],                # product_id
            product["name"],                      # product_name
            product["category"],                  # category
            quantity,                             # quantity
            product["price"],                     # unit_price
            total_amount,                         # total_amount
            random.choice(STORE_LOCATIONS)        # store_location
        ])
    
    return output.getvalue()

def upload_to_minio(s3_client, bucket_name, file_name, file_content):
    """Uploads the string content to MinIO as a file."""
    try:
        print(f"📤 Uploading {file_name} to MinIO bucket '{bucket_name}'...")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=file_content,
            ContentType='text/csv'
        )
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
        
        # Randomize the number of records we simulate in this batch
        num_records = random.randint(50, 300)
        print(f"🎲 Generating {num_records} fake sales records...")
        
        start_time = time.time()
        csv_data = generate_sales_data(num_records)
        
        # Name the file with a timestamp
        timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_name = f"sales_batch_{timestamp_str}_{random.randint(1000, 9999)}.csv"
        
        upload_to_minio(s3_client, BUCKET_NAME, file_name, csv_data)
        
        elapsed = round(time.time() - start_time, 2)
        print(f"🎉 Simulation finished successfully in {elapsed}s.")
        print()
        
    except Exception as e:
        print(f"🚨 An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
