import os
import sys
import logging
import boto3
import psycopg2
from botocore.exceptions import ClientError

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

def test_minio_connection():
    """Verify that MinIO is reachable and the bucket exists."""
    endpoint = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
    access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
    secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')
    bucket = 'raw-sales-data'

    logging.info(f"Testing MinIO connection at {endpoint}...")
    try:
        s3 = boto3.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
        s3.list_objects_v2(Bucket=bucket)
        logging.info("✅ MinIO connection and bucket access: SUCCESS")
        return True
    except Exception as e:
        logging.error(f"❌ MinIO connection failed: {e}")
        return False

def test_postgres_connection():
    """Verify that PostgreSQL is reachable and the database is accessible."""
    host = os.getenv('DB_HOST', 'localhost')
    port = os.getenv('DB_PORT', '5432')
    user = os.getenv('DB_USER', 'airflow')
    password = os.getenv('DB_PASSWORD', 'airflow')
    db = os.getenv('DB_NAME', 'airflow')

    logging.info(f"Testing PostgreSQL connection at {host}:{port}...")
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=db,
            connect_timeout=5
        )
        conn.close()
        logging.info("✅ PostgreSQL connection: SUCCESS")
        return True
    except Exception as e:
        logging.error(f"❌ PostgreSQL connection failed: {e}")
        return False

def main():
    success = True
    if not test_minio_connection():
        success = False
    if not test_postgres_connection():
        success = False
    
    if not success:
        logging.error("❌ Data flow validation FAILED")
        sys.exit(1)
    
    logging.info("🎉 All data flow components are reachable!")

if __name__ == "__main__":
    main()
