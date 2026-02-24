from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime, timedelta
import boto3
import pandas as pd
import io
import os
import logging

# Slack Webhook Connection ID (stored in Airflow UI)
SLACK_CONN_ID = 'slack_webhook'

def task_fail_slack_alert(context):
    """
    Callback function that sends a Slack notification when a task fails.
    """
    ti = context.get('task_instance')
    dag_id = ti.dag_id
    task_id = ti.task_id
    execution_date = context.get('execution_date')
    log_url = ti.log_url

    slack_msg = f"""
    :red_circle: Task Failed.
    *Dag*: {dag_id}
    *Task*: {task_id}
    *Execution Time*: {execution_date}
    *Log URL*: {log_url}
    """
    
    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=slack_msg,
        username='Airflow-Alert'
    )
    return failed_alert.execute(context=context)

# Configuration for connections
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000') # Use docker service name
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')
BUCKET_NAME = 'raw-sales-data'
POSTGRES_CONN_ID = 'salesdb_conn' # We will need to set this connection up in Airflow UI
PROCESSED_DIR = '/opt/airflow/data/processed' # Inside container

# Ensure processed output directory exists
os.makedirs(PROCESSED_DIR, exist_ok=True)

# Default args for the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'on_failure_callback': task_fail_slack_alert,
}

def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name='us-east-1'
    )

def scan_minio_for_new_files(ti, **kwargs):
    """Task 1: Scans MinIO for CSV files and compares with a tracking mechanism"""
    s3_client = get_s3_client()
    
    try:
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
        if 'Contents' not in response:
            logging.info("No files found in MinIO bucket.")
            return []
            
        all_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.csv')]
        
        # Pull processed files tracking from XCom or a local tracker file to determine "new" files.
        # For this simplified pipeline, we'll assume we process all files we see and rely on 
        # PostgreSQL's ON CONFLICT to handle deduplication at the database level.
        # In a production environment, you'd maintain a state table of processed files.
        
        logging.info(f"Identified {len(all_files)} total files in MinIO.")
        
        # Explicitly push file list for visibility in XComs
        ti.xcom_push(key='file_count', value=len(all_files))
        ti.xcom_push(key='file_list', value=all_files)
        
        return all_files
        
    except Exception as e:
        logging.error(f"Error scanning MinIO: {e}")
        raise

def download_and_validate(ti, **kwargs):
    """Task 2: Downloads files from MinIO and validates schema."""
    files_to_process = ti.xcom_pull(task_ids='scan_minio')
    
    if not files_to_process:
        logging.info("No files to process.")
        return []

    s3_client = get_s3_client()
    valid_files_data = []

    expected_columns = [
        'transaction_id', 'transaction_date', 'customer_id', 'customer_name', 
        'salesperson_id', 'salesperson_name', 'product_id', 'product_name', 
        'category', 'quantity', 'unit_price', 'total_amount', 'store_location'
    ]

    for file_key in files_to_process:
        logging.info(f"Downloading {file_key}")
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
        
        # Read the CSV content directly into a pandas DataFrame
        csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(io.StringIO(csv_content))
        
        # Schema Validation
        if list(df.columns) == expected_columns:
            logging.info(f"✅ Schema validation passed for {file_key}")
            # Pass the dataframe JSON representation downstream
            valid_files_data.append({
                'filename': file_key,
                'data': df.to_json(orient='records')
            })
        else:
            logging.warning(f"❌ Schema validation failed for {file_key}. Expected {expected_columns}, got {list(df.columns)}")

    return valid_files_data

def clean_data(ti, **kwargs):
    """Task 3: Cleans the data (fixes types, drops bad rows)."""
    valid_files = ti.xcom_pull(task_ids='download_and_validate')
    
    if not valid_files:
        logging.info("No valid data to clean.")
        return []

    cleaned_dataframes = []

    for file_info in valid_files:
        filename = file_info['filename']
        df = pd.read_json(io.StringIO(file_info['data']), orient='records')
        
        initial_rows = len(df)
        logging.info(f"Cleaning {filename} (Initial rows: {initial_rows})")
        
        # 1. Drop complete duplicates
        df.drop_duplicates(inplace=True)
        
        # 2. Fix quantity - sometimes it's string "X units" from our dirty generator
        # Force to numeric and coerce errors to NaN, then drop NaNs
        df['quantity'] = pd.to_numeric(df['quantity'].astype(str).str.replace(r'\D', '', regex=True), errors='coerce')
        
        # 3. Handle anomalies: Drop rows where customer name is missing, location is missing, or total is negative
        # In a real environment, you might Impute instead of Drop, or send to a Dead Letter Queue.
        df.dropna(subset=['customer_name', 'store_location', 'quantity'], inplace=True)
        df = df[df['total_amount'] >= 0]
        
        # 4. Enforce strict data types before database insertion
        df['quantity'] = df['quantity'].astype(int)
        df['unit_price'] = df['unit_price'].astype(float)
        df['total_amount'] = df['total_amount'].astype(float)
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])

        final_rows = len(df)
        logging.info(f"✅ Finished cleaning {filename}. Removed {initial_rows - final_rows} bad/duplicate rows. Clean rows: {final_rows}")
        
        # Push metrics to XCom for monitoring
        ti.xcom_push(key=f'{filename}_initial_rows', value=initial_rows)
        ti.xcom_push(key=f'{filename}_clean_rows', value=final_rows)
        ti.xcom_push(key=f'{filename}_dropped_rows', value=initial_rows - final_rows)

        # Save a local cache of the processed file
        processed_filepath = os.path.join(PROCESSED_DIR, f"clean_{filename}")
        df.to_csv(processed_filepath, index=False)
        
        cleaned_dataframes.append({
            'filename': filename,
            'clean_filepath': processed_filepath
        })

    return cleaned_dataframes

def load_to_postgres(ti, **kwargs):
    """Task 4: Inserts clean rows into PostgreSQL using ON CONFLICT DO NOTHING."""
    cleaned_files = ti.xcom_pull(task_ids='clean_data')
    
    if not cleaned_files:
        logging.info("No clean data to load into Postgres.")
        return

    # Initialize PostgresHook
    try:
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        logging.info(f"Connected to PostgreSQL using connection ID: {POSTGRES_CONN_ID}")
    except Exception as e:
        logging.error(f"Failed connecting to Postgres: Ensure the connection '{POSTGRES_CONN_ID}' exists in Airflow UI. Error: {e}")
        raise

    # 1. Ensure target table exists mapping perfectly to our schema
    create_table_query = """
    CREATE TABLE IF NOT EXISTS sales (
        transaction_id VARCHAR(50) PRIMARY KEY,
        transaction_date TIMESTAMP NOT NULL,
        customer_id VARCHAR(50) NOT NULL,
        customer_name VARCHAR(100) NOT NULL,
        salesperson_id VARCHAR(50) NOT NULL,
        salesperson_name VARCHAR(100) NOT NULL,
        product_id VARCHAR(50) NOT NULL,
        product_name VARCHAR(100) NOT NULL,
        category VARCHAR(50) NOT NULL,
        quantity INTEGER NOT NULL,
        unit_price DECIMAL(10, 2) NOT NULL,
        total_amount DECIMAL(10, 2) NOT NULL,
        store_location VARCHAR(100) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        cursor.execute(create_table_query)
        conn.commit()
        logging.info("Verified destination table 'sales' exists.")
    except Exception as e:
        logging.error(f"Failed to create table: {e}")
        conn.rollback()
        raise

    total_inserted = 0

    for file_info in cleaned_files:
        filepath = file_info['clean_filepath']
        logging.info(f"Loading {filepath} into Database...")
        
        df = pd.read_csv(filepath)
        
        # Convert DataFrame to list of tuples for psycopg2 insertion
        # We replace NaNs with None so psycopg2 can translate it to SQL NULL
        df = df.where(pd.notnull(df), None)
        records = [tuple(x) for x in df.to_numpy()]

        # The UPSERT logic (ON CONFLICT DO NOTHING) ensuring idempotency
        insert_query = """
            INSERT INTO sales (
                transaction_id, transaction_date, customer_id, customer_name,
                salesperson_id, salesperson_name, product_id, product_name,
                category, quantity, unit_price, total_amount, store_location
            ) VALUES %s
            ON CONFLICT (transaction_id) DO NOTHING;
        """

        # We use execute_values for efficient bulk loading
        try:
            from psycopg2.extras import execute_values
            conn = pg_hook.get_conn()
            cursor = conn.cursor()
            execute_values(cursor, insert_query, records)
            conn.commit()
            
            inserted_count = cursor.rowcount
            total_inserted += inserted_count
            logging.info(f"✅ Upserted {len(records)} records from {file_info['filename']} ({inserted_count} new rows inserted).")
            # Push specific file metrics
            ti.xcom_push(key=f"{file_info['filename']}_inserted", value=inserted_count)
            
        except Exception as e:
            logging.error(f"Database insertion failed for {file_info['filename']}: {e}")
            conn.rollback()
            raise
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()

    logging.info(f"🎉 Pipeline finished. Total new rows successfully inserted across all files: {total_inserted}")
    ti.xcom_push(key='total_upserted_rows', value=total_inserted)

# Define the DAG
with DAG(
    'sales_data_pipeline',
    default_args=default_args,
    description='ETL pipeline taking sales CSVs from MinIO, cleaning them, and loading to PostgreSQL',
    schedule_interval='@daily', 
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sales', 'etl', 'minio', 'postgres'],
) as dag:

    # Task 1
    t1_scan_minio = PythonOperator(
        task_id='scan_minio',
        python_callable=scan_minio_for_new_files,
        provide_context=True
    )

    # Task 2
    t2_download_and_validate = PythonOperator(
        task_id='download_and_validate',
        python_callable=download_and_validate,
        provide_context=True
    )

    # Task 3
    t3_clean_data = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data,
        provide_context=True
    )

    # Task 4
    t4_load_postgres = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        provide_context=True
    )

    # Define execution order
    t1_scan_minio >> t2_download_and_validate >> t3_clean_data >> t4_load_postgres
