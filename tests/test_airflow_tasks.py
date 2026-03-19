import pytest
import pandas as pd
import json
from unittest.mock import MagicMock
from airflow.dags.sales_data_pipeline import clean_data, download_and_validate, scan_minio_for_new_files

@pytest.fixture
def mock_ti():
    """Fixture to mock Airflow TaskInstance"""
    ti = MagicMock()
    # Mock xcom_pull to return specific data depending on task_ids
    return ti

def test_clean_data(mock_ti):
    """Test the clean_data function filters anomalies and fixes types."""
    # Simulating data as passed via XCom from download_and_validate
    dirty_data = [
        {
            "transaction_id": "T001", "transaction_date": "2024-01-01 10:00:00",
            "customer_id": "C001", "customer_name": "Alice",
            "salesperson_id": "S001", "salesperson_name": "Bob",
            "product_id": "P001", "product_name": "Widget", "category": "A",
            "quantity": "10 units",  # Dirty quantity
            "unit_price": 5.0, "total_amount": 50.0, "store_location": "NY"
        },
        {
            "transaction_id": "T002", "transaction_date": "2024-01-01 10:00:00",
            "customer_id": "C002", "customer_name": None, # Missing customer name
            "salesperson_id": "S001", "salesperson_name": "Bob",
            "product_id": "P002", "product_name": "Gadget", "category": "B",
            "quantity": 5, "unit_price": 10.0, "total_amount": 50.0, "store_location": "LA"
        },
        {
            "transaction_id": "T003", "transaction_date": "2024-01-01 10:00:00",
            "customer_id": "C003", "customer_name": "Charlie",
            "salesperson_id": "S001", "salesperson_name": "Bob",
            "product_id": "P003", "product_name": "Thing", "category": "C",
            "quantity": 2, "unit_price": 10.0, "total_amount": -20.0, # Negative total
            "store_location": "CHI"
        }
    ]
    
    # Mock what comes from xcom_pull
    valid_files_input = [{
        'filename': 'test_data.csv',
        'data': json.dumps(dirty_data)
    }]
    mock_ti.xcom_pull.return_value = valid_files_input
    
    # Run the function
    result = clean_data(ti=mock_ti)
    
    assert len(result) == 1
    clean_filepath = result[0]['clean_filepath']
    
    # Read the cleaned CSV saved to disk
    df_clean = pd.read_csv(clean_filepath)
    
    # Assertions
    # 1. Row count should be 1 (T001 is fixed, T002 dropped for missing name, T003 dropped for negative amount)
    assert len(df_clean) == 1
    
    # 2. Assert 'quantity' was cleaned from "10 units" to 10
    assert df_clean.iloc[0]['quantity'] == 10
    
    # 3. Assert correct strict typing
    assert pd.api.types.is_numeric_dtype(df_clean['quantity'])
    assert pd.api.types.is_float_dtype(df_clean['unit_price'])
    assert pd.api.types.is_float_dtype(df_clean['total_amount'])

def test_download_and_validate_success(mocker, mock_ti):
    """Test validation succeeds when given correct columns."""
    
    # Mock the XCom pull to provide a filename
    mock_ti.xcom_pull.return_value = ['test_valid.csv']
    
    # Mock boto3 client
    mock_s3 = MagicMock()
    mocker.patch('airflow.dags.sales_data_pipeline.get_s3_client', return_value=mock_s3)
    
    # Mock S3 response body with a valid schema string
    mock_body = MagicMock()
    # Provide expected columns (comma separated) with one data row
    valid_csv = "transaction_id,transaction_date,customer_id,customer_name,salesperson_id,salesperson_name,product_id,product_name,category,quantity,unit_price,total_amount,store_location\nT1,2024-01-01,C1,Name,S1,SName,P1,PName,Cat,1,10.0,10.0,Loc\n"
    mock_body.read.return_value = valid_csv.encode('utf-8')
    mock_s3.get_object.return_value = {'Body': mock_body}
    
    results = download_and_validate(ti=mock_ti)
    
    assert len(results) == 1
    assert results[0]['filename'] == 'test_valid.csv'
    
def test_download_and_validate_failure(mocker, mock_ti):
    """Test validation fails when columns are wrong."""
    mock_ti.xcom_pull.return_value = ['test_invalid.csv']
    
    mock_s3 = MagicMock()
    mocker.patch('airflow.dags.sales_data_pipeline.get_s3_client', return_value=mock_s3)
    
    mock_body = MagicMock()
    # Provide bad columns
    invalid_csv = "wrong_id,bad_date,customer\n1,2,3\n"
    mock_body.read.return_value = invalid_csv.encode('utf-8')
    mock_s3.get_object.return_value = {'Body': mock_body}
    
    results = download_and_validate(ti=mock_ti)
    
    # Empty list expected since the file fails validation
    assert len(results) == 0

def test_scan_minio_for_new_files(mocker, mock_ti):
    """Test scanning MinIO bucket for correctly formatted CSVs."""
    mock_s3 = MagicMock()
    mocker.patch('airflow.dags.sales_data_pipeline.get_s3_client', return_value=mock_s3)
    
    # Mock the list_objects_v2 response to return a mix of CSV and non-CSV files
    mock_response = {
        'Contents': [
            {'Key': 'sales_data_1.csv'},
            {'Key': 'sales_data_2.csv'},
            {'Key': 'ignore_me.txt'},
            {'Key': 'metadata.json'}
        ]
    }
    mock_s3.list_objects_v2.return_value = mock_response
    
    results = scan_minio_for_new_files(ti=mock_ti)
    
    # Should only return the .csv files
    assert len(results) == 2
    assert 'sales_data_1.csv' in results
    assert 'sales_data_2.csv' in results
    
    # Verify it pushed the correct counts to XCom
    mock_ti.xcom_push.assert_any_call(key='file_count', value=2)
