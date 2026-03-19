import pytest
from datetime import datetime
from airflow.dags.data_generation_dag import generate_sale, generate_month, PRODUCTS, CUSTOMERS, SALESPERSONS, STORE_LOCATIONS

def test_generate_sale_structure():
    """Test that generate_sale returns a list with 13 elements representing a row."""
    sale_record = generate_sale()
    assert isinstance(sale_record, list)
    assert len(sale_record) == 13

def test_generate_sale_data_types():
    """Test standard data types for a clean sale record."""
    # We might occasionally get a dirty record, so we test standard types with allowance for dirty strings
    sale_record = generate_sale()
    
    transaction_id = sale_record[0]
    transaction_date = sale_record[1]
    quantity = sale_record[9]
    total_amount = sale_record[11]

    # transaction_id should be a string (UUID)
    assert isinstance(transaction_id, str)
    assert len(transaction_id) > 20
    
    # Date should be an ISO format string parsable by datetime
    assert isinstance(datetime.fromisoformat(transaction_date), datetime)
    
    # Depending on dirty injection, quantity might be int or str
    assert isinstance(quantity, (int, str))
    
    # Total amount should be float
    assert isinstance(total_amount, float)

def test_generate_month_count():
    """Test that generate_month returns at least the exact number of requested rows (possibly more if duplicated)."""
    num_requested = 50
    records = generate_month(num_requested)
    
    # Should be at least 50. It might be slightly more due to the 5% duplication chance.
    assert len(records) >= num_requested
    
    # Check that it's a list of lists
    assert isinstance(records[0], list)
    
def test_generate_sale_contains_valid_master_data():
    """Test that the generated data pulls from the master lists properly."""
    sale_record = generate_sale()
    product_name = sale_record[7]
    customer_name = sale_record[3]
    salesperson_name = sale_record[5]
    
    # Validate against master lists (allowing for missing customer_name due to dirty injection)
    product_names = [p['name'] for p in PRODUCTS]
    customer_names = [c['name'] for c in CUSTOMERS] + [""]
    salesperson_names = [s['name'] for s in SALESPERSONS]
    
    assert product_name in product_names
    assert customer_name in customer_names
    assert salesperson_name in salesperson_names
