"""
Demo DAG showcasing Airflow 3.0 best practices with @dag, @task, and datasets.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.datasets import Dataset

# Define datasets for data lineage
input_dataset = Dataset("demo://input_data")
processed_dataset = Dataset("demo://processed_data")
output_dataset = Dataset("demo://output_data")

@dag(
    dag_id='airflow_3_demo',
    description='Demo DAG showcasing Airflow 3.0 best practices',
    schedule=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['demo', 'airflow3', 'best-practices'],
    owner_links={"demo-team": "mailto:demo@company.com"},
    doc_md=__doc__,
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=2),
    }
)
def airflow_3_demo():
    """
    Demo workflow showcasing Airflow 3.0 patterns.
    
    This DAG demonstrates:
    - @dag decorator usage
    - @task decorator with dataset lineage
    - Modern dependency management
    - Documentation best practices
    """
    
    @task(outlets=[input_dataset])
    def extract_data():
        """Extract data from source."""
        print("Extracting data...")
        return {"records": 100, "status": "extracted"}
    
    @task(inlets=[input_dataset], outlets=[processed_dataset])
    def transform_data(data):
        """Transform the extracted data."""
        print(f"Transforming {data['records']} records...")
        return {"records": data['records'], "status": "transformed"}
    
    @task(inlets=[processed_dataset], outlets=[output_dataset])
    def load_data(data):
        """Load data to destination."""
        print(f"Loading {data['records']} records...")
        return {"records": data['records'], "status": "loaded"}
    
    @task
    def notify_completion(data):
        """Send completion notification."""
        print(f"ETL completed successfully! {data['records']} records processed.")
        return "notification_sent"
    
    # Define the workflow using the new syntax
    extracted = extract_data()
    transformed = transform_data(extracted)
    loaded = load_data(transformed)
    notify_completion(loaded)

# Instantiate the DAG
airflow_3_demo_dag = airflow_3_demo()