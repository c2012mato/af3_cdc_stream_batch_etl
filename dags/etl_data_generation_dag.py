"""
Data generation DAG for continuous data creation.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.datasets import Dataset

# Import custom operators
import sys
import os
sys.path.append('/opt/airflow/src')

from operators.etl_operators import GenerateDataOperator

# Define datasets
generated_data_dataset = Dataset("postgres://etl_demo/generated_data")

@dag(
    dag_id='etl_data_generation',
    description='Continuous data generation for ETL testing',
    schedule=timedelta(minutes=15),  # Run every 15 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'data-generation', 'testing'],
    owner_links={"etl-team": "mailto:etl-team@company.com"},
    doc_md=__doc__,
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=2),
    }
)
def etl_data_generation():
    """
    Continuous data generation workflow for ETL testing.
    
    This DAG generates continuous test data to feed the ETL pipeline.
    """
    
    @task(outlets=[generated_data_dataset])
    def generate_continuous_data():
        """Generate continuous data for testing."""
        from operators.etl_operators import GenerateDataOperator
        operator = GenerateDataOperator(
            task_id='generate_continuous_data',
            mode='continuous',
            duration_minutes=10  # Run for 10 minutes
        )
        return operator.execute({})
    
    # Execute the data generation task
    generate_continuous_data()

# Instantiate the DAG
etl_data_generation_dag = etl_data_generation()