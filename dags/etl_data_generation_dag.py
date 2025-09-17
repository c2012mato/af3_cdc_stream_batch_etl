"""
Data generation DAG for continuous data creation.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator

# Import custom operators
import sys
import os
sys.path.append('/opt/airflow/src')

from operators.etl_operators import GenerateDataOperator

# Default arguments
default_args = {
    'owner': 'etl-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'catchup': False
}

# Create the DAG
dag = DAG(
    'etl_data_generation',
    default_args=default_args,
    description='Continuous data generation for ETL testing',
    schedule_interval=timedelta(minutes=15),  # Run every 15 minutes
    max_active_runs=1,
    tags=['etl', 'data-generation', 'testing']
)

# Generate continuous data
generate_continuous_data = GenerateDataOperator(
    task_id='generate_continuous_data',
    mode='continuous',
    duration_minutes=10,  # Run for 10 minutes
    dag=dag
)

generate_continuous_data