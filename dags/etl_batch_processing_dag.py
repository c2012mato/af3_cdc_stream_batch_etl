"""
Batch processing DAG for daily analytics.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator

# Import custom operators
import sys
import os
sys.path.append('/opt/airflow/src')

from operators.etl_operators import ProcessBatchOperator, CheckSystemHealthOperator

# Default arguments
default_args = {
    'owner': 'etl-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': True  # Enable catchup for historical processing
}

# Create the DAG
dag = DAG(
    'etl_batch_processing',
    default_args=default_args,
    description='Daily batch processing for analytics and reporting',
    schedule_interval='0 1 * * *',  # Run daily at 1 AM
    max_active_runs=1,
    tags=['etl', 'batch', 'analytics']
)

# Pre-processing health check
pre_health_check = CheckSystemHealthOperator(
    task_id='pre_processing_health_check',
    check_components=['debezium', 'cdc'],
    dag=dag
)

# Process batch analytics for the previous day
process_batch = ProcessBatchOperator(
    task_id='process_daily_batch',
    analysis_date='{{ ds }}',  # Use execution date
    dag=dag
)

# Post-processing health check
post_health_check = CheckSystemHealthOperator(
    task_id='post_processing_health_check',
    check_components=['debezium', 'cdc'],
    dag=dag
)

# Define task dependencies
pre_health_check >> process_batch >> post_health_check