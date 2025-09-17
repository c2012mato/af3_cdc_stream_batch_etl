"""
Batch processing DAG for daily analytics.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.datasets import Dataset

# Import custom operators
import sys
import os
sys.path.append('/opt/airflow/src')

from operators.etl_operators import ProcessBatchOperator, CheckSystemHealthOperator

# Define datasets
batch_input_dataset = Dataset("postgres://etl_demo/daily_batch_input")
batch_output_dataset = Dataset("postgres://etl_demo/daily_batch_output")

@dag(
    dag_id='etl_batch_processing',
    description='Daily batch processing for analytics and reporting',
    schedule='0 1 * * *',  # Run daily at 1 AM
    start_date=datetime(2024, 1, 1),
    catchup=True,  # Enable catchup for historical processing
    max_active_runs=1,
    tags=['etl', 'batch', 'analytics'],
    owner_links={"etl-team": "mailto:etl-team@company.com"},
    doc_md=__doc__,
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    }
)
def etl_batch_processing():
    """
    Daily batch processing workflow for analytics and reporting.
    
    This DAG performs:
    - Pre-processing health check
    - Daily batch analytics processing
    - Post-processing health check
    """
    
    @task
    def pre_processing_health_check():
        """Perform pre-processing health check."""
        from operators.etl_operators import CheckSystemHealthOperator
        operator = CheckSystemHealthOperator(
            task_id='pre_processing_health_check',
            check_components=['debezium', 'cdc']
        )
        return operator.execute({})
    
    @task(inlets=[batch_input_dataset], outlets=[batch_output_dataset])
    def process_daily_batch(ds=None):
        """Process batch analytics for the previous day."""
        from operators.etl_operators import ProcessBatchOperator
        operator = ProcessBatchOperator(
            task_id='process_daily_batch',
            analysis_date=ds  # Use execution date
        )
        return operator.execute({})
    
    @task
    def post_processing_health_check():
        """Perform post-processing health check."""
        from operators.etl_operators import CheckSystemHealthOperator
        operator = CheckSystemHealthOperator(
            task_id='post_processing_health_check',
            check_components=['debezium', 'cdc']
        )
        return operator.execute({})
    
    # Define task dependencies
    pre_check = pre_processing_health_check()
    batch_process = process_daily_batch()
    post_check = post_processing_health_check()
    
    pre_check >> batch_process >> post_check

# Instantiate the DAG
etl_batch_processing_dag = etl_batch_processing()