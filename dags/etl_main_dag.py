"""
Main ETL DAG for orchestrating CDC, batch, and stream processing.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.datasets import Dataset

# Import custom operators and processors
import sys
import os
sys.path.append('/opt/airflow/src')

from operators.etl_operators import (
    SetupDebeziumConnectorOperator,
    GenerateDataOperator,
    ProcessBatchOperator,
    MonitorCDCOperator,
    MonitorStreamOperator,
    CheckSystemHealthOperator,
    CleanupDataOperator
)

# Define datasets for data lineage (replacing assets in Airflow 3.0)
raw_data_dataset = Dataset("postgres://etl_demo/raw_data")
cdc_events_dataset = Dataset("kafka://etl_cdc_events")
stream_events_dataset = Dataset("kafka://etl_stream_events")
processed_data_dataset = Dataset("postgres://etl_demo/processed_data")

@dag(
    dag_id='etl_main_processing',
    description='Main ETL processing DAG with CDC, batch, and stream processing',
    schedule=timedelta(minutes=60),  # Run every hour
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'cdc', 'batch', 'stream'],
    owner_links={"etl-team": "mailto:etl-team@company.com"},
    doc_md=__doc__,
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
)
def etl_main_processing():
    """
    Main ETL processing workflow with CDC, batch, and stream processing.
    
    This DAG orchestrates the complete ETL pipeline including:
    - Health checks
    - Debezium connector setup
    - Data generation
    - CDC and stream monitoring
    - Batch processing
    - Cleanup operations
    """
    
    @task
    def health_check():
        """Perform initial system health check."""
        from operators.etl_operators import CheckSystemHealthOperator
        operator = CheckSystemHealthOperator(
            task_id='health_check',
            check_components=['debezium', 'cdc', 'stream']
        )
        return operator.execute({})
    
    @task(outlets=[raw_data_dataset])
    def setup_debezium():
        """Setup Debezium connector for CDC."""
        from operators.etl_operators import SetupDebeziumConnectorOperator
        operator = SetupDebeziumConnectorOperator(
            task_id='setup_debezium_connector',
            connector_name='etl-postgres-connector',
            database_conn_id='postgres_etl',
            debezium_conn_id='debezium_default'
        )
        return operator.execute({})
    
    @task(outlets=[raw_data_dataset])
    def generate_test_data():
        """Generate test data for processing."""
        from operators.etl_operators import GenerateDataOperator
        operator = GenerateDataOperator(
            task_id='generate_test_data',
            mode='batch',
            customers=10,
            products=20,
            orders=50
        )
        return operator.execute({})
    
    @task(inlets=[raw_data_dataset], outlets=[cdc_events_dataset])
    def monitor_cdc_processing():
        """Monitor CDC processing in parallel."""
        from operators.etl_operators import MonitorCDCOperator
        operator = MonitorCDCOperator(
            task_id='monitor_cdc_processing',
            timeout_minutes=30
        )
        return operator.execute({})
    
    @task(inlets=[cdc_events_dataset], outlets=[stream_events_dataset])
    def monitor_stream_processing():
        """Monitor stream processing in parallel."""
        from operators.etl_operators import MonitorStreamOperator
        operator = MonitorStreamOperator(
            task_id='monitor_stream_processing',
            timeout_minutes=30
        )
        return operator.execute({})
    
    @task(inlets=[stream_events_dataset], outlets=[processed_data_dataset])
    def process_batch_analytics(ds=None):
        """Process batch analytics."""
        from operators.etl_operators import ProcessBatchOperator
        operator = ProcessBatchOperator(
            task_id='process_batch_analytics',
            analysis_date=ds  # Use execution date
        )
        return operator.execute({})
    
    @task
    def cleanup_old_data():
        """Cleanup old data to maintain performance."""
        from operators.etl_operators import CleanupDataOperator
        operator = CleanupDataOperator(
            task_id='cleanup_old_data',
            database_conn_id='postgres_etl',
            days_to_keep=30
        )
        return operator.execute({})
    
    @task
    def final_health_check():
        """Perform final system health check."""
        from operators.etl_operators import CheckSystemHealthOperator
        operator = CheckSystemHealthOperator(
            task_id='final_health_check',
            check_components=['debezium', 'cdc', 'stream']
        )
        return operator.execute({})
    
    # Define task dependencies using the new syntax
    health_check_result = health_check()
    setup_result = setup_debezium()
    data_result = generate_test_data()
    
    # Parallel processing
    cdc_result = monitor_cdc_processing()
    stream_result = monitor_stream_processing()
    
    # Sequential processing after parallel
    batch_result = process_batch_analytics()
    cleanup_result = cleanup_old_data()
    final_check = final_health_check()
    
    # Set up dependencies
    health_check_result >> setup_result >> data_result
    data_result >> [cdc_result, stream_result]
    [cdc_result, stream_result] >> batch_result >> cleanup_result >> final_check

# Instantiate the DAG
etl_main_processing_dag = etl_main_processing()