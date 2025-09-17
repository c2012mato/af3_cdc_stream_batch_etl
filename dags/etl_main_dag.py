"""
Main ETL DAG for orchestrating CDC, batch, and stream processing.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Import custom operators
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

# Default arguments
default_args = {
    'owner': 'etl-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Create the DAG
dag = DAG(
    'etl_main_processing',
    default_args=default_args,
    description='Main ETL processing DAG with CDC, batch, and stream processing',
    schedule_interval=timedelta(minutes=60),  # Run every hour
    max_active_runs=1,
    tags=['etl', 'cdc', 'batch', 'stream']
)

# Task 1: Health check
health_check = CheckSystemHealthOperator(
    task_id='health_check',
    check_components=['debezium', 'cdc', 'stream'],
    dag=dag
)

# Task 2: Setup Debezium connector
setup_debezium = SetupDebeziumConnectorOperator(
    task_id='setup_debezium_connector',
    connector_name='etl-postgres-connector',
    database_conn_id='postgres_etl',
    debezium_conn_id='debezium_default',
    dag=dag
)

# Task 3: Generate some test data
generate_data = GenerateDataOperator(
    task_id='generate_test_data',
    mode='batch',
    customers=10,
    products=20,
    orders=50,
    dag=dag
)

# Task 4: Start CDC monitoring (parallel)
monitor_cdc = MonitorCDCOperator(
    task_id='monitor_cdc_processing',
    timeout_minutes=30,
    dag=dag
)

# Task 5: Start stream monitoring (parallel)
monitor_stream = MonitorStreamOperator(
    task_id='monitor_stream_processing',
    timeout_minutes=30,
    dag=dag
)

# Task 6: Process batch analytics
process_batch = ProcessBatchOperator(
    task_id='process_batch_analytics',
    analysis_date='{{ ds }}',  # Use execution date
    dag=dag
)

# Task 7: Cleanup old data (weekly)
cleanup_data = CleanupDataOperator(
    task_id='cleanup_old_data',
    database_conn_id='postgres_etl',
    days_to_keep=30,
    dag=dag
)

# Task 8: Final health check
final_health_check = CheckSystemHealthOperator(
    task_id='final_health_check',
    check_components=['debezium', 'cdc', 'stream'],
    dag=dag
)

# Dummy tasks for organization
start_parallel = DummyOperator(
    task_id='start_parallel_processing',
    dag=dag
)

end_parallel = DummyOperator(
    task_id='end_parallel_processing',
    dag=dag
)

# Define task dependencies
health_check >> setup_debezium >> generate_data >> start_parallel

# Parallel processing
start_parallel >> [monitor_cdc, monitor_stream]
[monitor_cdc, monitor_stream] >> end_parallel

# Sequential processing after parallel
end_parallel >> process_batch >> cleanup_data >> final_health_check