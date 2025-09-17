"""
Custom Airflow operators for ETL processing.
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime

from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.hooks.postgres_hook import PostgresHook

from hooks.debezium_hook import DebeziumHook
from processors.data_generator import DataGenerator
from processors.cdc_processor import CDCProcessor
from processors.batch_processor import BatchProcessor
from processors.stream_processor import StreamProcessor

logger = logging.getLogger(__name__)


class SetupDebeziumConnectorOperator(BaseOperator):
    """Operator to setup Debezium connector."""
    
    template_fields = ['connector_name', 'database_conn_id']
    
    def __init__(self,
                 connector_name: str,
                 database_conn_id: str = 'postgres_etl',
                 debezium_conn_id: str = 'debezium_default',
                 schema_include_list: str = "etl_demo",
                 table_include_list: str = "etl_demo.customers,etl_demo.products,etl_demo.orders,etl_demo.order_items",
                 **kwargs):
        super().__init__(**kwargs)
        self.connector_name = connector_name
        self.database_conn_id = database_conn_id
        self.debezium_conn_id = debezium_conn_id
        self.schema_include_list = schema_include_list
        self.table_include_list = table_include_list
    
    def execute(self, context: Context):
        """Execute the operator."""
        debezium_hook = DebeziumHook(self.debezium_conn_id)
        postgres_hook = PostgresHook(self.database_conn_id)
        
        # Get database connection details
        postgres_conn = postgres_hook.get_connection(self.database_conn_id)
        
        # Check if connector already exists
        existing_connectors = debezium_hook.list_connectors()
        if self.connector_name in existing_connectors:
            logger.info(f"Connector {self.connector_name} already exists, updating configuration")
            
            # Get current configuration
            current_config = debezium_hook.get_connector_config(self.connector_name)
            if current_config:
                # Update the connector
                new_config = debezium_hook.get_postgres_connector_config(
                    connector_name=self.connector_name,
                    database_hostname=postgres_conn.host,
                    database_port=postgres_conn.port or 5432,
                    database_user=postgres_conn.login,
                    database_password=postgres_conn.password,
                    database_dbname=postgres_conn.schema,
                    schema_include_list=self.schema_include_list,
                    table_include_list=self.table_include_list
                )
                
                success = debezium_hook.update_connector(self.connector_name, new_config)
                if not success:
                    raise Exception(f"Failed to update connector {self.connector_name}")
            else:
                raise Exception(f"Could not get current configuration for {self.connector_name}")
        else:
            # Create new connector
            logger.info(f"Creating new connector {self.connector_name}")
            
            connector_config = debezium_hook.get_postgres_connector_config(
                connector_name=self.connector_name,
                database_hostname=postgres_conn.host,
                database_port=postgres_conn.port or 5432,
                database_user=postgres_conn.login,
                database_password=postgres_conn.password,
                database_dbname=postgres_conn.schema,
                schema_include_list=self.schema_include_list,
                table_include_list=self.table_include_list
            )
            
            success = debezium_hook.create_connector(self.connector_name, connector_config)
            if not success:
                raise Exception(f"Failed to create connector {self.connector_name}")
        
        # Wait for connector to be running
        success = debezium_hook.wait_for_connector_status(self.connector_name, "RUNNING", timeout=120)
        if not success:
            raise Exception(f"Connector {self.connector_name} did not reach RUNNING state")
        
        logger.info(f"Debezium connector {self.connector_name} is ready")
        return {"connector_name": self.connector_name, "status": "RUNNING"}


class GenerateDataOperator(BaseOperator):
    """Operator to generate test data."""
    
    template_fields = ['customers', 'products', 'orders']
    
    def __init__(self,
                 mode: str = 'batch',  # 'batch' or 'continuous'
                 customers: int = 50,
                 products: int = 100,
                 orders: int = 200,
                 duration_minutes: int = 10,
                 **kwargs):
        super().__init__(**kwargs)
        self.mode = mode
        self.customers = customers
        self.products = products
        self.orders = orders
        self.duration_minutes = duration_minutes
    
    def execute(self, context: Context):
        """Execute the operator."""
        logger.info(f"Starting data generation in {self.mode} mode")
        
        generator = DataGenerator()
        
        if self.mode == 'batch':
            generator.generate_batch_data(
                customers=self.customers,
                products=self.products,
                orders=self.orders
            )
        else:  # continuous
            generator.generate_continuous_data(duration_minutes=self.duration_minutes)
        
        stats = generator.stats.to_dict()
        logger.info(f"Data generation completed: {stats}")
        return stats


class ProcessBatchOperator(BaseOperator):
    """Operator to process batch analytics."""
    
    template_fields = ['analysis_date']
    
    def __init__(self,
                 analysis_date: Optional[str] = None,  # YYYY-MM-DD format
                 **kwargs):
        super().__init__(**kwargs)
        self.analysis_date = analysis_date
    
    def execute(self, context: Context):
        """Execute the operator."""
        analysis_datetime = None
        if self.analysis_date:
            analysis_datetime = datetime.strptime(self.analysis_date, "%Y-%m-%d")
        
        logger.info(f"Starting batch processing for {analysis_datetime or 'current date'}")
        
        processor = BatchProcessor()
        results = processor.process_batch(analysis_datetime)
        
        logger.info(f"Batch processing completed: {results}")
        return results


class MonitorCDCOperator(BaseOperator):
    """Operator to monitor CDC processing."""
    
    template_fields = ['max_events', 'timeout_minutes']
    
    def __init__(self,
                 max_events: Optional[int] = None,
                 timeout_minutes: int = 30,
                 **kwargs):
        super().__init__(**kwargs)
        self.max_events = max_events
        self.timeout_minutes = timeout_minutes
    
    def execute(self, context: Context):
        """Execute the operator."""
        logger.info(f"Starting CDC monitoring for {self.timeout_minutes} minutes")
        
        processor = CDCProcessor()
        
        # Start processing with timeout
        import threading
        import time
        
        processing_thread = threading.Thread(
            target=processor.run_consumer,
            args=(self.max_events,)
        )
        processing_thread.daemon = True
        processing_thread.start()
        
        # Wait for timeout
        processing_thread.join(timeout=self.timeout_minutes * 60)
        
        if processing_thread.is_alive():
            logger.info(f"CDC monitoring timeout reached ({self.timeout_minutes} minutes)")
        
        metrics = processor.metrics.to_dict()
        logger.info(f"CDC monitoring completed: {metrics}")
        return metrics


class MonitorStreamOperator(BaseOperator):
    """Operator to monitor stream processing."""
    
    template_fields = ['max_events', 'timeout_minutes']
    
    def __init__(self,
                 max_events: Optional[int] = None,
                 timeout_minutes: int = 30,
                 **kwargs):
        super().__init__(**kwargs)
        self.max_events = max_events
        self.timeout_minutes = timeout_minutes
    
    def execute(self, context: Context):
        """Execute the operator."""
        logger.info(f"Starting stream monitoring for {self.timeout_minutes} minutes")
        
        processor = StreamProcessor()
        
        # Start processing with timeout
        import threading
        
        processing_thread = threading.Thread(
            target=processor.run_stream_processor,
            args=(self.max_events,)
        )
        processing_thread.daemon = True
        processing_thread.start()
        
        # Wait for timeout
        processing_thread.join(timeout=self.timeout_minutes * 60)
        
        if processing_thread.is_alive():
            logger.info(f"Stream monitoring timeout reached ({self.timeout_minutes} minutes)")
        
        metrics = processor.metrics.to_dict()
        logger.info(f"Stream monitoring completed: {metrics}")
        return metrics


class CheckSystemHealthOperator(BaseOperator):
    """Operator to check system health."""
    
    def __init__(self,
                 check_components: list = None,
                 **kwargs):
        super().__init__(**kwargs)
        self.check_components = check_components or ['debezium', 'cdc', 'stream']
    
    def execute(self, context: Context):
        """Execute the operator."""
        logger.info("Performing system health check")
        
        health_results = {
            "timestamp": datetime.now().isoformat(),
            "overall_status": "healthy",
            "components": {}
        }
        
        # Check Debezium
        if 'debezium' in self.check_components:
            try:
                debezium_hook = DebeziumHook()
                if debezium_hook.test_connection():
                    connectors = debezium_hook.list_connectors()
                    health_results["components"]["debezium"] = {
                        "status": "healthy",
                        "connectors": connectors
                    }
                else:
                    health_results["components"]["debezium"] = {
                        "status": "unhealthy",
                        "error": "Connection failed"
                    }
                    health_results["overall_status"] = "degraded"
            except Exception as e:
                health_results["components"]["debezium"] = {
                    "status": "unhealthy",
                    "error": str(e)
                }
                health_results["overall_status"] = "degraded"
        
        # Check CDC processor
        if 'cdc' in self.check_components:
            try:
                cdc_processor = CDCProcessor()
                cdc_health = cdc_processor.health_check()
                health_results["components"]["cdc"] = cdc_health
                
                if cdc_health["status"] != "healthy":
                    health_results["overall_status"] = "degraded"
            except Exception as e:
                health_results["components"]["cdc"] = {
                    "status": "unhealthy",
                    "error": str(e)
                }
                health_results["overall_status"] = "degraded"
        
        # Check stream processor
        if 'stream' in self.check_components:
            try:
                stream_processor = StreamProcessor()
                stream_health = stream_processor.health_check()
                health_results["components"]["stream"] = stream_health
                
                if stream_health["status"] != "healthy":
                    health_results["overall_status"] = "degraded"
            except Exception as e:
                health_results["components"]["stream"] = {
                    "status": "unhealthy",
                    "error": str(e)
                }
                health_results["overall_status"] = "degraded"
        
        logger.info(f"Health check completed: {health_results}")
        
        # Fail the task if overall status is not healthy
        if health_results["overall_status"] not in ["healthy", "degraded"]:
            raise Exception(f"System health check failed: {health_results}")
        
        return health_results


class CleanupDataOperator(BaseOperator):
    """Operator to cleanup old data and maintain system performance."""
    
    def __init__(self,
                 database_conn_id: str = 'postgres_etl',
                 days_to_keep: int = 30,
                 **kwargs):
        super().__init__(**kwargs)
        self.database_conn_id = database_conn_id
        self.days_to_keep = days_to_keep
    
    def execute(self, context: Context):
        """Execute the operator."""
        logger.info(f"Starting data cleanup, keeping {self.days_to_keep} days of data")
        
        postgres_hook = PostgresHook(self.database_conn_id)
        
        cleanup_results = {
            "timestamp": datetime.now().isoformat(),
            "tables_cleaned": [],
            "records_deleted": 0
        }
        
        # Define cleanup queries
        cleanup_queries = [
            {
                "table": "product_analytics",
                "query": f"""
                    DELETE FROM etl_demo.product_analytics 
                    WHERE date_analyzed < CURRENT_DATE - INTERVAL '{self.days_to_keep} days'
                """
            },
            {
                "table": "revenue_summary",
                "query": f"""
                    DELETE FROM etl_demo.revenue_summary 
                    WHERE date_analyzed < CURRENT_DATE - INTERVAL '{self.days_to_keep} days'
                """
            }
        ]
        
        try:
            for cleanup in cleanup_queries:
                result = postgres_hook.run(cleanup["query"], autocommit=True)
                cleanup_results["tables_cleaned"].append(cleanup["table"])
                logger.info(f"Cleaned up {cleanup['table']} table")
            
            # Get total record counts for reporting
            total_records_query = """
                SELECT 
                    (SELECT COUNT(*) FROM etl_demo.customers) as customers,
                    (SELECT COUNT(*) FROM etl_demo.products) as products,
                    (SELECT COUNT(*) FROM etl_demo.orders) as orders,
                    (SELECT COUNT(*) FROM etl_demo.order_items) as order_items,
                    (SELECT COUNT(*) FROM etl_demo.product_analytics) as product_analytics,
                    (SELECT COUNT(*) FROM etl_demo.revenue_summary) as revenue_summary
            """
            
            record_counts = postgres_hook.get_first(total_records_query)
            cleanup_results["current_record_counts"] = dict(zip(
                ["customers", "products", "orders", "order_items", "product_analytics", "revenue_summary"],
                record_counts
            ))
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            raise
        
        logger.info(f"Data cleanup completed: {cleanup_results}")
        return cleanup_results