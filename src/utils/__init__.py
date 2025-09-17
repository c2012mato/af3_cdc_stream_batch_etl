"""
Utility functions for the ETL system.
"""

import logging
import json
from datetime import datetime
from typing import Dict, Any, Optional
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

from config import config

logger = logging.getLogger(__name__)


def get_database_connection():
    """Get a database connection."""
    return psycopg2.connect(
        config.database.connection_string,
        cursor_factory=RealDictCursor
    )


def execute_query(query: str, params: Optional[tuple] = None) -> list:
    """Execute a query and return results."""
    try:
        with get_database_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        raise


def get_table_row_count(table_name: str, schema: str = "etl_demo") -> int:
    """Get row count for a table."""
    try:
        query = f"SELECT COUNT(*) as count FROM {schema}.{table_name}"
        result = execute_query(query)
        return result[0]['count'] if result else 0
    except Exception as e:
        logger.error(f"Error getting row count for {schema}.{table_name}: {e}")
        return 0


def get_system_stats() -> Dict[str, Any]:
    """Get overall system statistics."""
    stats = {
        "timestamp": datetime.now().isoformat(),
        "table_counts": {},
        "recent_activity": {}
    }
    
    # Get table counts
    tables = ["customers", "products", "orders", "order_items", 
              "customer_segments", "product_analytics", "revenue_summary"]
    
    for table in tables:
        stats["table_counts"][table] = get_table_row_count(table)
    
    # Get recent activity
    try:
        # Recent orders (last 24 hours)
        recent_orders_query = """
            SELECT COUNT(*) as count, COALESCE(SUM(total_amount), 0) as total_revenue
            FROM etl_demo.orders 
            WHERE created_at >= NOW() - INTERVAL '24 hours'
        """
        recent_orders = execute_query(recent_orders_query)
        if recent_orders:
            stats["recent_activity"]["orders_24h"] = recent_orders[0]['count']
            stats["recent_activity"]["revenue_24h"] = float(recent_orders[0]['total_revenue'])
        
        # Recent customers (last 24 hours)
        recent_customers_query = """
            SELECT COUNT(*) as count
            FROM etl_demo.customers 
            WHERE created_at >= NOW() - INTERVAL '24 hours'
        """
        recent_customers = execute_query(recent_customers_query)
        if recent_customers:
            stats["recent_activity"]["new_customers_24h"] = recent_customers[0]['count']
    
    except Exception as e:
        logger.error(f"Error getting recent activity: {e}")
        stats["recent_activity"]["error"] = str(e)
    
    return stats


def format_currency(amount: float) -> str:
    """Format currency amount."""
    return f"${amount:,.2f}"


def format_number(number: int) -> str:
    """Format number with commas."""
    return f"{number:,}"


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Safely divide two numbers."""
    return numerator / denominator if denominator != 0 else default


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def safe_json_dumps(data: Any) -> str:
    """Safely serialize data to JSON."""
    try:
        return json.dumps(data, default=json_serial, indent=2)
    except Exception as e:
        logger.error(f"Error serializing to JSON: {e}")
        return json.dumps({"error": str(e)})


def dataframe_to_dict_list(df: pd.DataFrame) -> list:
    """Convert pandas DataFrame to list of dictionaries."""
    return df.to_dict('records')


def calculate_percentage_change(old_value: float, new_value: float) -> float:
    """Calculate percentage change between two values."""
    if old_value == 0:
        return 100.0 if new_value > 0 else 0.0
    return ((new_value - old_value) / old_value) * 100


def setup_logging(level: str = "INFO", format_string: str = None):
    """Setup logging configuration."""
    if format_string is None:
        format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=format_string,
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def validate_config():
    """Validate configuration settings."""
    errors = []
    
    # Check database configuration
    try:
        with get_database_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
    except Exception as e:
        errors.append(f"Database connection failed: {e}")
    
    # Check Redis configuration
    try:
        import redis
        redis_client = redis.Redis(
            host=config.redis.host,
            port=config.redis.port,
            password=config.redis.password or None,
            db=config.redis.db
        )
        redis_client.ping()
    except Exception as e:
        errors.append(f"Redis connection failed: {e}")
    
    # Check Kafka configuration
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=config.kafka.bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        # Just create the producer, don't send anything
        producer.close()
    except Exception as e:
        errors.append(f"Kafka connection failed: {e}")
    
    if errors:
        raise Exception(f"Configuration validation failed: {', '.join(errors)}")
    
    logger.info("Configuration validation passed")


class ETLMetrics:
    """Utility class for ETL metrics collection."""
    
    def __init__(self):
        self.metrics = {}
        self.start_time = datetime.now()
    
    def record_metric(self, name: str, value: Any):
        """Record a metric value."""
        self.metrics[name] = {
            "value": value,
            "timestamp": datetime.now().isoformat()
        }
    
    def increment_counter(self, name: str, amount: int = 1):
        """Increment a counter metric."""
        if name not in self.metrics:
            self.metrics[name] = {"value": 0, "timestamp": datetime.now().isoformat()}
        self.metrics[name]["value"] += amount
        self.metrics[name]["timestamp"] = datetime.now().isoformat()
    
    def get_runtime(self) -> float:
        """Get runtime in seconds."""
        return (datetime.now() - self.start_time).total_seconds()
    
    def get_all_metrics(self) -> Dict[str, Any]:
        """Get all recorded metrics."""
        return {
            "start_time": self.start_time.isoformat(),
            "runtime_seconds": self.get_runtime(),
            "metrics": self.metrics
        }
    
    def to_json(self) -> str:
        """Convert metrics to JSON string."""
        return safe_json_dumps(self.get_all_metrics())


def create_summary_report(title: str, data: Dict[str, Any]) -> str:
    """Create a formatted summary report."""
    report = [
        "=" * 50,
        f" {title}",
        "=" * 50,
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        ""
    ]
    
    def format_section(section_name: str, section_data: Any, indent: int = 0):
        indent_str = "  " * indent
        if isinstance(section_data, dict):
            report.append(f"{indent_str}{section_name}:")
            for key, value in section_data.items():
                if isinstance(value, dict):
                    format_section(key, value, indent + 1)
                else:
                    report.append(f"{indent_str}  {key}: {value}")
        else:
            report.append(f"{indent_str}{section_name}: {section_data}")
        
        if indent == 0:
            report.append("")
    
    for key, value in data.items():
        format_section(key, value)
    
    report.append("=" * 50)
    return "\n".join(report)