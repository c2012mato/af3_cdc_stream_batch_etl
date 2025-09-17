"""
CDC Processor for handling Change Data Capture events from Debezium.
Includes CRUD operations, caching, and metrics collection.
"""

import json
import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict
from enum import Enum
import redis
import psycopg2
from kafka import KafkaConsumer, KafkaProducer
from prometheus_client import Counter, Histogram, Gauge

from config import config

logger = logging.getLogger(__name__)


class CDCOperation(Enum):
    """CDC operation types."""
    CREATE = "c"
    UPDATE = "u"
    DELETE = "d"
    READ = "r"  # Initial snapshot


@dataclass
class CDCEvent:
    """CDC event data structure."""
    operation: str
    table: str
    before: Optional[Dict[str, Any]]
    after: Optional[Dict[str, Any]]
    timestamp: datetime
    source_lsn: Optional[str] = None
    transaction_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            **asdict(self),
            "timestamp": self.timestamp.isoformat()
        }
    
    @classmethod
    def from_debezium_record(cls, record: Dict[str, Any]) -> 'CDCEvent':
        """Create CDCEvent from Debezium record."""
        payload = record.get("payload", {})
        source = payload.get("source", {})
        
        return cls(
            operation=payload.get("op", ""),
            table=source.get("table", ""),
            before=payload.get("before"),
            after=payload.get("after"),
            timestamp=datetime.fromtimestamp(payload.get("ts_ms", 0) / 1000),
            source_lsn=source.get("lsn"),
            transaction_id=source.get("txId")
        )


@dataclass
class ProcessingMetrics:
    """Metrics for CDC processing."""
    events_processed: int = 0
    events_cached: int = 0
    events_failed: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    processing_time_total: float = 0.0
    start_time: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        runtime = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
        return {
            **asdict(self),
            "runtime_seconds": runtime,
            "events_per_second": self.events_processed / runtime if runtime > 0 else 0,
            "avg_processing_time_ms": (self.processing_time_total / self.events_processed * 1000) 
                                    if self.events_processed > 0 else 0
        }


class CDCProcessor:
    """Process CDC events with caching and metrics."""
    
    def __init__(self):
        self.database_config = config.database
        self.kafka_config = config.kafka
        self.redis_config = config.redis
        
        # Initialize components
        self.redis_client = self._create_redis_client()
        self.kafka_consumer = self._create_kafka_consumer()
        self.kafka_producer = self._create_kafka_producer()
        
        # Metrics
        self.metrics = ProcessingMetrics(start_time=datetime.now())
        self._setup_prometheus_metrics()
        
        # Cache settings
        self.cache_ttl = 3600  # 1 hour
        self.cache_prefix = "cdc:"
        
    def _create_redis_client(self) -> redis.Redis:
        """Create Redis client for caching."""
        return redis.Redis(
            host=self.redis_config.host,
            port=self.redis_config.port,
            password=self.redis_config.password or None,
            db=self.redis_config.db,
            decode_responses=True
        )
    
    def _create_kafka_consumer(self) -> KafkaConsumer:
        """Create Kafka consumer for CDC events."""
        return KafkaConsumer(
            self.kafka_config.cdc_topic,
            bootstrap_servers=self.kafka_config.bootstrap_servers,
            group_id=self.kafka_config.consumer_group,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
    
    def _create_kafka_producer(self) -> KafkaProducer:
        """Create Kafka producer for processed events."""
        return KafkaProducer(
            bootstrap_servers=self.kafka_config.bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
    
    def _setup_prometheus_metrics(self):
        """Setup Prometheus metrics."""
        self.prometheus_events_processed = Counter(
            'cdc_events_processed_total',
            'Total number of CDC events processed',
            ['table', 'operation']
        )
        self.prometheus_processing_time = Histogram(
            'cdc_processing_duration_seconds',
            'Time spent processing CDC events',
            ['table', 'operation']
        )
        self.prometheus_cache_operations = Counter(
            'cdc_cache_operations_total',
            'Total cache operations',
            ['operation', 'result']
        )
        self.prometheus_active_consumers = Gauge(
            'cdc_active_consumers',
            'Number of active CDC consumers'
        )
    
    def get_database_connection(self):
        """Get database connection."""
        return psycopg2.connect(self.database_config.connection_string)
    
    def cache_key(self, table: str, record_id: Any) -> str:
        """Generate cache key for a record."""
        return f"{self.cache_prefix}{table}:{record_id}"
    
    def get_from_cache(self, table: str, record_id: Any) -> Optional[Dict[str, Any]]:
        """Get record from cache."""
        try:
            key = self.cache_key(table, record_id)
            cached_data = self.redis_client.get(key)
            
            if cached_data:
                self.metrics.cache_hits += 1
                self.prometheus_cache_operations.labels(operation='get', result='hit').inc()
                return json.loads(cached_data)
            else:
                self.metrics.cache_misses += 1
                self.prometheus_cache_operations.labels(operation='get', result='miss').inc()
                return None
                
        except Exception as e:
            logger.error(f"Cache get error for {table}:{record_id}: {e}")
            return None
    
    def set_in_cache(self, table: str, record_id: Any, data: Dict[str, Any]) -> bool:
        """Set record in cache."""
        try:
            key = self.cache_key(table, record_id)
            self.redis_client.setex(key, self.cache_ttl, json.dumps(data))
            self.metrics.events_cached += 1
            self.prometheus_cache_operations.labels(operation='set', result='success').inc()
            return True
            
        except Exception as e:
            logger.error(f"Cache set error for {table}:{record_id}: {e}")
            self.prometheus_cache_operations.labels(operation='set', result='error').inc()
            return False
    
    def delete_from_cache(self, table: str, record_id: Any) -> bool:
        """Delete record from cache."""
        try:
            key = self.cache_key(table, record_id)
            deleted = self.redis_client.delete(key)
            self.prometheus_cache_operations.labels(operation='delete', result='success').inc()
            return deleted > 0
            
        except Exception as e:
            logger.error(f"Cache delete error for {table}:{record_id}: {e}")
            self.prometheus_cache_operations.labels(operation='delete', result='error').inc()
            return False
    
    def process_create_event(self, event: CDCEvent) -> bool:
        """Process CREATE event."""
        if not event.after:
            logger.warning(f"CREATE event without after data: {event.table}")
            return False
        
        record_id = event.after.get('id')
        if record_id:
            # Cache the new record
            self.set_in_cache(event.table, record_id, event.after)
            logger.debug(f"Cached new {event.table} record: {record_id}")
        
        return True
    
    def process_update_event(self, event: CDCEvent) -> bool:
        """Process UPDATE event."""
        if not event.after:
            logger.warning(f"UPDATE event without after data: {event.table}")
            return False
        
        record_id = event.after.get('id')
        if record_id:
            # Update cache with new data
            self.set_in_cache(event.table, record_id, event.after)
            
            # Log significant changes
            if event.before and event.after:
                changes = {}
                for key, new_value in event.after.items():
                    old_value = event.before.get(key)
                    if old_value != new_value:
                        changes[key] = {'old': old_value, 'new': new_value}
                
                if changes:
                    logger.info(f"Updated {event.table} {record_id}: {changes}")
        
        return True
    
    def process_delete_event(self, event: CDCEvent) -> bool:
        """Process DELETE event."""
        if not event.before:
            logger.warning(f"DELETE event without before data: {event.table}")
            return False
        
        record_id = event.before.get('id')
        if record_id:
            # Remove from cache
            self.delete_from_cache(event.table, record_id)
            logger.debug(f"Removed {event.table} record from cache: {record_id}")
        
        return True
    
    def process_read_event(self, event: CDCEvent) -> bool:
        """Process READ event (initial snapshot)."""
        if not event.after:
            logger.warning(f"READ event without after data: {event.table}")
            return False
        
        record_id = event.after.get('id')
        if record_id:
            # Cache snapshot data
            self.set_in_cache(event.table, record_id, event.after)
            logger.debug(f"Cached snapshot {event.table} record: {record_id}")
        
        return True
    
    def process_cdc_event(self, event: CDCEvent) -> bool:
        """Process a single CDC event."""
        start_time = time.time()
        
        try:
            operation = event.operation.lower()
            
            if operation == CDCOperation.CREATE.value:
                success = self.process_create_event(event)
            elif operation == CDCOperation.UPDATE.value:
                success = self.process_update_event(event)
            elif operation == CDCOperation.DELETE.value:
                success = self.process_delete_event(event)
            elif operation == CDCOperation.READ.value:
                success = self.process_read_event(event)
            else:
                logger.warning(f"Unknown CDC operation: {operation}")
                return False
            
            if success:
                self.metrics.events_processed += 1
                self.prometheus_events_processed.labels(
                    table=event.table, 
                    operation=operation
                ).inc()
                
                # Send processed event to stream topic
                self.kafka_producer.send(
                    self.kafka_config.stream_topic,
                    value=event.to_dict()
                )
            else:
                self.metrics.events_failed += 1
            
            processing_time = time.time() - start_time
            self.metrics.processing_time_total += processing_time
            self.prometheus_processing_time.labels(
                table=event.table,
                operation=operation
            ).observe(processing_time)
            
            return success
            
        except Exception as e:
            logger.error(f"Error processing CDC event: {e}")
            self.metrics.events_failed += 1
            return False
    
    def run_consumer(self, max_events: Optional[int] = None):
        """Run the CDC consumer."""
        logger.info("Starting CDC consumer")
        self.prometheus_active_consumers.inc()
        
        try:
            events_processed = 0
            
            for message in self.kafka_consumer:
                try:
                    # Parse Debezium message
                    cdc_event = CDCEvent.from_debezium_record(message.value)
                    
                    # Process the event
                    self.process_cdc_event(cdc_event)
                    
                    events_processed += 1
                    
                    # Log progress periodically
                    if events_processed % 100 == 0:
                        logger.info(f"Processed {events_processed} events. Metrics: {self.metrics.to_dict()}")
                    
                    # Stop if max events reached
                    if max_events and events_processed >= max_events:
                        logger.info(f"Reached max events limit: {max_events}")
                        break
                        
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    self.metrics.events_failed += 1
                    continue
        
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            self.prometheus_active_consumers.dec()
            logger.info(f"CDC consumer stopped. Final metrics: {self.metrics.to_dict()}")
    
    def get_cached_record(self, table: str, record_id: Any) -> Optional[Dict[str, Any]]:
        """Get a record from cache (public interface)."""
        return self.get_from_cache(table, record_id)
    
    def get_table_stats(self, table: str) -> Dict[str, Any]:
        """Get statistics for a specific table."""
        try:
            with self.get_database_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT 
                            COUNT(*) as total_records,
                            MAX(created_at) as latest_created,
                            MAX(updated_at) as latest_updated
                        FROM {self.database_config.schema}.{table}
                    """)
                    result = cursor.fetchone()
                    
                    if result:
                        return {
                            "table": table,
                            "total_records": result[0],
                            "latest_created": result[1].isoformat() if result[1] else None,
                            "latest_updated": result[2].isoformat() if result[2] else None
                        }
        
        except Exception as e:
            logger.error(f"Error getting table stats for {table}: {e}")
        
        return {"table": table, "error": "Unable to fetch stats"}
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check."""
        health = {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "components": {}
        }
        
        # Check Redis
        try:
            self.redis_client.ping()
            health["components"]["redis"] = "healthy"
        except Exception as e:
            health["components"]["redis"] = f"unhealthy: {e}"
            health["status"] = "degraded"
        
        # Check Database
        try:
            with self.get_database_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
            health["components"]["database"] = "healthy"
        except Exception as e:
            health["components"]["database"] = f"unhealthy: {e}"
            health["status"] = "degraded"
        
        # Add metrics
        health["metrics"] = self.metrics.to_dict()
        
        return health


def main():
    """Main function for running the CDC processor."""
    import argparse
    
    parser = argparse.ArgumentParser(description="ETL CDC Processor")
    parser.add_argument("--max-events", type=int, help="Maximum events to process")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    processor = CDCProcessor()
    processor.run_consumer(max_events=args.max_events)


if __name__ == "__main__":
    main()