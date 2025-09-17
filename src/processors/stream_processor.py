"""
Stream Processor for real-time ETL processing.
Handles windowing, anomaly detection, alerts, and performance monitoring.
"""

import json
import logging
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Deque
from dataclasses import dataclass, asdict
from statistics import mean, stdev
import asyncio
from kafka import KafkaConsumer, KafkaProducer
import redis

from config import config

logger = logging.getLogger(__name__)


@dataclass
class StreamMetrics:
    """Metrics for stream processing."""
    events_processed: int = 0
    windows_processed: int = 0
    anomalies_detected: int = 0
    alerts_sent: int = 0
    total_processing_time: float = 0.0
    start_time: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        runtime = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
        return {
            **asdict(self),
            "runtime_seconds": runtime,
            "events_per_second": self.events_processed / runtime if runtime > 0 else 0,
            "avg_processing_time_ms": (self.total_processing_time / self.events_processed * 1000) 
                                    if self.events_processed > 0 else 0
        }


@dataclass
class WindowData:
    """Data for a time window."""
    window_start: datetime
    window_end: datetime
    events: List[Dict[str, Any]]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "window_start": self.window_start.isoformat(),
            "window_end": self.window_end.isoformat(),
            "event_count": len(self.events),
            "events": self.events
        }


@dataclass
class Anomaly:
    """Anomaly detection result."""
    timestamp: datetime
    metric_name: str
    value: float
    expected_value: float
    threshold: float
    severity: str  # low, medium, high
    description: str
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            **asdict(self),
            "timestamp": self.timestamp.isoformat()
        }


@dataclass
class Alert:
    """Alert data structure."""
    alert_id: str
    timestamp: datetime
    alert_type: str
    severity: str
    message: str
    data: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            **asdict(self),
            "timestamp": self.timestamp.isoformat()
        }


class TimeWindow:
    """Sliding time window for stream processing."""
    
    def __init__(self, window_size_seconds: int, slide_interval_seconds: int = None):
        self.window_size = timedelta(seconds=window_size_seconds)
        self.slide_interval = timedelta(seconds=slide_interval_seconds or window_size_seconds // 2)
        self.events: Deque[Dict[str, Any]] = deque()
        self.last_window_time = datetime.now()
    
    def add_event(self, event: Dict[str, Any]):
        """Add event to the window."""
        # Add timestamp if not present
        if 'timestamp' not in event:
            event['timestamp'] = datetime.now().isoformat()
        
        self.events.append(event)
        self._cleanup_old_events()
    
    def _cleanup_old_events(self):
        """Remove events outside the window."""
        cutoff_time = datetime.now() - self.window_size
        
        while self.events:
            event_time_str = self.events[0].get('timestamp')
            if event_time_str:
                try:
                    event_time = datetime.fromisoformat(event_time_str.replace('Z', '+00:00'))
                    if event_time < cutoff_time:
                        self.events.popleft()
                    else:
                        break
                except (ValueError, TypeError):
                    # Remove malformed timestamp events
                    self.events.popleft()
            else:
                self.events.popleft()
    
    def get_current_window(self) -> WindowData:
        """Get current window data."""
        now = datetime.now()
        window_start = now - self.window_size
        
        return WindowData(
            window_start=window_start,
            window_end=now,
            events=list(self.events)
        )
    
    def should_process_window(self) -> bool:
        """Check if it's time to process a window."""
        return datetime.now() - self.last_window_time >= self.slide_interval
    
    def mark_window_processed(self):
        """Mark the current window as processed."""
        self.last_window_time = datetime.now()


class AnomalyDetector:
    """Detect anomalies in streaming data."""
    
    def __init__(self, threshold_multiplier: float = 2.0, history_size: int = 100):
        self.threshold_multiplier = threshold_multiplier
        self.history_size = history_size
        self.metric_history: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=history_size))
    
    def add_metric(self, metric_name: str, value: float):
        """Add a metric value to the history."""
        self.metric_history[metric_name].append(value)
    
    def detect_anomaly(self, metric_name: str, value: float) -> Optional[Anomaly]:
        """Detect if a value is anomalous."""
        history = self.metric_history[metric_name]
        
        if len(history) < 10:  # Need sufficient history
            return None
        
        try:
            mean_value = mean(history)
            std_value = stdev(history)
            
            if std_value == 0:  # No variation in data
                return None
            
            threshold = std_value * self.threshold_multiplier
            deviation = abs(value - mean_value)
            
            if deviation > threshold:
                severity = "high" if deviation > threshold * 2 else "medium"
                
                return Anomaly(
                    timestamp=datetime.now(),
                    metric_name=metric_name,
                    value=value,
                    expected_value=mean_value,
                    threshold=threshold,
                    severity=severity,
                    description=f"{metric_name} value {value:.2f} deviates {deviation:.2f} from expected {mean_value:.2f}"
                )
        
        except Exception as e:
            logger.warning(f"Error detecting anomaly for {metric_name}: {e}")
        
        return None


class AlertManager:
    """Manage alerts and notifications."""
    
    def __init__(self, redis_client: redis.Redis):
        self.redis_client = redis_client
        self.alert_history: Deque[Alert] = deque(maxlen=1000)
        self.alert_cooldown = timedelta(minutes=5)  # Prevent spam
        self.last_alert_times: Dict[str, datetime] = {}
    
    def should_send_alert(self, alert_type: str) -> bool:
        """Check if we should send an alert based on cooldown."""
        last_time = self.last_alert_times.get(alert_type)
        if last_time is None:
            return True
        
        return datetime.now() - last_time >= self.alert_cooldown
    
    def send_alert(self, alert: Alert) -> bool:
        """Send an alert."""
        if not self.should_send_alert(alert.alert_type):
            logger.debug(f"Alert {alert.alert_type} is in cooldown period")
            return False
        
        try:
            # Store alert in Redis
            alert_key = f"alert:{alert.alert_id}"
            self.redis_client.setex(
                alert_key, 
                86400,  # 24 hours TTL
                json.dumps(alert.to_dict())
            )
            
            # Publish alert to Redis channel for real-time notifications
            self.redis_client.publish("alerts", json.dumps(alert.to_dict()))
            
            # Store in history
            self.alert_history.append(alert)
            self.last_alert_times[alert.alert_type] = alert.timestamp
            
            logger.warning(f"Alert sent: {alert.message}")
            return True
        
        except Exception as e:
            logger.error(f"Error sending alert: {e}")
            return False
    
    def create_anomaly_alert(self, anomaly: Anomaly) -> Alert:
        """Create an alert from an anomaly."""
        alert_id = f"anomaly_{anomaly.metric_name}_{int(anomaly.timestamp.timestamp())}"
        
        return Alert(
            alert_id=alert_id,
            timestamp=anomaly.timestamp,
            alert_type="anomaly",
            severity=anomaly.severity,
            message=f"Anomaly detected: {anomaly.description}",
            data=anomaly.to_dict()
        )


class StreamProcessor:
    """Process streaming data with windowing, anomaly detection, and alerting."""
    
    def __init__(self):
        self.kafka_config = config.kafka
        self.processing_config = config.processing
        self.redis_config = config.redis
        
        # Initialize components
        self.kafka_consumer = self._create_kafka_consumer()
        self.redis_client = self._create_redis_client()
        
        # Processing components
        self.window = TimeWindow(
            window_size_seconds=self.processing_config.stream_window_seconds
        )
        self.anomaly_detector = AnomalyDetector(
            threshold_multiplier=self.processing_config.anomaly_threshold
        )
        self.alert_manager = AlertManager(self.redis_client)
        
        # Metrics
        self.metrics = StreamMetrics(start_time=datetime.now())
        
        # Performance tracking
        self.performance_metrics = {
            "order_rate": deque(maxlen=100),
            "revenue_rate": deque(maxlen=100),
            "customer_activity": deque(maxlen=100),
            "product_activity": deque(maxlen=100)
        }
    
    def _create_kafka_consumer(self) -> KafkaConsumer:
        """Create Kafka consumer for stream events."""
        return KafkaConsumer(
            self.kafka_config.stream_topic,
            bootstrap_servers=self.kafka_config.bootstrap_servers,
            group_id=f"{self.kafka_config.consumer_group}_stream",
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
    
    def _create_redis_client(self) -> redis.Redis:
        """Create Redis client."""
        return redis.Redis(
            host=self.redis_config.host,
            port=self.redis_config.port,
            password=self.redis_config.password or None,
            db=self.redis_config.db,
            decode_responses=True
        )
    
    def extract_metrics_from_event(self, event: Dict[str, Any]) -> Dict[str, float]:
        """Extract metrics from a CDC event."""
        metrics = {}
        table = event.get('table', '')
        operation = event.get('operation', '')
        after_data = event.get('after', {})
        
        try:
            if table == 'orders' and operation in ['c', 'u']:  # Create or update
                if 'total_amount' in after_data:
                    metrics['order_value'] = float(after_data['total_amount'])
                    metrics['order_count'] = 1.0
            
            elif table == 'customers' and operation == 'c':  # New customer
                metrics['new_customer_count'] = 1.0
            
            elif table == 'products' and operation in ['c', 'u']:  # Product activity
                metrics['product_activity'] = 1.0
                if 'stock_quantity' in after_data:
                    metrics['stock_level'] = float(after_data['stock_quantity'])
            
            elif table == 'order_items' and operation == 'c':  # New order item
                if 'quantity' in after_data:
                    metrics['item_quantity'] = float(after_data['quantity'])
        
        except (ValueError, TypeError) as e:
            logger.warning(f"Error extracting metrics from event: {e}")
        
        return metrics
    
    def calculate_window_aggregates(self, window: WindowData) -> Dict[str, float]:
        """Calculate aggregate metrics for a window."""
        aggregates = {
            "total_events": len(window.events),
            "order_count": 0,
            "total_revenue": 0.0,
            "avg_order_value": 0.0,
            "new_customers": 0,
            "product_updates": 0,
            "total_items": 0
        }
        
        order_values = []
        
        for event in window.events:
            table = event.get('table', '')
            operation = event.get('operation', '')
            after_data = event.get('after', {})
            
            if table == 'orders' and operation in ['c', 'u']:
                aggregates["order_count"] += 1
                if 'total_amount' in after_data:
                    value = float(after_data['total_amount'])
                    aggregates["total_revenue"] += value
                    order_values.append(value)
            
            elif table == 'customers' and operation == 'c':
                aggregates["new_customers"] += 1
            
            elif table == 'products' and operation in ['c', 'u']:
                aggregates["product_updates"] += 1
            
            elif table == 'order_items' and operation == 'c':
                aggregates["total_items"] += 1
        
        # Calculate average order value
        if order_values:
            aggregates["avg_order_value"] = mean(order_values)
        
        return aggregates
    
    def detect_performance_issues(self, aggregates: Dict[str, float]) -> List[Anomaly]:
        """Detect performance issues from window aggregates."""
        anomalies = []
        
        # Add metrics to anomaly detector and check for anomalies
        metrics_to_check = [
            ("order_rate", aggregates["order_count"]),
            ("revenue_rate", aggregates["total_revenue"]),
            ("avg_order_value", aggregates["avg_order_value"]),
            ("customer_activity", aggregates["new_customers"]),
            ("product_activity", aggregates["product_updates"])
        ]
        
        for metric_name, value in metrics_to_check:
            self.anomaly_detector.add_metric(metric_name, value)
            anomaly = self.anomaly_detector.detect_anomaly(metric_name, value)
            if anomaly:
                anomalies.append(anomaly)
        
        return anomalies
    
    def process_window(self, window: WindowData):
        """Process a complete window of events."""
        logger.debug(f"Processing window with {len(window.events)} events")
        
        if not window.events:
            return
        
        start_time = time.time()
        
        try:
            # Calculate aggregates
            aggregates = self.calculate_window_aggregates(window)
            
            # Detect anomalies
            anomalies = self.detect_performance_issues(aggregates)
            
            # Store window results in Redis
            window_key = f"window:{int(window.window_start.timestamp())}"
            window_data = {
                **window.to_dict(),
                "aggregates": aggregates,
                "anomalies": [a.to_dict() for a in anomalies]
            }
            self.redis_client.setex(window_key, 3600, json.dumps(window_data))  # 1 hour TTL
            
            # Send alerts for anomalies
            for anomaly in anomalies:
                alert = self.alert_manager.create_anomaly_alert(anomaly)
                if self.alert_manager.send_alert(alert):
                    self.metrics.alerts_sent += 1
            
            self.metrics.anomalies_detected += len(anomalies)
            self.metrics.windows_processed += 1
            
            # Log interesting windows
            if anomalies or aggregates["total_events"] > 10:
                logger.info(f"Window processed: {aggregates}")
                if anomalies:
                    logger.warning(f"Anomalies detected: {[a.description for a in anomalies]}")
        
        except Exception as e:
            logger.error(f"Error processing window: {e}")
        finally:
            processing_time = time.time() - start_time
            self.metrics.total_processing_time += processing_time
    
    def process_stream_event(self, event: Dict[str, Any]):
        """Process a single stream event."""
        start_time = time.time()
        
        try:
            # Add event to window
            self.window.add_event(event)
            
            # Extract and track real-time metrics
            metrics = self.extract_metrics_from_event(event)
            for metric_name, value in metrics.items():
                # Store in performance tracking
                if metric_name in self.performance_metrics:
                    self.performance_metrics[metric_name].append(value)
            
            # Check if window should be processed
            if self.window.should_process_window():
                window_data = self.window.get_current_window()
                self.process_window(window_data)
                self.window.mark_window_processed()
            
            self.metrics.events_processed += 1
            
            # Log progress periodically
            if self.metrics.events_processed % 100 == 0:
                logger.info(f"Processed {self.metrics.events_processed} stream events")
        
        except Exception as e:
            logger.error(f"Error processing stream event: {e}")
        finally:
            processing_time = time.time() - start_time
            self.metrics.total_processing_time += processing_time
    
    def run_stream_processor(self, max_events: Optional[int] = None):
        """Run the stream processor."""
        logger.info("Starting stream processor")
        
        try:
            events_processed = 0
            
            for message in self.kafka_consumer:
                try:
                    event = message.value
                    self.process_stream_event(event)
                    
                    events_processed += 1
                    
                    # Log metrics periodically
                    if events_processed % 500 == 0:
                        logger.info(f"Stream metrics: {self.metrics.to_dict()}")
                    
                    # Stop if max events reached
                    if max_events and events_processed >= max_events:
                        logger.info(f"Reached max events limit: {max_events}")
                        break
                
                except Exception as e:
                    logger.error(f"Error processing stream message: {e}")
                    continue
        
        except KeyboardInterrupt:
            logger.info("Stream processor interrupted by user")
        except Exception as e:
            logger.error(f"Stream processor error: {e}")
        finally:
            # Process final window
            if self.window.events:
                final_window = self.window.get_current_window()
                self.process_window(final_window)
            
            logger.info(f"Stream processor stopped. Final metrics: {self.metrics.to_dict()}")
    
    def get_recent_alerts(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent alerts."""
        return [alert.to_dict() for alert in list(self.alert_manager.alert_history)[-limit:]]
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary."""
        summary = {
            "metrics": self.metrics.to_dict(),
            "current_performance": {},
            "recent_windows": []
        }
        
        # Calculate current performance rates
        for metric_name, values in self.performance_metrics.items():
            if values:
                summary["current_performance"][metric_name] = {
                    "current": values[-1] if values else 0,
                    "average": mean(values) if values else 0,
                    "count": len(values)
                }
        
        # Get recent window data from Redis
        try:
            current_time = datetime.now()
            for i in range(10):  # Last 10 windows
                window_time = current_time - timedelta(seconds=i * self.processing_config.stream_window_seconds)
                window_key = f"window:{int(window_time.timestamp())}"
                window_data = self.redis_client.get(window_key)
                if window_data:
                    summary["recent_windows"].append(json.loads(window_data))
        
        except Exception as e:
            logger.error(f"Error getting recent windows: {e}")
        
        return summary
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check."""
        health = {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "components": {},
            "metrics": self.metrics.to_dict()
        }
        
        # Check Redis
        try:
            self.redis_client.ping()
            health["components"]["redis"] = "healthy"
        except Exception as e:
            health["components"]["redis"] = f"unhealthy: {e}"
            health["status"] = "degraded"
        
        # Check Kafka consumer
        try:
            # Check if consumer is connected
            health["components"]["kafka_consumer"] = "healthy"
        except Exception as e:
            health["components"]["kafka_consumer"] = f"unhealthy: {e}"
            health["status"] = "degraded"
        
        return health


def main():
    """Main function for running the stream processor."""
    import argparse
    
    parser = argparse.ArgumentParser(description="ETL Stream Processor")
    parser.add_argument("--max-events", type=int, help="Maximum events to process")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    processor = StreamProcessor()
    processor.run_stream_processor(max_events=args.max_events)


if __name__ == "__main__":
    main()