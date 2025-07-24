"""
Real-Time Streaming Infrastructure - Day 3 Implementation
Kafka-based streaming data processing with real-time feature computation
"""

import json
import time
import threading
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import pandas as pd
import numpy as np
from loguru import logger
from prometheus_client import Counter, Histogram, Gauge, start_http_server


@dataclass
class StreamEvent:
    """Stream event data structure"""
    event_id: str
    timestamp: datetime
    source: str
    event_type: str
    data: Dict[str, Any]
    
    def to_json(self) -> str:
        """Convert to JSON string for serialization"""
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        return json.dumps(data)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'StreamEvent':
        """Create from JSON string"""
        data = json.loads(json_str)
        data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        return cls(**data)


class StreamProducer:
    """High-performance streaming data producer"""
    
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self._setup_metrics()
        
    def _setup_metrics(self):
        """Setup Prometheus metrics"""
        self.events_produced = Counter('stream_events_produced_total', 'Total events produced')
        self.production_latency = Histogram('stream_production_latency_seconds', 'Event production latency')
        
    def connect(self):
        """Connect to Kafka cluster"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: v.encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                batch_size=16384,  # Optimize for throughput
                linger_ms=10,      # Small batching delay
                compression_type='lz4'  # Fast compression
            )
            logger.info(f"Connected to Kafka cluster: {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    def produce_event(self, topic: str, event: StreamEvent, key: Optional[str] = None):
        """Produce single event to stream"""
        if not self.producer:
            self.connect()
            
        try:
            with self.production_latency.time():
                future = self.producer.send(
                    topic=topic,
                    key=key,
                    value=event.to_json(),
                    timestamp_ms=int(event.timestamp.timestamp() * 1000)
                )
                
                # Non-blocking callback for error handling
                future.add_callback(self._on_send_success)
                future.add_errback(self._on_send_error)
                
                self.events_produced.inc()
                
        except Exception as e:
            logger.error(f"Failed to produce event: {e}")
            raise
    
    def _on_send_success(self, record_metadata):
        """Callback for successful send"""
        logger.debug(f"Event sent to {record_metadata.topic}:{record_metadata.partition}")
    
    def _on_send_error(self, exception):
        """Callback for send error"""
        logger.error(f"Failed to send event: {exception}")
    
    def close(self):
        """Close producer connection"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Stream producer closed")


class StreamConsumer:
    """High-performance streaming data consumer"""
    
    def __init__(self, topics: List[str], group_id: str, bootstrap_servers: str = 'localhost:9092'):
        self.topics = topics
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        self.consumer = None
        self.running = False
        self.message_handlers: Dict[str, Callable] = {}
        self._setup_metrics()
        
    def _setup_metrics(self):
        """Setup Prometheus metrics"""
        self.events_consumed = Counter('stream_events_consumed_total', 'Total events consumed')
        self.processing_latency = Histogram('stream_processing_latency_seconds', 'Event processing latency')
        self.consumer_lag = Gauge('stream_consumer_lag', 'Consumer lag in messages')
        
    def connect(self):
        """Connect to Kafka cluster"""
        try:
            self.consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                value_deserializer=lambda v: v.decode('utf-8'),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                auto_offset_reset='latest',  # Start from latest for real-time processing
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                fetch_min_bytes=1024,  # Optimize for throughput
                fetch_max_wait_ms=500
            )
            logger.info(f"Connected consumer {self.group_id} to topics: {self.topics}")
        except Exception as e:
            logger.error(f"Failed to connect consumer: {e}")
            raise
    
    def register_handler(self, event_type: str, handler: Callable[[StreamEvent], None]):
        """Register event handler for specific event type"""
        self.message_handlers[event_type] = handler
        logger.info(f"Registered handler for event type: {event_type}")
    
    def start_consuming(self):
        """Start consuming messages"""
        if not self.consumer:
            self.connect()
            
        self.running = True
        logger.info("Starting stream consumption...")
        
        try:
            for message in self.consumer:
                if not self.running:
                    break
                    
                try:
                    with self.processing_latency.time():
                        # Parse stream event
                        event = StreamEvent.from_json(message.value)
                        
                        # Calculate processing lag
                        current_time = datetime.now()
                        lag_ms = (current_time - event.timestamp).total_seconds() * 1000
                        self.consumer_lag.set(lag_ms)
                        
                        # Route to appropriate handler
                        if event.event_type in self.message_handlers:
                            self.message_handlers[event.event_type](event)
                        else:
                            logger.warning(f"No handler for event type: {event.event_type}")
                        
                        self.events_consumed.inc()
                        
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        finally:
            self.stop_consuming()
    
    def stop_consuming(self):
        """Stop consuming messages"""
        self.running = False
        if self.consumer:
            self.consumer.close()
            logger.info("Stream consumer stopped")


class RealTimeFeatureEngine:
    """Real-time feature computation engine with sliding windows"""
    
    def __init__(self, window_size_minutes: int = 5):
        self.window_size = timedelta(minutes=window_size_minutes)
        self.feature_windows: Dict[str, List[Dict]] = {}
        self.feature_cache: Dict[str, Any] = {}
        self.feature_freshness: Dict[str, datetime] = {}
        self._setup_metrics()
        
    def _setup_metrics(self):
        """Setup feature computation metrics"""
        self.features_computed = Counter('features_computed_total', 'Total features computed')
        self.feature_computation_time = Histogram('feature_computation_seconds', 'Feature computation time')
        self.active_windows = Gauge('active_feature_windows', 'Number of active feature windows')
        
    def add_event_to_window(self, feature_key: str, event_data: Dict[str, Any], timestamp: datetime):
        """Add event to sliding window for feature computation"""
        if feature_key not in self.feature_windows:
            self.feature_windows[feature_key] = []
        
        # Add new event
        self.feature_windows[feature_key].append({
            'timestamp': timestamp,
            'data': event_data
        })
        
        # Remove events outside window
        cutoff_time = timestamp - self.window_size
        self.feature_windows[feature_key] = [
            item for item in self.feature_windows[feature_key]
            if item['timestamp'] > cutoff_time
        ]
        
        self.active_windows.set(len(self.feature_windows))
    
    def compute_windowed_features(self, feature_key: str) -> Dict[str, float]:
        """Compute features for sliding window"""
        if feature_key not in self.feature_windows or not self.feature_windows[feature_key]:
            return {}
        
        with self.feature_computation_time.time():
            window_data = self.feature_windows[feature_key]
            
            # Extract numeric values for computation
            numeric_values = []
            for item in window_data:
                if 'value' in item['data'] and isinstance(item['data']['value'], (int, float)):
                    numeric_values.append(item['data']['value'])
            
            if not numeric_values:
                return {}
            
            # Compute windowed features
            features = {
                f'{feature_key}_count': len(numeric_values),
                f'{feature_key}_sum': sum(numeric_values),
                f'{feature_key}_mean': np.mean(numeric_values),
                f'{feature_key}_std': np.std(numeric_values),
                f'{feature_key}_min': min(numeric_values),
                f'{feature_key}_max': max(numeric_values),
                f'{feature_key}_range': max(numeric_values) - min(numeric_values)
            }
            
            # Update feature cache and freshness
            self.feature_cache.update(features)
            self.feature_freshness[feature_key] = datetime.now()
            self.features_computed.inc(len(features))
            
            return features
    
    def get_feature_freshness(self, feature_key: str) -> Optional[timedelta]:
        """Get feature freshness (time since last update)"""
        if feature_key not in self.feature_freshness:
            return None
        
        return datetime.now() - self.feature_freshness[feature_key]
    
    def get_stale_features(self, max_age_minutes: int = 10) -> List[str]:
        """Get list of stale features"""
        max_age = timedelta(minutes=max_age_minutes)
        stale_features = []
        
        for feature_key, last_update in self.feature_freshness.items():
            if datetime.now() - last_update > max_age:
                stale_features.append(feature_key)
        
        return stale_features


class StreamAnalytics:
    """Real-time stream analytics and monitoring"""
    
    def __init__(self):
        self.anomaly_thresholds: Dict[str, float] = {}
        self.alert_callbacks: List[Callable] = []
        self._setup_metrics()
        
    def _setup_metrics(self):
        """Setup analytics metrics"""
        self.anomalies_detected = Counter('anomalies_detected_total', 'Total anomalies detected')
        self.alerts_triggered = Counter('alerts_triggered_total', 'Total alerts triggered')
        
    def set_anomaly_threshold(self, metric_name: str, threshold: float):
        """Set anomaly detection threshold"""
        self.anomaly_thresholds[metric_name] = threshold
        logger.info(f"Set anomaly threshold for {metric_name}: {threshold}")
    
    def check_anomaly(self, metric_name: str, value: float) -> bool:
        """Check if value is anomalous"""
        if metric_name not in self.anomaly_thresholds:
            return False
        
        threshold = self.anomaly_thresholds[metric_name]
        is_anomaly = abs(value) > threshold
        
        if is_anomaly:
            self.anomalies_detected.inc()
            self._trigger_alert(f"Anomaly detected in {metric_name}: {value} (threshold: {threshold})")
        
        return is_anomaly
    
    def register_alert_callback(self, callback: Callable[[str], None]):
        """Register alert callback function"""
        self.alert_callbacks.append(callback)
    
    def _trigger_alert(self, message: str):
        """Trigger alert notifications"""
        self.alerts_triggered.inc()
        logger.warning(f"ALERT: {message}")
        
        for callback in self.alert_callbacks:
            try:
                callback(message)
            except Exception as e:
                logger.error(f"Alert callback failed: {e}")


class StreamingPipeline:
    """Complete streaming pipeline orchestrator"""
    
    def __init__(self, name: str, bootstrap_servers: str = 'localhost:9092'):
        self.name = name
        self.bootstrap_servers = bootstrap_servers
        self.producer = StreamProducer(bootstrap_servers)
        self.consumers: List[StreamConsumer] = []
        self.feature_engine = RealTimeFeatureEngine()
        self.analytics = StreamAnalytics()
        self.running = False
        
    def add_consumer(self, topics: List[str], group_id: str) -> StreamConsumer:
        """Add consumer to pipeline"""
        consumer = StreamConsumer(topics, group_id, self.bootstrap_servers)
        self.consumers.append(consumer)
        return consumer
    
    def start_pipeline(self):
        """Start the complete streaming pipeline"""
        logger.info(f"Starting streaming pipeline: {self.name}")
        
        # Start metrics server
        start_http_server(8000)
        logger.info("Prometheus metrics server started on port 8000")
        
        # Connect producer
        self.producer.connect()
        
        # Start consumers in separate threads
        consumer_threads = []
        for consumer in self.consumers:
            thread = threading.Thread(target=consumer.start_consuming)
            thread.daemon = True
            thread.start()
            consumer_threads.append(thread)
        
        self.running = True
        logger.info("Streaming pipeline started successfully")
        
        return consumer_threads
    
    def stop_pipeline(self):
        """Stop the streaming pipeline"""
        logger.info("Stopping streaming pipeline...")
        self.running = False
        
        # Stop consumers
        for consumer in self.consumers:
            consumer.stop_consuming()
        
        # Close producer
        self.producer.close()
        
        logger.info("Streaming pipeline stopped")
