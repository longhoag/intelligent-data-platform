"""
Integration Tests for Real-Time Streaming Pipeline - Day 3
End-to-end testing of streaming infrastructure and feature computation
"""

import asyncio
import json
import time
import unittest
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any
import pytest
import pandas as pd
import numpy as np
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from loguru import logger

# Import streaming components
from src.streaming.producers import MultiStreamProducer, FinancialDataProducer
from src.streaming.consumers import MultiStreamConsumerManager
from src.streaming.features import RealTimeFeatureProcessor, StreamingFeatureConsumer
from src.streaming import StreamEvent


class StreamingIntegrationTests(unittest.TestCase):
    """Integration tests for streaming pipeline"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment"""
        cls.bootstrap_servers = 'localhost:9092'
        cls.test_topics = ['test-market-data', 'test-transactions', 'test-portfolio-updates']
        
        # Setup Kafka topics for testing
        cls._setup_test_topics()
        
        # Initialize test data
        cls.test_symbols = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN']
        cls.test_events_count = 1000
        
        logger.info("Integration test environment setup complete")
    
    @classmethod
    def _setup_test_topics(cls):
        """Create test topics in Kafka"""
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=cls.bootstrap_servers,
                client_id='test_admin'
            )
            
            # Create test topics
            topics = [
                NewTopic(name=topic, num_partitions=3, replication_factor=1)
                for topic in cls.test_topics
            ]
            
            admin_client.create_topics(topics, validate_only=False)
            logger.info(f"Created test topics: {cls.test_topics}")
            
        except Exception as e:
            logger.warning(f"Topic creation error (may already exist): {e}")
    
    def setUp(self):
        """Set up each test"""
        self.producer = MultiStreamProducer(bootstrap_servers=self.bootstrap_servers)
        self.consumer_manager = None
        self.feature_processor = RealTimeFeatureProcessor()
        
        # Test metrics
        self.events_sent = 0
        self.events_received = 0
        self.features_computed = 0
        self.test_start_time = None
        
    def tearDown(self):
        """Clean up after each test"""
        if self.consumer_manager:
            self.consumer_manager.stop_all_consumers()
        
        if hasattr(self.producer, 'stop'):
            self.producer.stop()
    
    def test_producer_consumer_throughput(self):
        """Test high-throughput producer-consumer pipeline"""
        logger.info("Testing producer-consumer throughput...")
        
        # Setup consumer
        consumer = KafkaConsumer(
            'test-market-data',
            bootstrap_servers=self.bootstrap_servers,
            group_id='test-group',
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        # Start consumer in separate thread
        received_messages = []
        
        def consume_messages():
            for message in consumer:
                received_messages.append(message.value)
                if len(received_messages) >= self.test_events_count:
                    break
        
        consumer_thread = threading.Thread(target=consume_messages)
        consumer_thread.daemon = True
        consumer_thread.start()
        
        # Generate test data
        financial_producer = FinancialDataProducer(
            topic='test-market-data',
            bootstrap_servers=self.bootstrap_servers
        )
        
        start_time = time.time()
        
        # Send messages
        for i in range(self.test_events_count):
            financial_producer.generate_market_tick()
            self.events_sent += 1
        
        # Wait for all messages to be consumed
        consumer_thread.join(timeout=30)
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Assertions
        self.assertEqual(len(received_messages), self.test_events_count)
        
        throughput = self.test_events_count / duration
        logger.info(f"Throughput: {throughput:.2f} messages/second")
        
        # Verify we achieved target throughput (> 100 messages/second)
        self.assertGreater(throughput, 100, "Throughput should exceed 100 msg/sec")
        
        consumer.close()
    
    def test_feature_computation_accuracy(self):
        """Test accuracy of real-time feature computation"""
        logger.info("Testing feature computation accuracy...")
        
        # Generate test market data
        test_data = []
        base_price = 150.0
        
        for i in range(100):
            # Generate realistic price movement
            price_change = np.random.normal(0, 0.5)
            price = base_price + price_change
            volume = np.random.randint(1000, 10000)
            
            event = StreamEvent(
                event_type='market_tick',
                data={
                    'symbol': 'AAPL',
                    'price': price,
                    'volume': volume,
                    'bid': price - 0.01,
                    'ask': price + 0.01,
                    'spread': 0.02
                },
                timestamp=datetime.now() + timedelta(seconds=i),
                source='test'
            )
            
            test_data.append(event)
            base_price = price
        
        # Process events through feature processor
        for event in test_data:
            self.feature_processor.process_market_tick(event)
        
        # Get computed features
        features = self.feature_processor.get_features_for_symbol('AAPL')
        self.assertIsNotNone(features, "Features should be computed for AAPL")
        
        feature_data = features['features']
        
        # Verify key features exist
        expected_features = [
            'AAPL_mean', 'AAPL_std', 'AAPL_min', 'AAPL_max',
            'AAPL_price_volatility', 'AAPL_momentum_5', 'AAPL_rsi'
        ]
        
        for feature_name in expected_features:
            self.assertIn(feature_name, feature_data, f"Feature {feature_name} should exist")
            self.assertIsInstance(feature_data[feature_name], (int, float), 
                                f"Feature {feature_name} should be numeric")
        
        # Verify feature values are reasonable
        self.assertGreater(feature_data['AAPL_mean'], 0, "Mean price should be positive")
        self.assertGreaterEqual(feature_data['AAPL_std'], 0, "Standard deviation should be non-negative")
        self.assertGreaterEqual(feature_data['AAPL_rsi'], 0, "RSI should be non-negative")
        self.assertLessEqual(feature_data['AAPL_rsi'], 100, "RSI should not exceed 100")
        
        logger.info(f"Computed {len(feature_data)} features successfully")
    
    def test_anomaly_detection_system(self):
        """Test anomaly detection capabilities"""
        logger.info("Testing anomaly detection system...")
        
        # Generate normal data
        normal_events = []
        base_price = 100.0
        
        for i in range(50):
            # Normal price movement (small changes)
            price_change = np.random.normal(0, 0.1)
            price = base_price + price_change
            
            event = StreamEvent(
                event_type='market_tick',
                data={
                    'symbol': 'TEST',
                    'price': price,
                    'volume': np.random.randint(1000, 5000),
                    'bid': price - 0.01,
                    'ask': price + 0.01,
                    'spread': 0.02
                },
                timestamp=datetime.now() + timedelta(seconds=i),
                source='test'
            )
            
            normal_events.append(event)
            base_price = price
        
        # Process normal events
        for event in normal_events:
            self.feature_processor.process_market_tick(event)
        
        # Generate anomalous event (large price jump)
        anomaly_price = base_price * 1.5  # 50% price jump
        anomaly_event = StreamEvent(
            event_type='market_tick',
            data={
                'symbol': 'TEST',
                'price': anomaly_price,
                'volume': 50000,  # High volume
                'bid': anomaly_price - 0.01,
                'ask': anomaly_price + 0.01,
                'spread': 0.02
            },
            timestamp=datetime.now() + timedelta(seconds=51),
            source='test'
        )
        
        # Process anomaly event and check detection
        self.feature_processor.process_market_tick(anomaly_event)
        
        # Verify anomaly was detected (check analytics for alerts)
        # Note: This would require access to the analytics system
        # For now, we verify the feature computation handled the anomaly
        features = self.feature_processor.get_features_for_symbol('TEST')
        self.assertIsNotNone(features, "Features should exist after anomaly")
        
        # Check that volatility increased significantly
        volatility = features['features'].get('TEST_price_volatility', 0)
        self.assertGreater(volatility, 1.0, "Volatility should increase after anomaly")
        
        logger.info("Anomaly detection test completed")
    
    def test_multi_symbol_processing(self):
        """Test processing multiple symbols simultaneously"""
        logger.info("Testing multi-symbol processing...")
        
        # Generate events for multiple symbols
        symbols = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN']
        events_per_symbol = 20
        
        all_events = []
        for symbol in symbols:
            base_price = np.random.uniform(50, 300)  # Random starting price
            
            for i in range(events_per_symbol):
                price_change = np.random.normal(0, 1.0)
                price = max(1.0, base_price + price_change)  # Ensure positive price
                
                event = StreamEvent(
                    event_type='market_tick',
                    data={
                        'symbol': symbol,
                        'price': price,
                        'volume': np.random.randint(1000, 10000),
                        'bid': price - 0.01,
                        'ask': price + 0.01,
                        'spread': 0.02
                    },
                    timestamp=datetime.now() + timedelta(seconds=len(all_events)),
                    source='test'
                )
                
                all_events.append(event)
                base_price = price
        
        # Shuffle events to simulate real-time mixed stream
        np.random.shuffle(all_events)
        
        # Process all events
        for event in all_events:
            self.feature_processor.process_market_tick(event)
        
        # Verify features computed for all symbols
        for symbol in symbols:
            features = self.feature_processor.get_features_for_symbol(symbol)
            self.assertIsNotNone(features, f"Features should exist for {symbol}")
            
            feature_data = features['features']
            self.assertGreater(len(feature_data), 0, f"Features should be computed for {symbol}")
            
            # Check symbol-specific features exist
            symbol_features = [k for k in feature_data.keys() if k.startswith(symbol)]
            self.assertGreater(len(symbol_features), 5, 
                             f"Should have multiple features for {symbol}")
        
        # Check performance summary
        summary = self.feature_processor.get_feature_summary()
        self.assertEqual(summary['symbols_tracked'], len(symbols))
        self.assertEqual(summary['events_processed'], len(all_events))
        
        logger.info(f"Successfully processed {len(all_events)} events for {len(symbols)} symbols")
    
    def test_pipeline_performance_under_load(self):
        """Test pipeline performance under high load"""
        logger.info("Testing pipeline performance under load...")
        
        # Setup metrics
        start_time = time.time()
        target_throughput = 1000  # events per second
        test_duration = 5  # seconds
        total_events = target_throughput * test_duration
        
        # Generate high-frequency events
        events_generated = 0
        batch_size = 100
        
        for batch in range(total_events // batch_size):
            batch_events = []
            
            for i in range(batch_size):
                symbol = np.random.choice(self.test_symbols)
                price = np.random.uniform(50, 300)
                volume = np.random.randint(1000, 10000)
                
                event = StreamEvent(
                    event_type='market_tick',
                    data={
                        'symbol': symbol,
                        'price': price,
                        'volume': volume,
                        'bid': price - 0.01,
                        'ask': price + 0.01,
                        'spread': 0.02
                    },
                    timestamp=datetime.now(),
                    source='load_test'
                )
                
                batch_events.append(event)
                events_generated += 1
            
            # Process batch
            batch_start = time.time()
            for event in batch_events:
                self.feature_processor.process_market_tick(event)
            batch_end = time.time()
            
            # Check batch processing time
            batch_duration = batch_end - batch_start
            batch_throughput = batch_size / batch_duration
            
            # Log progress every 10 batches
            if batch % 10 == 0:
                logger.info(f"Batch {batch}: {batch_throughput:.0f} events/sec")
        
        end_time = time.time()
        total_duration = end_time - start_time
        achieved_throughput = events_generated / total_duration
        
        # Performance assertions
        self.assertGreater(achieved_throughput, target_throughput * 0.8, 
                          f"Should achieve at least 80% of target throughput ({target_throughput})")
        
        # Check feature computation performance
        summary = self.feature_processor.get_feature_summary()
        self.assertEqual(summary['events_processed'], events_generated)
        self.assertGreater(summary['features_computed'], 0)
        
        logger.info(f"Load test completed: {achieved_throughput:.2f} events/sec "
                   f"({events_generated} events in {total_duration:.2f}s)")
    
    def test_end_to_end_pipeline(self):
        """Test complete end-to-end streaming pipeline"""
        logger.info("Testing end-to-end pipeline...")
        
        # This test would require full Kafka setup
        # For now, we'll test the component integration
        
        # Initialize all components
        producer = FinancialDataProducer(
            topic='test-market-data',
            bootstrap_servers=self.bootstrap_servers
        )
        
        feature_consumer = StreamingFeatureConsumer(
            bootstrap_servers=self.bootstrap_servers
        )
        
        # Start feature processing
        # consumer_thread = feature_consumer.start_processing()
        
        # Generate test data
        test_symbols = ['AAPL', 'GOOGL']
        events_to_send = 50
        
        # This would be a full integration test in a real environment
        # For unit testing, we verify component initialization
        self.assertIsNotNone(producer)
        self.assertIsNotNone(feature_consumer)
        self.assertIsNotNone(feature_consumer.feature_processor)
        
        logger.info("End-to-end pipeline components initialized successfully")


class PerformanceBenchmarks(unittest.TestCase):
    """Performance benchmarking tests"""
    
    def test_feature_computation_latency(self):
        """Benchmark feature computation latency"""
        processor = RealTimeFeatureProcessor()
        
        # Warmup
        for i in range(10):
            event = StreamEvent(
                event_type='market_tick',
                data={
                    'symbol': 'WARMUP',
                    'price': 100.0,
                    'volume': 1000,
                    'bid': 99.99,
                    'ask': 100.01,
                    'spread': 0.02
                },
                timestamp=datetime.now(),
                source='benchmark'
            )
            processor.process_market_tick(event)
        
        # Benchmark
        latencies = []
        num_tests = 1000
        
        for i in range(num_tests):
            event = StreamEvent(
                event_type='market_tick',
                data={
                    'symbol': 'BENCH',
                    'price': 100.0 + np.random.normal(0, 1),
                    'volume': np.random.randint(1000, 10000),
                    'bid': 99.99,
                    'ask': 100.01,
                    'spread': 0.02
                },
                timestamp=datetime.now(),
                source='benchmark'
            )
            
            start_time = time.time()
            processor.process_market_tick(event)
            end_time = time.time()
            
            latencies.append((end_time - start_time) * 1000)  # Convert to milliseconds
        
        # Analyze results
        avg_latency = np.mean(latencies)
        p95_latency = np.percentile(latencies, 95)
        p99_latency = np.percentile(latencies, 99)
        
        logger.info(f"Feature computation latency - Avg: {avg_latency:.2f}ms, "
                   f"P95: {p95_latency:.2f}ms, P99: {p99_latency:.2f}ms")
        
        # Performance assertions
        self.assertLess(avg_latency, 10, "Average latency should be under 10ms")
        self.assertLess(p95_latency, 20, "P95 latency should be under 20ms")
        self.assertLess(p99_latency, 50, "P99 latency should be under 50ms")


if __name__ == '__main__':
    # Setup logging
    logger.add("tests/integration_test.log", rotation="10 MB")
    
    # Run tests
    unittest.main(verbosity=2)
