#!/usr/bin/env python3
"""
Day 3 Streaming System - Simulation Test
Tests the real-time processing system without requiring Kafka infrastructure
"""

import asyncio
import time
import threading
import uuid
from datetime import datetime, timedelta
from typing import Dict, List
import numpy as np
from loguru import logger

# Import streaming components
from src.streaming import StreamEvent, StreamProducer
from src.streaming.producers import FinancialDataProducer
from src.streaming.features import RealTimeFeatureProcessor, SlidingWindowFeatures
from src.streaming.consumers import HighThroughputConsumer


class MockKafkaProducer:
    """Mock Kafka producer for simulation"""
    
    def __init__(self):
        self.events_sent = []
        self.performance_metrics = {
            'events_sent': 0,
            'events_per_second': 0,
            'data_mb_sent': 0.0
        }
    
    def send(self, topic: str, key: str = None, value: str = None, timestamp_ms: int = None):
        """Mock send method"""
        self.events_sent.append({
            'topic': topic,
            'key': key,
            'value': value,
            'timestamp_ms': timestamp_ms,
            'sent_at': time.time()
        })
        self.performance_metrics['events_sent'] += 1
        self.performance_metrics['data_mb_sent'] += len(value.encode('utf-8')) / 1024 / 1024
        return MockFuture()
    
    def flush(self):
        pass


class MockFuture:
    """Mock Kafka future"""
    
    def add_callback(self, callback):
        # Simulate successful send
        class MockMetadata:
            topic = 'test-topic'
            partition = 0
        callback(MockMetadata())
    
    def add_errback(self, errback):
        pass


class StreamingSimulationTest:
    """Complete simulation test for Day 3 streaming system"""
    
    def __init__(self):
        self.producer_mock = MockKafkaProducer()
        self.feature_processor = RealTimeFeatureProcessor(window_minutes=1)
        self.sliding_features = SlidingWindowFeatures(window_minutes=1)
        
        # Test configuration
        self.symbols = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN', 'NVDA', 'META']
        self.target_events_per_second = 1000
        self.test_duration_seconds = 30  # Short test duration
        
        # Results tracking
        self.events_generated = 0
        self.features_computed = 0
        self.anomalies_detected = 0
        self.start_time = None
        
        logger.info("Streaming simulation test initialized")
    
    def generate_realistic_market_data(self, symbol: str, base_price: float) -> Dict:
        """Generate realistic market tick data"""
        # Simulate realistic price movement
        price_change = np.random.normal(0, base_price * 0.001)  # 0.1% volatility
        price = max(base_price + price_change, 0.01)
        
        # Generate bid/ask spread
        spread_pct = np.random.uniform(0.001, 0.005)  # 0.1-0.5% spread
        spread = price * spread_pct
        bid = price - spread/2
        ask = price + spread/2
        
        # Generate volume
        volume = np.random.randint(100, 10000)
        
        return {
            'symbol': symbol,
            'price': round(price, 2),
            'volume': volume,
            'bid': round(bid, 2),
            'ask': round(ask, 2),
            'spread': round(spread, 2)
        }
    
    def simulate_high_frequency_stream(self):
        """Simulate high-frequency market data stream"""
        logger.info(f"Starting high-frequency simulation: {self.target_events_per_second} events/sec for {self.test_duration_seconds}s")
        
        # Initialize base prices
        base_prices = {symbol: np.random.uniform(100, 500) for symbol in self.symbols}
        
        self.start_time = time.time()
        end_time = self.start_time + self.test_duration_seconds
        interval = 1.0 / self.target_events_per_second
        
        while time.time() < end_time:
            try:
                # Generate market tick for random symbol
                symbol = np.random.choice(self.symbols)
                
                # Generate market data
                market_data = self.generate_realistic_market_data(symbol, base_prices[symbol])
                base_prices[symbol] = market_data['price']  # Update base price
                
                # Create stream event
                event = StreamEvent(
                    event_id=str(uuid.uuid4()),
                    timestamp=datetime.now(),
                    source="simulation",
                    event_type="market_tick",
                    data=market_data
                )
                
                # Process through feature processor
                self.feature_processor.process_market_tick(event)
                
                # Process through sliding features for detailed computation
                features = self.sliding_features.add_market_tick(
                    symbol, event.timestamp, market_data['price'], market_data['volume']
                )
                
                if features:
                    self.features_computed += len(features)
                    
                    # Test anomaly detection
                    is_anomaly, anomaly_score = self.sliding_features.detect_anomaly(
                        symbol, market_data['price']
                    )
                    if is_anomaly:
                        self.anomalies_detected += 1
                
                # Mock send to Kafka
                self.producer_mock.send(
                    topic='market-data',
                    key=symbol,
                    value=event.to_json(),
                    timestamp_ms=int(event.timestamp.timestamp() * 1000)
                )
                
                self.events_generated += 1
                
                # Sleep to maintain target frequency
                time.sleep(interval)
                
            except Exception as e:
                logger.error(f"Error in simulation: {e}")
                break
    
    def simulate_volume_spike(self):
        """Simulate volume spike detection"""
        logger.info("Testing volume spike detection...")
        
        symbol = 'SPIKE_TEST'
        
        # Generate normal volume events
        for i in range(10):
            normal_volume = np.random.randint(1000, 3000)
            event = StreamEvent(
                event_id=str(uuid.uuid4()),
                timestamp=datetime.now() + timedelta(seconds=i),
                source="simulation",
                event_type="market_tick",
                data={
                    'symbol': symbol,
                    'price': 100.0 + np.random.normal(0, 0.5),
                    'volume': normal_volume,
                    'bid': 99.99,
                    'ask': 100.01,
                    'spread': 0.02
                }
            )
            self.feature_processor.process_market_tick(event)
        
        # Generate volume spike
        spike_volume = 15000  # 5x normal volume
        spike_event = StreamEvent(
            event_id=str(uuid.uuid4()),
            timestamp=datetime.now() + timedelta(seconds=11),
            source="simulation",
            event_type="market_tick",
            data={
                'symbol': symbol,
                'price': 100.5,
                'volume': spike_volume,
                'bid': 100.49,
                'ask': 100.51,
                'spread': 0.02
            }
        )
        
        self.feature_processor.process_market_tick(spike_event)
        
        return True
    
    def simulate_price_anomaly(self):
        """Simulate price anomaly detection"""
        logger.info("Testing price anomaly detection...")
        
        symbol = 'ANOMALY_TEST'
        base_price = 150.0
        
        # Generate normal price movements
        for i in range(20):
            normal_price = base_price + np.random.normal(0, 0.5)  # Small movements
            event = StreamEvent(
                event_id=str(uuid.uuid4()),
                timestamp=datetime.now() + timedelta(seconds=i),
                source="simulation",
                event_type="market_tick",
                data={
                    'symbol': symbol,
                    'price': normal_price,
                    'volume': np.random.randint(1000, 3000),
                    'bid': normal_price - 0.01,
                    'ask': normal_price + 0.01,
                    'spread': 0.02
                }
            )
            self.feature_processor.process_market_tick(event)
        
        # Generate price anomaly
        anomaly_price = base_price * 1.3  # 30% price jump
        anomaly_event = StreamEvent(
            event_id=str(uuid.uuid4()),
            timestamp=datetime.now() + timedelta(seconds=21),
            source="simulation",
            event_type="market_tick",
            data={
                'symbol': symbol,
                'price': anomaly_price,
                'volume': 5000,
                'bid': anomaly_price - 0.01,
                'ask': anomaly_price + 0.01,
                'spread': 0.02
            }
        )
        
        self.feature_processor.process_market_tick(anomaly_event)
        
        return True
    
    def run_comprehensive_test(self):
        """Run comprehensive Day 3 streaming test"""
        logger.info("🚀 Starting Day 3 Streaming System Comprehensive Test")
        logger.info("=" * 60)
        
        try:
            # Test 1: High-frequency stream simulation
            logger.info("📊 Test 1: High-frequency market data simulation")
            self.simulate_high_frequency_stream()
            
            # Calculate performance metrics
            elapsed_time = time.time() - self.start_time
            actual_throughput = self.events_generated / elapsed_time
            
            logger.info(f"✅ Generated {self.events_generated:,} events in {elapsed_time:.2f}s")
            logger.info(f"✅ Achieved throughput: {actual_throughput:.1f} events/second")
            logger.info(f"✅ Target throughput: {self.target_events_per_second} events/second")
            
            # Test 2: Feature computation validation
            logger.info("🧮 Test 2: Feature computation validation")
            
            processor_summary = self.feature_processor.get_feature_summary()
            logger.info(f"✅ Events processed: {processor_summary['events_processed']:,}")
            logger.info(f"✅ Features computed: {self.features_computed:,}")
            logger.info(f"✅ Symbols tracked: {processor_summary['symbols_tracked']}")
            
            # Test 3: Anomaly detection
            logger.info("🚨 Test 3: Anomaly detection systems")
            self.simulate_volume_spike()
            self.simulate_price_anomaly()
            
            final_summary = self.feature_processor.get_feature_summary()
            logger.info(f"✅ Anomalies detected: {final_summary['anomalies_detected']}")
            
            # Test 4: Technical indicator validation
            logger.info("📈 Test 4: Technical indicators validation")
            
            # Get features for a test symbol
            test_features = self.feature_processor.get_features_for_symbol('AAPL')
            if test_features:
                features = test_features['features']
                logger.info(f"✅ AAPL Mean Price: ${features.get('AAPL_mean', 0):.2f}")
                logger.info(f"✅ AAPL Volatility: {features.get('AAPL_price_volatility', 0):.3f}")
                logger.info(f"✅ AAPL RSI: {features.get('AAPL_rsi', 0):.1f}")
                logger.info(f"✅ AAPL Momentum: {features.get('AAPL_momentum_5', 0):.2f}%")
            
            # Performance validation
            logger.info("⚡ Test 5: Performance validation")
            
            throughput_success = actual_throughput >= (self.target_events_per_second * 0.8)  # 80% of target
            feature_computation_success = self.features_computed > 0
            anomaly_detection_success = final_summary['anomalies_detected'] > 0
            
            logger.info(f"✅ Throughput test: {'PASS' if throughput_success else 'FAIL'}")
            logger.info(f"✅ Feature computation: {'PASS' if feature_computation_success else 'FAIL'}")
            logger.info(f"✅ Anomaly detection: {'PASS' if anomaly_detection_success else 'FAIL'}")
            
            # Final results
            logger.info("\n" + "=" * 60)
            logger.info("🎯 DAY 3 STREAMING SYSTEM TEST RESULTS")
            logger.info("=" * 60)
            
            all_tests_passed = all([
                throughput_success,
                feature_computation_success,
                anomaly_detection_success
            ])
            
            if all_tests_passed:
                logger.info("🎉 ALL TESTS PASSED - Day 3 system is fully operational!")
                logger.info(f"📊 Final Metrics:")
                logger.info(f"   • Throughput: {actual_throughput:.1f} events/sec")
                logger.info(f"   • Events processed: {final_summary['events_processed']:,}")
                logger.info(f"   • Features computed: {self.features_computed:,}")
                logger.info(f"   • Symbols tracked: {final_summary['symbols_tracked']}")
                logger.info(f"   • Anomalies detected: {final_summary['anomalies_detected']}")
                return True
            else:
                logger.error("❌ Some tests failed - check logs for details")
                return False
                
        except Exception as e:
            logger.error(f"❌ Test failed with error: {e}")
            return False


def main():
    """Run the simulation test"""
    logger.add("logs/day3_simulation_test.log", rotation="10 MB")
    
    # Create and run test
    test = StreamingSimulationTest()
    success = test.run_comprehensive_test()
    
    if success:
        print("\n✅ Day 3 streaming system simulation test completed successfully!")
        return 0
    else:
        print("\n❌ Day 3 streaming system simulation test failed!")
        return 1


if __name__ == "__main__":
    exit(main())
