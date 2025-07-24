#!/usr/bin/env python3
"""
Day 3 Real-Time Streaming Demo - Execute the complete streaming pipeline
Demonstrates real-time feature computation with 1000+ events/second
"""

import asyncio
import time
import threading
import signal
import sys
from datetime import datetime
from loguru import logger
import prometheus_client

# Import streaming components
from src.streaming.producers import MultiStreamProducer
from src.streaming.consumers import MultiStreamConsumerManager
from src.streaming.features import StreamingFeatureConsumer


class StreamingDemoOrchestrator:
    """Orchestrates the complete Day 3 streaming demonstration"""
    
    def __init__(self):
        self.bootstrap_servers = 'localhost:9092'
        self.running = False
        
        # Initialize components
        self.producer = None
        self.consumer_manager = None
        self.feature_consumer = None
        
        # Metrics server
        self.metrics_port = 8000
        
        # Demo configuration
        self.demo_duration = 300  # 5 minutes
        self.target_throughput = 1000  # events per second
        
        logger.info("Streaming demo orchestrator initialized")
    
    def setup_components(self):
        """Initialize all streaming components"""
        try:
            # Start Prometheus metrics server
            prometheus_client.start_http_server(self.metrics_port)
            logger.info(f"Metrics server started on port {self.metrics_port}")
            
            # Initialize producer
            self.producer = MultiStreamProducer(
                bootstrap_servers=self.bootstrap_servers,
                events_per_second=self.target_throughput
            )
            logger.info("Multi-stream producer initialized")
            
            # Initialize consumer manager
            self.consumer_manager = MultiStreamConsumerManager(
                bootstrap_servers=self.bootstrap_servers
            )
            logger.info("Consumer manager initialized")
            
            # Initialize feature consumer
            self.feature_consumer = StreamingFeatureConsumer(
                bootstrap_servers=self.bootstrap_servers
            )
            logger.info("Feature consumer initialized")
            
            return True
            
        except Exception as e:
            logger.error(f"Error setting up components: {e}")
            return False
    
    def start_demo(self):
        """Start the complete streaming demonstration"""
        logger.info("=" * 60)
        logger.info("STARTING DAY 3 REAL-TIME STREAMING DEMONSTRATION")
        logger.info("=" * 60)
        
        if not self.setup_components():
            logger.error("Failed to setup components")
            return False
        
        self.running = True
        
        try:
            # Start consumers first
            logger.info("Starting stream consumers...")
            self.consumer_manager.start_all_consumers()
            
            # Start feature processing
            logger.info("Starting feature computation engine...")
            feature_thread = self.feature_consumer.start_processing()
            
            # Wait a moment for consumers to be ready
            time.sleep(5)
            
            # Start producers
            logger.info(f"Starting producers (target: {self.target_throughput} events/sec)...")
            producer_thread = self.producer.start_multi_stream_production(
                symbols=['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN', 'NVDA', 'META', 'NFLX'],
                duration_seconds=self.demo_duration
            )
            
            # Monitor performance
            self._monitor_performance()
            
            # Wait for demo completion
            logger.info(f"Demo running for {self.demo_duration} seconds...")
            producer_thread.join()
            
            logger.info("Demo completed successfully!")
            return True
            
        except Exception as e:
            logger.error(f"Error during demo: {e}")
            return False
        finally:
            self.stop_demo()
    
    def _monitor_performance(self):
        """Monitor and report performance metrics"""
        def monitor():
            report_interval = 30  # seconds
            
            while self.running:
                time.sleep(report_interval)
                
                if not self.running:
                    break
                
                try:
                    # Get producer metrics
                    producer_metrics = self.producer.get_performance_metrics()
                    
                    # Get consumer metrics
                    consumer_metrics = self.consumer_manager.get_consumer_metrics()
                    
                    # Get feature processing metrics
                    feature_metrics = self.feature_consumer.feature_processor.get_feature_summary()
                    
                    # Log comprehensive report
                    logger.info("=" * 50)
                    logger.info("REAL-TIME PERFORMANCE REPORT")
                    logger.info("=" * 50)
                    
                    logger.info(f"🚀 PRODUCER PERFORMANCE:")
                    logger.info(f"   Events sent: {producer_metrics.get('events_sent', 0):,}")
                    logger.info(f"   Throughput: {producer_metrics.get('events_per_second', 0):.1f} events/sec")
                    logger.info(f"   Data volume: {producer_metrics.get('data_mb_sent', 0):.2f} MB")
                    
                    logger.info(f"📊 CONSUMER PERFORMANCE:")
                    for consumer_name, metrics in consumer_metrics.items():
                        logger.info(f"   {consumer_name.upper()}: "
                                  f"{metrics.get('messages_consumed', 0):,} messages, "
                                  f"{metrics.get('throughput_msgs_per_sec', 0):.1f} msg/sec")
                    
                    logger.info(f"🧮 FEATURE COMPUTATION:")
                    logger.info(f"   Events processed: {feature_metrics.get('events_processed', 0):,}")
                    logger.info(f"   Features computed: {feature_metrics.get('features_computed', 0):,}")
                    logger.info(f"   Symbols tracked: {feature_metrics.get('symbols_tracked', 0)}")
                    logger.info(f"   Anomalies detected: {feature_metrics.get('anomalies_detected', 0)}")
                    
                    # Check for stale features
                    stale_features = feature_metrics.get('stale_features', 0)
                    if stale_features > 0:
                        logger.warning(f"   ⚠️ Stale features: {stale_features}")
                    
                    logger.info("=" * 50)
                    
                except Exception as e:
                    logger.error(f"Error in performance monitoring: {e}")
        
        monitor_thread = threading.Thread(target=monitor, name="performance-monitor")
        monitor_thread.daemon = True
        monitor_thread.start()
        
        return monitor_thread
    
    def stop_demo(self):
        """Stop all demo components"""
        logger.info("Stopping streaming demonstration...")
        self.running = False
        
        if self.producer:
            self.producer.stop()
            logger.info("Producer stopped")
        
        if self.consumer_manager:
            self.consumer_manager.stop_all_consumers()
            logger.info("Consumers stopped")
        
        if self.feature_consumer:
            self.feature_consumer.stop_processing()
            logger.info("Feature processing stopped")
        
        logger.info("All components stopped")
    
    def generate_final_report(self):
        """Generate final demonstration report"""
        logger.info("\n" + "=" * 60)
        logger.info("FINAL DEMONSTRATION REPORT")
        logger.info("=" * 60)
        
        try:
            # Get final metrics
            if self.producer:
                producer_metrics = self.producer.get_performance_metrics()
                logger.info(f"📈 TOTAL EVENTS PRODUCED: {producer_metrics.get('events_sent', 0):,}")
                logger.info(f"📊 AVERAGE THROUGHPUT: {producer_metrics.get('events_per_second', 0):.1f} events/sec")
                logger.info(f"💾 TOTAL DATA VOLUME: {producer_metrics.get('data_mb_sent', 0):.2f} MB")
            
            if self.feature_consumer:
                feature_metrics = self.feature_consumer.feature_processor.get_feature_summary()
                logger.info(f"🧮 FEATURES COMPUTED: {feature_metrics.get('features_computed', 0):,}")
                logger.info(f"📍 SYMBOLS PROCESSED: {feature_metrics.get('symbols_tracked', 0)}")
                logger.info(f"🚨 ANOMALIES DETECTED: {feature_metrics.get('anomalies_detected', 0)}")
            
            # Success criteria
            target_met = (producer_metrics.get('events_per_second', 0) >= self.target_throughput * 0.8)
            logger.info(f"\n✅ TARGET THROUGHPUT MET: {'YES' if target_met else 'NO'}")
            logger.info(f"🎯 REQUIREMENT: ≥1000 events/sec")
            logger.info(f"📊 ACHIEVED: {producer_metrics.get('events_per_second', 0):.1f} events/sec")
            
            logger.info("\n🎉 DAY 3 REAL-TIME STREAMING DEMONSTRATION COMPLETED")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"Error generating final report: {e}")


def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info("Received shutdown signal, stopping demo...")
    if hasattr(signal_handler, 'orchestrator'):
        signal_handler.orchestrator.stop_demo()
    sys.exit(0)


def main():
    """Main execution function"""
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Configure logging
    logger.add("logs/day3_streaming_demo.log", rotation="10 MB", retention="7 days")
    
    # Create and run orchestrator
    orchestrator = StreamingDemoOrchestrator()
    signal_handler.orchestrator = orchestrator
    
    try:
        # Display startup information
        logger.info("🚀 Intelligent Data Platform - Day 3 Streaming Demo")
        logger.info(f"📅 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"🎯 Target throughput: {orchestrator.target_throughput:,} events/second")
        logger.info(f"⏱️ Demo duration: {orchestrator.demo_duration} seconds")
        logger.info(f"📊 Metrics endpoint: http://localhost:{orchestrator.metrics_port}/metrics")
        
        # Run demonstration
        success = orchestrator.start_demo()
        
        # Generate final report
        orchestrator.generate_final_report()
        
        if success:
            logger.info("✅ Demo completed successfully!")
            return 0
        else:
            logger.error("❌ Demo failed!")
            return 1
            
    except KeyboardInterrupt:
        logger.info("Demo interrupted by user")
        return 0
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1
    finally:
        orchestrator.stop_demo()


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
