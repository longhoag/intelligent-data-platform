"""
Stream Consumers for Real-Time Event Processing - Day 3 Implementation
High-performance Kafka consumers for financial data streaming
"""

import asyncio
import json
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable
from collections import defaultdict, deque
import pandas as pd
import numpy as np
from loguru import logger
from kafka import KafkaConsumer
from confluent_kafka import Consumer, OFFSET_BEGINNING
import prometheus_client

from . import StreamEvent, StreamAnalytics


class HighThroughputConsumer:
    """High-performance Kafka consumer for real-time financial data"""
    
    def __init__(self, 
                 topics: List[str],
                 group_id: str,
                 bootstrap_servers: str = 'localhost:9092',
                 max_poll_records: int = 1000,
                 auto_offset_reset: str = 'latest'):
        
        self.topics = topics
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        
        # Consumer configuration for high throughput
        self.consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': auto_offset_reset,
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 1000,
            'session.timeout.ms': 30000,
            'heartbeat.interval.ms': 10000,
            'max.poll.interval.ms': 300000,
            'fetch.min.bytes': 1024,
            'fetch.max.wait.ms': 500,
            'max.partition.fetch.bytes': 1048576,  # 1MB
        }
        
        self.consumer = None
        self.running = False
        self.event_handlers: Dict[str, List[Callable]] = defaultdict(list)
        self.analytics = StreamAnalytics()
        
        # Performance metrics
        self.messages_consumed = 0
        self.bytes_consumed = 0
        self.processing_times = []
        self.errors_count = 0
        
        # Prometheus metrics
        self.messages_counter = prometheus_client.Counter(
            'kafka_messages_consumed_total',
            'Total messages consumed',
            ['topic', 'group_id']
        )
        self.processing_histogram = prometheus_client.Histogram(
            'kafka_message_processing_seconds',
            'Time spent processing messages',
            ['topic', 'event_type']
        )
        
        logger.info(f"Initialized high-throughput consumer for topics: {topics}")
    
    def register_handler(self, event_type: str, handler: Callable[[StreamEvent], None]):
        """Register event handler for specific event type"""
        self.event_handlers[event_type].append(handler)
        logger.info(f"Registered handler for event type: {event_type}")
    
    def start_consuming(self):
        """Start consuming messages from Kafka"""
        try:
            self.consumer = Consumer(self.consumer_config)
            self.consumer.subscribe(self.topics)
            self.running = True
            
            logger.info(f"Started consuming from topics: {self.topics}")
            
            while self.running:
                # Poll for messages
                messages = self.consumer.consume(num_messages=100, timeout=1.0)
                
                if not messages:
                    continue
                
                # Process messages in batch
                self._process_message_batch(messages)
                
                # Commit offsets
                self.consumer.commit(asynchronous=True)
        
        except Exception as e:
            logger.error(f"Error in consumer: {e}")
            self.errors_count += 1
        finally:
            if self.consumer:
                self.consumer.close()
    
    def _process_message_batch(self, messages):
        """Process batch of messages for better throughput"""
        start_time = time.time()
        
        for message in messages:
            if message.error():
                logger.error(f"Kafka error: {message.error()}")
                self.errors_count += 1
                continue
            
            try:
                # Parse message
                topic = message.topic()
                value = json.loads(message.value().decode('utf-8'))
                timestamp = datetime.fromtimestamp(message.timestamp()[1] / 1000)
                
                # Create stream event
                event = StreamEvent(
                    event_id=value.get('id', str(hash(str(value) + str(timestamp)))),
                    event_type=value.get('event_type', 'unknown'),
                    data=value,
                    timestamp=timestamp,
                    source=topic
                )
                
                # Route to appropriate handlers
                self._route_event(event)
                
                # Update metrics
                self.messages_consumed += 1
                self.bytes_consumed += len(message.value())
                self.messages_counter.labels(topic=topic, group_id=self.group_id).inc()
                
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                self.errors_count += 1
        
        # Track processing time
        processing_time = time.time() - start_time
        self.processing_times.append(processing_time)
        
        # Keep only recent processing times (last 1000)
        if len(self.processing_times) > 1000:
            self.processing_times = self.processing_times[-1000:]
    
    def _route_event(self, event: StreamEvent):
        """Route event to registered handlers"""
        handlers = self.event_handlers.get(event.event_type, [])
        
        for handler in handlers:
            try:
                with self.processing_histogram.labels(
                    topic=event.source, event_type=event.event_type
                ).time():
                    handler(event)
            except Exception as e:
                logger.error(f"Error in event handler: {e}")
                self.errors_count += 1
    
    def stop_consuming(self):
        """Stop consuming messages"""
        self.running = False
        logger.info("Consumer stopped")
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get consumer performance metrics"""
        avg_processing_time = np.mean(self.processing_times) if self.processing_times else 0
        
        return {
            'messages_consumed': self.messages_consumed,
            'bytes_consumed': self.bytes_consumed,
            'errors_count': self.errors_count,
            'avg_processing_time_ms': avg_processing_time * 1000,
            'throughput_msgs_per_sec': self.messages_consumed / max(sum(self.processing_times), 1),
            'throughput_mb_per_sec': (self.bytes_consumed / 1024 / 1024) / max(sum(self.processing_times), 1)
        }


class FinancialDataConsumer:
    """Specialized consumer for financial market data"""
    
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        self.consumer = HighThroughputConsumer(
            topics=['market-data'],
            group_id='financial-data-processor',
            bootstrap_servers=bootstrap_servers
        )
        
        # Market data aggregation
        self.symbol_data: Dict[str, Dict] = defaultdict(dict)
        self.price_history: Dict[str, List] = defaultdict(lambda: deque(maxlen=1000))
        
        # Register handlers
        self.consumer.register_handler('market_tick', self._process_market_tick)
        self.consumer.register_handler('trade', self._process_trade)
        self.consumer.register_handler('quote', self._process_quote)
        
        logger.info("Financial data consumer initialized")
    
    def _process_market_tick(self, event: StreamEvent):
        """Process market tick data"""
        data = event.data
        symbol = data['symbol']
        price = data['price']
        volume = data['volume']
        
        # Update current symbol data
        self.symbol_data[symbol].update({
            'last_price': price,
            'last_volume': volume,
            'last_update': event.timestamp,
            'bid': data.get('bid'),
            'ask': data.get('ask'),
            'spread': data.get('spread')
        })
        
        # Add to price history
        self.price_history[symbol].append({
            'timestamp': event.timestamp,
            'price': price,
            'volume': volume
        })
        
        # Compute real-time indicators
        self._compute_technical_indicators(symbol)
    
    def _process_trade(self, event: StreamEvent):
        """Process trade execution data"""
        data = event.data
        symbol = data['symbol']
        
        # Update trade statistics
        if 'trades_today' not in self.symbol_data[symbol]:
            self.symbol_data[symbol]['trades_today'] = 0
            self.symbol_data[symbol]['volume_today'] = 0
        
        self.symbol_data[symbol]['trades_today'] += 1
        self.symbol_data[symbol]['volume_today'] += data.get('volume', 0)
        self.symbol_data[symbol]['last_trade_price'] = data['price']
        self.symbol_data[symbol]['last_trade_time'] = event.timestamp
    
    def _process_quote(self, event: StreamEvent):
        """Process bid/ask quote data"""
        data = event.data
        symbol = data['symbol']
        
        self.symbol_data[symbol].update({
            'bid': data['bid'],
            'ask': data['ask'],
            'bid_size': data.get('bid_size'),
            'ask_size': data.get('ask_size'),
            'spread': data['ask'] - data['bid'],
            'mid_price': (data['ask'] + data['bid']) / 2
        })
    
    def _compute_technical_indicators(self, symbol: str):
        """Compute real-time technical indicators"""
        history = list(self.price_history[symbol])
        if len(history) < 10:
            return
        
        prices = [item['price'] for item in history]
        volumes = [item['volume'] for item in history]
        
        # Simple moving averages
        if len(prices) >= 10:
            self.symbol_data[symbol]['sma_10'] = np.mean(prices[-10:])
        if len(prices) >= 20:
            self.symbol_data[symbol]['sma_20'] = np.mean(prices[-20:])
        if len(prices) >= 50:
            self.symbol_data[symbol]['sma_50'] = np.mean(prices[-50:])
        
        # Volume weighted average price (VWAP)
        total_volume = sum(volumes)
        if total_volume > 0:
            vwap = sum(p * v for p, v in zip(prices, volumes)) / total_volume
            self.symbol_data[symbol]['vwap'] = vwap
        
        # Price volatility (rolling standard deviation)
        if len(prices) >= 20:
            self.symbol_data[symbol]['volatility_20'] = np.std(prices[-20:])
        
        # Price momentum
        if len(prices) >= 10:
            momentum = (prices[-1] - prices[-10]) / prices[-10] * 100
            self.symbol_data[symbol]['momentum_10'] = momentum


class TransactionConsumer:
    """Consumer for transaction processing events"""
    
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        self.consumer = HighThroughputConsumer(
            topics=['transactions'],
            group_id='transaction-processor',
            bootstrap_servers=bootstrap_servers
        )
        
        # Transaction aggregation
        self.transaction_stats = defaultdict(lambda: {
            'count': 0,
            'total_value': 0,
            'avg_value': 0,
            'last_transaction': None
        })
        
        # Risk monitoring
        self.risk_limits = {
            'max_transaction_value': 1000000,  # $1M
            'max_daily_volume': 50000000,      # $50M
            'max_position_size': 10000000      # $10M
        }
        
        # Register handlers
        self.consumer.register_handler('transaction', self._process_transaction)
        self.consumer.register_handler('order', self._process_order)
        
        logger.info("Transaction consumer initialized")
    
    def _process_transaction(self, event: StreamEvent):
        """Process transaction events"""
        data = event.data
        symbol = data['symbol']
        value = data['value']
        transaction_type = data['type']
        
        # Update statistics
        stats = self.transaction_stats[symbol]
        stats['count'] += 1
        stats['total_value'] += value
        stats['avg_value'] = stats['total_value'] / stats['count']
        stats['last_transaction'] = event.timestamp
        
        # Risk checks
        self._check_risk_limits(symbol, value, data)
        
        # Pattern detection
        self._detect_transaction_patterns(symbol, data)
    
    def _process_order(self, event: StreamEvent):
        """Process order events"""
        data = event.data
        order_type = data.get('order_type', 'market')
        
        # Track order flow
        if 'order_flow' not in self.transaction_stats:
            self.transaction_stats['order_flow'] = defaultdict(int)
        
        self.transaction_stats['order_flow'][order_type] += 1
    
    def _check_risk_limits(self, symbol: str, value: float, transaction_data: Dict):
        """Check transaction against risk limits"""
        if value > self.risk_limits['max_transaction_value']:
            logger.warning(f"Large transaction detected: {symbol} = ${value:,.2f}")
            self.consumer.analytics._trigger_alert(
                f"Large transaction: {symbol} ${value:,.2f} exceeds limit"
            )
        
        daily_volume = self.transaction_stats[symbol]['total_value']
        if daily_volume > self.risk_limits['max_daily_volume']:
            logger.warning(f"High daily volume: {symbol} = ${daily_volume:,.2f}")
            self.consumer.analytics._trigger_alert(
                f"High daily volume: {symbol} ${daily_volume:,.2f}"
            )
    
    def _detect_transaction_patterns(self, symbol: str, transaction_data: Dict):
        """Detect unusual transaction patterns"""
        stats = self.transaction_stats[symbol]
        value = transaction_data['value']
        
        # Check for unusual transaction size
        if stats['count'] > 10 and value > stats['avg_value'] * 5:
            logger.info(f"Unusual transaction size detected: {symbol} ${value:,.2f}")


class PortfolioConsumer:
    """Consumer for portfolio update events"""
    
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        self.consumer = HighThroughputConsumer(
            topics=['portfolio-updates'],
            group_id='portfolio-processor',
            bootstrap_servers=bootstrap_servers
        )
        
        # Portfolio tracking
        self.portfolios: Dict[str, Dict] = defaultdict(lambda: {
            'positions': {},
            'total_value': 0,
            'pnl': 0,
            'last_update': None
        })
        
        # Register handlers
        self.consumer.register_handler('position_update', self._process_position_update)
        self.consumer.register_handler('pnl_update', self._process_pnl_update)
        
        logger.info("Portfolio consumer initialized")
    
    def _process_position_update(self, event: StreamEvent):
        """Process portfolio position updates"""
        data = event.data
        portfolio_id = data['portfolio_id']
        symbol = data['symbol']
        quantity = data['quantity']
        market_value = data['market_value']
        
        # Update position
        portfolio = self.portfolios[portfolio_id]
        portfolio['positions'][symbol] = {
            'quantity': quantity,
            'market_value': market_value,
            'last_update': event.timestamp
        }
        
        # Recalculate total portfolio value
        portfolio['total_value'] = sum(
            pos['market_value'] for pos in portfolio['positions'].values()
        )
        portfolio['last_update'] = event.timestamp
    
    def _process_pnl_update(self, event: StreamEvent):
        """Process PnL updates"""
        data = event.data
        portfolio_id = data['portfolio_id']
        pnl = data['pnl']
        
        self.portfolios[portfolio_id]['pnl'] = pnl
        self.portfolios[portfolio_id]['last_update'] = event.timestamp


class MultiStreamConsumerManager:
    """Manager for multiple consumer instances"""
    
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.consumers = {}
        self.threads = {}
        
        # Initialize consumers
        self.consumers['financial'] = FinancialDataConsumer(bootstrap_servers)
        self.consumers['transaction'] = TransactionConsumer(bootstrap_servers)
        self.consumers['portfolio'] = PortfolioConsumer(bootstrap_servers)
        
        logger.info("Multi-stream consumer manager initialized")
    
    def start_all_consumers(self):
        """Start all consumer instances"""
        for name, consumer in self.consumers.items():
            thread = threading.Thread(
                target=consumer.consumer.start_consuming,
                name=f"consumer-{name}"
            )
            thread.daemon = True
            thread.start()
            self.threads[name] = thread
            logger.info(f"Started {name} consumer")
    
    def stop_all_consumers(self):
        """Stop all consumer instances"""
        for name, consumer in self.consumers.items():
            consumer.consumer.stop_consuming()
            logger.info(f"Stopped {name} consumer")
    
    def get_consumer_metrics(self) -> Dict[str, Any]:
        """Get metrics from all consumers"""
        metrics = {}
        for name, consumer in self.consumers.items():
            metrics[name] = consumer.consumer.get_performance_metrics()
        return metrics


# Standalone execution for testing
if __name__ == "__main__":
    manager = MultiStreamConsumerManager()
    
    try:
        manager.start_all_consumers()
        
        # Monitor performance
        while True:
            time.sleep(30)
            metrics = manager.get_consumer_metrics()
            logger.info(f"Consumer Metrics: {metrics}")
            
    except KeyboardInterrupt:
        manager.stop_all_consumers()
        logger.info("Consumer manager stopped")
