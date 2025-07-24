"""
Real-Time Feature Computation Engine - Day 3 Implementation
Streaming feature computation with sliding windows and anomaly detection
"""

import asyncio
import json
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from collections import defaultdict, deque
import pandas as pd
import numpy as np
from loguru import logger
import river
from river import anomaly, metrics, stats

from . import StreamConsumer, StreamEvent, RealTimeFeatureEngine, StreamAnalytics


class SlidingWindowFeatures:
    """Sliding window feature computation with river online learning"""
    
    def __init__(self, window_minutes: int = 5):
        self.window_size = timedelta(minutes=window_minutes)
        self.symbol_windows: Dict[str, deque] = defaultdict(deque)
        self.feature_cache: Dict[str, Dict[str, float]] = {}
        
        # Initialize river statistics
        self.stats_engines = defaultdict(lambda: {
            'mean': stats.Mean(),
            'var': stats.Var(),
            'min': stats.Min(),
            'max': stats.Max(),
            'ewm': stats.EWMean(0.1),  # Exponential weighted mean (alpha as positional arg)
            'quantile_95': stats.Quantile(0.95),
            'quantile_05': stats.Quantile(0.05)
        })
        
        # Anomaly detection per symbol
        self.anomaly_detectors = defaultdict(lambda: anomaly.HalfSpaceTrees(
            n_trees=10,
            height=8,
            window_size=250
        ))
        
        logger.info(f"Initialized sliding window features with {window_minutes}min windows")
    
    def add_market_tick(self, symbol: str, timestamp: datetime, price: float, volume: int):
        """Add market tick to sliding window"""
        # Add to window
        self.symbol_windows[symbol].append({
            'timestamp': timestamp,
            'price': price,
            'volume': volume
        })
        
        # Remove old data outside window
        cutoff_time = timestamp - self.window_size
        while (self.symbol_windows[symbol] and 
               self.symbol_windows[symbol][0]['timestamp'] < cutoff_time):
            self.symbol_windows[symbol].popleft()
        
        # Update online statistics
        stats_engine = self.stats_engines[symbol]
        stats_engine['mean'].update(price)
        stats_engine['var'].update(price)
        stats_engine['min'].update(price)
        stats_engine['max'].update(price)
        stats_engine['ewm'].update(price)
        stats_engine['quantile_95'].update(price)
        stats_engine['quantile_05'].update(price)
        
        # Compute windowed features
        features = self._compute_windowed_features(symbol)
        self.feature_cache[symbol] = features
        
        return features
    
    def _compute_windowed_features(self, symbol: str) -> Dict[str, float]:
        """Compute comprehensive windowed features"""
        window_data = list(self.symbol_windows[symbol])
        if len(window_data) < 2:
            return {}
        
        prices = [item['price'] for item in window_data]
        volumes = [item['volume'] for item in window_data]
        timestamps = [item['timestamp'] for item in window_data]
        
        # Basic statistics
        stats_engine = self.stats_engines[symbol]
        
        features = {
            # Online statistics (more accurate for streaming)
            f'{symbol}_mean': stats_engine['mean'].get(),
            f'{symbol}_std': np.sqrt(stats_engine['var'].get()),
            f'{symbol}_min': stats_engine['min'].get(),
            f'{symbol}_max': stats_engine['max'].get(),
            f'{symbol}_ewm': stats_engine['ewm'].get(),
            f'{symbol}_q95': stats_engine['quantile_95'].get(),
            f'{symbol}_q05': stats_engine['quantile_05'].get(),
            
            # Window-based features
            f'{symbol}_window_size': len(prices),
            f'{symbol}_price_range': max(prices) - min(prices),
            f'{symbol}_volume_total': sum(volumes),
            f'{symbol}_volume_mean': np.mean(volumes),
            
            # Price movement features
            f'{symbol}_price_change': prices[-1] - prices[0] if len(prices) > 1 else 0,
            f'{symbol}_price_change_pct': ((prices[-1] - prices[0]) / prices[0] * 100) if prices[0] != 0 else 0,
            
            # Volatility features
            f'{symbol}_price_volatility': np.std(prices) if len(prices) > 1 else 0,
            f'{symbol}_returns_volatility': np.std(np.diff(prices)) if len(prices) > 2 else 0,
            
            # Momentum indicators
            f'{symbol}_momentum_5': self._calculate_momentum(prices, 5),
            f'{symbol}_momentum_10': self._calculate_momentum(prices, 10),
            
            # Volume indicators
            f'{symbol}_volume_volatility': np.std(volumes) if len(volumes) > 1 else 0,
            f'{symbol}_volume_trend': self._calculate_trend(volumes),
            
            # Technical indicators
            f'{symbol}_rsi': self._calculate_rsi(prices),
            f'{symbol}_bollinger_position': self._calculate_bollinger_position(prices),
            
            # Time-based features
            f'{symbol}_time_span_seconds': (timestamps[-1] - timestamps[0]).total_seconds(),
            f'{symbol}_avg_tick_interval': self._calculate_avg_interval(timestamps)
        }
        
        return features
    
    def _calculate_momentum(self, prices: List[float], period: int) -> float:
        """Calculate price momentum over period"""
        if len(prices) < period + 1:
            return 0
        return (prices[-1] - prices[-period-1]) / prices[-period-1] * 100 if prices[-period-1] != 0 else 0
    
    def _calculate_trend(self, values: List[float]) -> float:
        """Calculate linear trend slope"""
        if len(values) < 2:
            return 0
        x = np.arange(len(values))
        slope, _ = np.polyfit(x, values, 1)
        return slope
    
    def _calculate_rsi(self, prices: List[float], period: int = 14) -> float:
        """Calculate Relative Strength Index"""
        if len(prices) < period + 1:
            return 50  # Neutral RSI
        
        deltas = np.diff(prices)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        avg_gain = np.mean(gains[-period:]) if len(gains) >= period else 0
        avg_loss = np.mean(losses[-period:]) if len(losses) >= period else 0
        
        if avg_loss == 0:
            return 100
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def _calculate_bollinger_position(self, prices: List[float], period: int = 20) -> float:
        """Calculate position within Bollinger Bands"""
        if len(prices) < period:
            return 0.5  # Middle position
        
        recent_prices = prices[-period:]
        mean_price = np.mean(recent_prices)
        std_price = np.std(recent_prices)
        
        if std_price == 0:
            return 0.5
        
        current_price = prices[-1]
        upper_band = mean_price + (2 * std_price)
        lower_band = mean_price - (2 * std_price)
        
        # Position between 0 (lower band) and 1 (upper band)
        position = (current_price - lower_band) / (upper_band - lower_band)
        return max(0, min(1, position))
    
    def _calculate_avg_interval(self, timestamps: List[datetime]) -> float:
        """Calculate average time interval between ticks"""
        if len(timestamps) < 2:
            return 0
        
        intervals = [(timestamps[i] - timestamps[i-1]).total_seconds() 
                    for i in range(1, len(timestamps))]
        return np.mean(intervals)
    
    def detect_anomaly(self, symbol: str, price: float) -> tuple[bool, float]:
        """Detect price anomaly using river anomaly detection"""
        detector = self.anomaly_detectors[symbol]
        
        # Create feature vector
        features = self.feature_cache.get(symbol, {})
        if not features:
            return False, 0.0
        
        # Use key features for anomaly detection
        feature_vector = {
            'price': price,
            'volatility': features.get(f'{symbol}_price_volatility', 0),
            'momentum': features.get(f'{symbol}_momentum_5', 0),
            'rsi': features.get(f'{symbol}_rsi', 50),
            'bollinger_pos': features.get(f'{symbol}_bollinger_position', 0.5)
        }
        
        # Get anomaly score
        anomaly_score = detector.score_one(feature_vector)
        
        # Update detector
        detector.learn_one(feature_vector)
        
        # Threshold for anomaly (adjust based on requirements)
        is_anomaly = anomaly_score > 0.5
        
        return is_anomaly, anomaly_score


class RealTimeFeatureProcessor:
    """Main processor for real-time feature computation"""
    
    def __init__(self, window_minutes: int = 5):
        self.sliding_features = SlidingWindowFeatures(window_minutes)
        self.analytics = StreamAnalytics()
        self.feature_store: Dict[str, Dict] = {}
        self.feature_freshness: Dict[str, datetime] = {}
        
        # Performance metrics
        self.events_processed = 0
        self.features_computed = 0
        self.anomalies_detected = 0
        
        # Setup anomaly thresholds
        self.analytics.set_anomaly_threshold('price_volatility', 5.0)
        self.analytics.set_anomaly_threshold('volume_spike', 10.0)
        
        logger.info("Real-time feature processor initialized")
    
    def process_market_tick(self, event: StreamEvent):
        """Process market tick event and compute features"""
        try:
            data = event.data
            symbol = data['symbol']
            price = data['price']
            volume = data['volume']
            timestamp = event.timestamp
            
            # Compute sliding window features
            features = self.sliding_features.add_market_tick(symbol, timestamp, price, volume)
            
            if features:
                # Store features
                self.feature_store[symbol] = {
                    'features': features,
                    'timestamp': timestamp,
                    'raw_data': data
                }
                self.feature_freshness[symbol] = timestamp
                self.features_computed += len(features)
                
                # Anomaly detection
                is_anomaly, anomaly_score = self.sliding_features.detect_anomaly(symbol, price)
                
                if is_anomaly:
                    self.anomalies_detected += 1
                    logger.warning(f"Anomaly detected for {symbol}: price={price}, score={anomaly_score:.3f}")
                    self.analytics._trigger_alert(
                        f"Price anomaly in {symbol}: {price} (score: {anomaly_score:.3f})"
                    )
                
                # Check for volume spikes
                volume_mean = features.get(f'{symbol}_volume_mean', 0)
                if volume_mean > 0 and volume > volume_mean * 3:  # 3x average volume
                    logger.info(f"Volume spike detected for {symbol}: {volume} vs avg {volume_mean:.0f}")
                    self.analytics._trigger_alert(
                        f"Volume spike in {symbol}: {volume} vs avg {volume_mean:.0f}"
                    )
            
            self.events_processed += 1
            
            # Log progress every 1000 events
            if self.events_processed % 1000 == 0:
                logger.info(f"Processed {self.events_processed} events, computed {self.features_computed} features")
            
        except Exception as e:
            logger.error(f"Error processing market tick: {e}")
    
    def process_transaction(self, event: StreamEvent):
        """Process transaction event"""
        try:
            data = event.data
            symbol = data['symbol']
            value = data['value']
            timestamp = event.timestamp
            
            # Update transaction-based features
            transaction_features = {
                f'{symbol}_last_transaction_value': value,
                f'{symbol}_last_transaction_time': timestamp.isoformat(),
                f'{symbol}_transaction_type': data['type']
            }
            
            # Merge with existing features
            if symbol in self.feature_store:
                self.feature_store[symbol]['features'].update(transaction_features)
            else:
                self.feature_store[symbol] = {
                    'features': transaction_features,
                    'timestamp': timestamp,
                    'raw_data': data
                }
            
            self.feature_freshness[symbol] = timestamp
            self.events_processed += 1
            
        except Exception as e:
            logger.error(f"Error processing transaction: {e}")
    
    def get_feature_summary(self) -> Dict[str, Any]:
        """Get summary of feature computation performance"""
        return {
            'events_processed': self.events_processed,
            'features_computed': self.features_computed,
            'anomalies_detected': self.anomalies_detected,
            'symbols_tracked': len(self.feature_store),
            'avg_features_per_symbol': self.features_computed / max(len(self.feature_store), 1),
            'stale_features': len(self.get_stale_features())
        }
    
    def get_stale_features(self, max_age_seconds: int = 300) -> List[str]:
        """Get features that haven't been updated recently"""
        stale_features = []
        cutoff_time = datetime.now() - timedelta(seconds=max_age_seconds)
        
        for symbol, last_update in self.feature_freshness.items():
            if last_update < cutoff_time:
                stale_features.append(symbol)
        
        return stale_features
    
    def get_features_for_symbol(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get latest features for specific symbol"""
        return self.feature_store.get(symbol)


class StreamingFeatureConsumer:
    """Consumer that processes streaming events for feature computation"""
    
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        self.feature_processor = RealTimeFeatureProcessor()
        self.consumer = StreamConsumer(
            topics=['market-data', 'transactions', 'portfolio-updates'],
            group_id='feature-computation',
            bootstrap_servers=bootstrap_servers
        )
        
        # Register event handlers
        self.consumer.register_handler('market_tick', self.feature_processor.process_market_tick)
        self.consumer.register_handler('transaction', self.feature_processor.process_transaction)
        
        logger.info("Streaming feature consumer initialized")
    
    def start_processing(self):
        """Start processing streaming events"""
        logger.info("Starting streaming feature computation...")
        
        # Start consumer in separate thread
        consumer_thread = threading.Thread(target=self.consumer.start_consuming)
        consumer_thread.daemon = True
        consumer_thread.start()
        
        # Monitor performance
        self._start_monitoring()
        
        return consumer_thread
    
    def _start_monitoring(self):
        """Start performance monitoring"""
        def monitor():
            while True:
                time.sleep(30)  # Report every 30 seconds
                summary = self.feature_processor.get_feature_summary()
                logger.info(f"Feature Computation Performance: {summary}")
        
        monitor_thread = threading.Thread(target=monitor)
        monitor_thread.daemon = True
        monitor_thread.start()
    
    def stop_processing(self):
        """Stop processing"""
        self.consumer.stop_consuming()
        logger.info("Streaming feature computation stopped")


# Standalone execution for testing
if __name__ == "__main__":
    processor = StreamingFeatureConsumer()
    thread = processor.start_processing()
    
    try:
        thread.join()
    except KeyboardInterrupt:
        processor.stop_processing()
        logger.info("Feature processing interrupted by user")
