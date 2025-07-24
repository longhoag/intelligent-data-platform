"""
Financial Data Stream Producers - Day 3 Real-time Implementation
Generates high-frequency financial market data streams
"""

import asyncio
import json
import random
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any
from dataclasses import dataclass
import pandas as pd
import numpy as np
from loguru import logger

from . import StreamProducer, StreamEvent


@dataclass
class MarketDataPoint:
    """Financial market data point"""
    symbol: str
    timestamp: datetime
    price: float
    volume: int
    bid: float
    ask: float
    spread: float


class FinancialDataProducer:
    """High-frequency financial data producer for real-time streams"""
    
    def __init__(self, producer: StreamProducer):
        self.producer = producer
        self.symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM', 'V', 'JNJ']
        self.base_prices = {symbol: random.uniform(100, 500) for symbol in self.symbols}
        self.running = False
        
    def generate_market_tick(self, symbol: str) -> MarketDataPoint:
        """Generate realistic market tick data"""
        # Simulate realistic price movement
        current_price = self.base_prices[symbol]
        price_change = random.gauss(0, current_price * 0.001)  # 0.1% volatility
        new_price = max(current_price + price_change, 0.01)
        self.base_prices[symbol] = new_price
        
        # Generate bid/ask spread
        spread_pct = random.uniform(0.001, 0.005)  # 0.1-0.5% spread
        spread = new_price * spread_pct
        bid = new_price - spread/2
        ask = new_price + spread/2
        
        # Generate volume
        volume = random.randint(100, 10000)
        
        return MarketDataPoint(
            symbol=symbol,
            timestamp=datetime.now(),
            price=round(new_price, 2),
            volume=volume,
            bid=round(bid, 2),
            ask=round(ask, 2),
            spread=round(spread, 2)
        )
    
    async def start_market_data_stream(self, events_per_second: int = 100):
        """Start high-frequency market data stream"""
        logger.info(f"Starting market data stream at {events_per_second} events/second")
        self.running = True
        
        interval = 1.0 / events_per_second
        
        while self.running:
            try:
                # Generate tick for random symbol
                symbol = random.choice(self.symbols)
                tick = self.generate_market_tick(symbol)
                
                # Create stream event
                event = StreamEvent(
                    event_id=f"market_{symbol}_{int(time.time()*1000)}",
                    timestamp=tick.timestamp,
                    source="market_data",
                    event_type="market_tick",
                    data={
                        "symbol": tick.symbol,
                        "price": tick.price,
                        "volume": tick.volume,
                        "bid": tick.bid,
                        "ask": tick.ask,
                        "spread": tick.spread
                    }
                )
                
                # Produce to Kafka
                self.producer.produce_event("market-data", event, key=symbol)
                
                await asyncio.sleep(interval)
                
            except Exception as e:
                logger.error(f"Error in market data stream: {e}")
                await asyncio.sleep(1)
    
    def stop_stream(self):
        """Stop market data stream"""
        self.running = False
        logger.info("Market data stream stopped")


class TransactionProducer:
    """Trading transaction stream producer"""
    
    def __init__(self, producer: StreamProducer):
        self.producer = producer
        self.symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM', 'V', 'JNJ']
        self.running = False
        
    def generate_transaction(self) -> Dict[str, Any]:
        """Generate realistic trading transaction"""
        symbol = random.choice(self.symbols)
        transaction_type = random.choice(['BUY', 'SELL'])
        quantity = random.randint(1, 1000)
        price = random.uniform(100, 500)
        
        return {
            "transaction_id": f"txn_{int(time.time()*1000)}_{random.randint(1000, 9999)}",
            "symbol": symbol,
            "type": transaction_type,
            "quantity": quantity,
            "price": round(price, 2),
            "value": round(quantity * price, 2),
            "commission": round(price * quantity * 0.001, 2),  # 0.1% commission
            "account_id": f"ACC{random.randint(100000, 999999)}"
        }
    
    async def start_transaction_stream(self, transactions_per_second: int = 50):
        """Start trading transaction stream"""
        logger.info(f"Starting transaction stream at {transactions_per_second} transactions/second")
        self.running = True
        
        interval = 1.0 / transactions_per_second
        
        while self.running:
            try:
                transaction = self.generate_transaction()
                
                event = StreamEvent(
                    event_id=transaction["transaction_id"],
                    timestamp=datetime.now(),
                    source="trading_system",
                    event_type="transaction",
                    data=transaction
                )
                
                self.producer.produce_event("transactions", event, key=transaction["symbol"])
                
                await asyncio.sleep(interval)
                
            except Exception as e:
                logger.error(f"Error in transaction stream: {e}")
                await asyncio.sleep(1)
    
    def stop_stream(self):
        """Stop transaction stream"""
        self.running = False
        logger.info("Transaction stream stopped")


class PortfolioUpdateProducer:
    """Portfolio update stream producer"""
    
    def __init__(self, producer: StreamProducer):
        self.producer = producer
        self.portfolios = [f"PF{i:06d}" for i in range(1000, 2000)]  # 1000 portfolios
        self.running = False
        
    def generate_portfolio_update(self) -> Dict[str, Any]:
        """Generate portfolio value update"""
        portfolio_id = random.choice(self.portfolios)
        
        return {
            "portfolio_id": portfolio_id,
            "total_value": round(random.uniform(10000, 1000000), 2),
            "cash_balance": round(random.uniform(1000, 50000), 2),
            "positions": random.randint(5, 50),
            "daily_pnl": round(random.gauss(0, 1000), 2),
            "risk_score": round(random.uniform(0, 10), 2)
        }
    
    async def start_portfolio_stream(self, updates_per_second: int = 20):
        """Start portfolio update stream"""
        logger.info(f"Starting portfolio stream at {updates_per_second} updates/second")
        self.running = True
        
        interval = 1.0 / updates_per_second
        
        while self.running:
            try:
                update = self.generate_portfolio_update()
                
                event = StreamEvent(
                    event_id=f"portfolio_{update['portfolio_id']}_{int(time.time()*1000)}",
                    timestamp=datetime.now(),
                    source="portfolio_system",
                    event_type="portfolio_update",
                    data=update
                )
                
                self.producer.produce_event("portfolio-updates", event, key=update["portfolio_id"])
                
                await asyncio.sleep(interval)
                
            except Exception as e:
                logger.error(f"Error in portfolio stream: {e}")
                await asyncio.sleep(1)
    
    def stop_stream(self):
        """Stop portfolio stream"""
        self.running = False
        logger.info("Portfolio stream stopped")


class MultiStreamProducer:
    """Orchestrates multiple financial data streams"""
    
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        self.producer = StreamProducer(bootstrap_servers)
        self.market_producer = FinancialDataProducer(self.producer)
        self.transaction_producer = TransactionProducer(self.producer)
        self.portfolio_producer = PortfolioUpdateProducer(self.producer)
        self.running = False
        
    async def start_all_streams(self, target_events_per_second: int = 1000):
        """Start all financial data streams to achieve target throughput"""
        logger.info(f"Starting all streams targeting {target_events_per_second} events/second")
        
        # Distribute events across stream types
        market_eps = int(target_events_per_second * 0.6)  # 60% market data
        transaction_eps = int(target_events_per_second * 0.3)  # 30% transactions
        portfolio_eps = int(target_events_per_second * 0.1)  # 10% portfolio updates
        
        # Connect producer
        self.producer.connect()
        
        # Start all streams concurrently
        tasks = [
            self.market_producer.start_market_data_stream(market_eps),
            self.transaction_producer.start_transaction_stream(transaction_eps),
            self.portfolio_producer.start_portfolio_stream(portfolio_eps)
        ]
        
        self.running = True
        logger.info(f"All streams started: {market_eps} market + {transaction_eps} transactions + {portfolio_eps} portfolio")
        
        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            logger.info("Stream interrupted by user")
        finally:
            self.stop_all_streams()
    
    def stop_all_streams(self):
        """Stop all streams"""
        logger.info("Stopping all streams...")
        self.running = False
        self.market_producer.stop_stream()
        self.transaction_producer.stop_stream()
        self.portfolio_producer.stop_stream()
        self.producer.close()
        logger.info("All streams stopped")


# Standalone script execution
if __name__ == "__main__":
    async def main():
        producer = MultiStreamProducer()
        await producer.start_all_streams(1000)  # 1000 events/second
    
    asyncio.run(main())
