"""
Data Extractors - Extract data from various sources (Day 1 Essential)
"""

import pandas as pd
import sqlite3
import requests
import os
import numpy as np
import logging
import time
import random
from typing import Dict, List, Any, Optional
from abc import ABC, abstractmethod
from loguru import logger


class BaseExtractor(ABC):
    """Abstract base class for data extractors"""
    
    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """Extract data and return as DataFrame"""
        pass
    
    @abstractmethod
    def validate_source(self) -> bool:
        """Validate data source is accessible"""
        pass


class APIExtractor(BaseExtractor):
    """Extract data from REST APIs"""
    
    def __init__(self, base_url: str, timeout: int = 30, max_retries: int = 3):
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()
    
    def extract(self, endpoint: str) -> pd.DataFrame:
        """Extract data from API endpoint"""
        url = f"{self.base_url}{endpoint}"
        
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Extracting data from {url} (attempt {attempt + 1})")
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()
                
                data = response.json()
                df = pd.DataFrame(data)
                
                logger.info(f"Successfully extracted {len(df)} records from {endpoint}")
                return df
                
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"Failed to extract from {url} after {self.max_retries} attempts")
                    raise
    
    def validate_source(self) -> bool:
        """Validate API is accessible"""
        try:
            response = self.session.get(self.base_url, timeout=10)
            return response.status_code < 500
        except requests.RequestException:
            return False


class FileExtractor(BaseExtractor):
    """Extract data from files with real data download capabilities"""
    
    def __init__(self, file_path: str = None, file_format: str = "csv", **kwargs):
        self.file_path = file_path
        self.file_format = file_format.lower()
        self.read_kwargs = kwargs
    
    def download_file(self, url: str, local_path: str) -> bool:
        """Download file from URL to local path"""
        try:
            logger.info(f"Downloading file from {url} to {local_path}")
            
            # Create directory if it doesn't exist
            import os
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            response = requests.get(url, stream=True, timeout=120)
            response.raise_for_status()
            
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"Successfully downloaded file to {local_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to download file from {url}: {e}")
            return False
    
    def extract(self, file_path: str = None, file_url: str = None) -> pd.DataFrame:
        """Extract data from file, downloading if URL provided"""
        target_path = file_path or self.file_path
        
        # Download file if URL is provided
        if file_url:
            if not self.download_file(file_url, target_path):
                raise Exception(f"Failed to download file from {file_url}")
        
        try:
            logger.info(f"Extracting data from {target_path}")
            
            if self.file_format == "csv":
                df = pd.read_csv(target_path, **self.read_kwargs)
            elif self.file_format == "json":
                df = pd.read_json(target_path, **self.read_kwargs)
            elif self.file_format == "excel":
                df = pd.read_excel(target_path, **self.read_kwargs)
            elif self.file_format == "parquet":
                df = pd.read_parquet(target_path, **self.read_kwargs)
            else:
                raise ValueError(f"Unsupported file format: {self.file_format}")
            
            logger.info(f"Successfully extracted {len(df)} records from {target_path}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract data from {target_path}: {e}")
            raise
    
    def validate_source(self) -> bool:
        """Validate file exists and is readable"""
        try:
            import os
            return os.path.exists(self.file_path) and os.access(self.file_path, os.R_OK)
        except Exception:
            return False


class DatabaseExtractor(BaseExtractor):
    """Extract data from SQLite databases"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    def extract(self, query: str) -> pd.DataFrame:
        """Extract data using SQL query"""
        try:
            import sqlite3
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql(query, conn)
            conn.close()
            
            logger.info(f"Extracted {len(df)} records from database")
            return df
            
        except Exception as e:
            logger.error(f"Database extraction failed: {e}")
            raise
    
    def validate_source(self) -> bool:
        """Validate database file exists and is accessible"""
        try:
            import sqlite3
            import os
            if not os.path.exists(self.db_path):
                return False
            conn = sqlite3.connect(self.db_path)
            conn.close()
            return True
        except Exception:
            return False


def create_financial_database() -> None:
    """Create financial trading database with realistic market data"""
    import sqlite3
    import os
    from datetime import datetime, timedelta
    import random
    
    os.makedirs("data", exist_ok=True)
    db_path = "data/financial_database.db"
    
    # Remove existing database to avoid conflicts
    if os.path.exists(db_path):
        os.remove(db_path)
    
    # Create database and tables
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create portfolios table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS portfolios (
            portfolio_id INTEGER PRIMARY KEY,
            account_id TEXT,
            account_name TEXT,
            total_value REAL,
            cash_balance REAL,
            created_date DATE,
            last_updated DATE,
            risk_level TEXT,
            investment_strategy TEXT
        )
    ''')
    
    # Create transactions table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id INTEGER PRIMARY KEY,
            portfolio_id INTEGER,
            symbol TEXT,
            company_name TEXT,
            transaction_type TEXT,
            quantity INTEGER,
            price REAL,
            transaction_date DATETIME,
            commission REAL,
            total_value REAL,
            FOREIGN KEY (portfolio_id) REFERENCES portfolios (portfolio_id)
        )
    ''')
    
    # Create market_data table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS market_data (
            id INTEGER PRIMARY KEY,
            symbol TEXT,
            date DATE,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            adjusted_close REAL,
            sector TEXT,
            market_cap REAL
        )
    ''')
    
    # Generate portfolio data (1000 portfolios)
    portfolios = []
    strategies = ["Growth", "Value", "Income", "Balanced", "Aggressive"]
    risk_levels = ["Conservative", "Moderate", "Aggressive", "Very Aggressive"]
    
    for i in range(1, 1001):
        created_date = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1400))
        portfolios.append((
            i,
            f"ACC{i:06d}",
            f"Account Holder {i}",
            round(random.uniform(10000, 1000000), 2),
            round(random.uniform(1000, 50000), 2),
            created_date.strftime('%Y-%m-%d'),
            (created_date + timedelta(days=random.randint(1, 100))).strftime('%Y-%m-%d'),
            random.choice(risk_levels),
            random.choice(strategies)
        ))
    
    cursor.executemany('''
        INSERT INTO portfolios VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', portfolios)
    
    # Generate transaction data (15000 transactions)
    transactions = []
    symbols = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "META", "NVDA", "JPM", "JNJ", "V", 
              "PG", "UNH", "HD", "MA", "BAC", "XOM", "DIS", "ADBE", "CRM", "NFLX"]
    companies = {
        "AAPL": "Apple Inc.", "GOOGL": "Alphabet Inc.", "MSFT": "Microsoft Corp.",
        "AMZN": "Amazon.com Inc.", "TSLA": "Tesla Inc.", "META": "Meta Platforms Inc.",
        "NVDA": "NVIDIA Corp.", "JPM": "JPMorgan Chase", "JNJ": "Johnson & Johnson",
        "V": "Visa Inc.", "PG": "Procter & Gamble", "UNH": "UnitedHealth Group"
    }
    transaction_types = ["BUY", "SELL"]
    
    for i in range(1, 15001):
        symbol = random.choice(symbols)
        transaction_date = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1400))
        price = round(random.uniform(50, 500), 2)
        quantity = random.randint(1, 1000)
        commission = round(random.uniform(5, 25), 2)
        
        transactions.append((
            i,
            random.randint(1, 1000),  # portfolio_id
            symbol,
            companies.get(symbol, f"{symbol} Corp."),
            random.choice(transaction_types),
            quantity,
            price,
            transaction_date.strftime('%Y-%m-%d %H:%M:%S'),
            commission,
            round(quantity * price + commission, 2)
        ))
    
    cursor.executemany('''
        INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', transactions)
    
    # Generate market data (25000 records)
    market_data = []
    sectors = ["Technology", "Healthcare", "Financial", "Consumer", "Energy", "Industrial"]
    
    for i in range(1, 25001):
        symbol = random.choice(symbols)
        date = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1400))
        open_price = round(random.uniform(50, 500), 2)
        high_price = round(open_price * random.uniform(1.0, 1.1), 2)
        low_price = round(open_price * random.uniform(0.9, 1.0), 2)
        close_price = round(random.uniform(low_price, high_price), 2)
        volume = random.randint(100000, 50000000)
        
        market_data.append((
            i,
            symbol,
            date.strftime('%Y-%m-%d'),
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            round(close_price * random.uniform(0.98, 1.02), 2),  # adjusted_close
            random.choice(sectors),
            random.randint(1000000000, 2000000000000)  # market_cap
        ))
    
    cursor.executemany('''
        INSERT INTO market_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', market_data)
    
    conn.commit()
    conn.close()
    
    total_records = 1000 + 15000 + 25000
    logger.info(f"Created financial database with {total_records:,} total records: {db_path}")
    logger.info(f"  - Portfolios: 1,000 records")
    logger.info(f"  - Transactions: 15,000 records")
    logger.info(f"  - Market Data: 25,000 records")
