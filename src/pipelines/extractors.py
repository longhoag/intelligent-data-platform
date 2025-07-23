"""
Data Extractors - Extract data from various sources
"""

import requests
import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Any, Optional
from abc import ABC, abstractmethod
import time

logger = logging.getLogger(__name__)


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
    """Extract data from files"""
    
    def __init__(self, file_path: str, file_format: str = "csv", **kwargs):
        self.file_path = file_path
        self.file_format = file_format.lower()
        self.read_kwargs = kwargs
    
    def extract(self) -> pd.DataFrame:
        """Extract data from file"""
        try:
            logger.info(f"Extracting data from {self.file_path}")
            
            if self.file_format == "csv":
                df = pd.read_csv(self.file_path, **self.read_kwargs)
            elif self.file_format == "json":
                df = pd.read_json(self.file_path, **self.read_kwargs)
            elif self.file_format == "parquet":
                df = pd.read_parquet(self.file_path, **self.read_kwargs)
            else:
                raise ValueError(f"Unsupported file format: {self.file_format}")
            
            logger.info(f"Successfully extracted {len(df)} records from {self.file_path}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract from {self.file_path}: {e}")
            raise
    
    def validate_source(self) -> bool:
        """Validate file exists and is readable"""
        try:
            import os
            return os.path.exists(self.file_path) and os.access(self.file_path, os.R_OK)
        except Exception:
            return False


def create_sample_data() -> None:
    """Create sample data file for testing - 10,000+ records for Day 1 requirements"""
    import os
    os.makedirs("data", exist_ok=True)
    
    # Create large sample CSV data (12,000 records to exceed 10k requirement)
    sample_data = pd.DataFrame({
        'id': range(1, 12001),
        'value': [f"sample_value_{i}" for i in range(1, 12001)],
        'category': ['A', 'B', 'C', 'D', 'E'] * 2400,
        'score': [i * 0.1 for i in range(1, 12001)],
        'timestamp': pd.date_range('2025-01-01', periods=12000, freq='1min'),
        'amount': np.random.uniform(10, 1000, 12000).round(2),
        'status': ['active', 'inactive', 'pending'] * 4000
    })
    
    sample_data.to_csv("data/sample_data.csv", index=False)
    logger.info("Created sample data file with 12,000 records: data/sample_data.csv")


def create_sample_database() -> None:
    """Create sample SQLite database with 10,000+ records"""
    import sqlite3
    import os
    
    os.makedirs("data", exist_ok=True)
    db_path = "data/sample_database.db"
    
    # Create database and tables
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create customers table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT,
            signup_date DATE,
            total_orders INTEGER
        )
    ''')
    
    # Create orders table  
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            product_name TEXT,
            amount REAL,
            order_date DATETIME
        )
    ''')
    
    # Insert 5000 customers
    customers_data = []
    for i in range(1, 5001):
        customers_data.append((
            i,
            f'Customer_{i}',
            f'customer{i}@example.com',
            f'2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}',
            np.random.randint(1, 50)
        ))
    
    cursor.executemany(
        'INSERT OR REPLACE INTO customers VALUES (?, ?, ?, ?, ?)',
        customers_data
    )
    
    # Insert 8000 orders
    orders_data = []
    for i in range(1, 8001):
        orders_data.append((
            i,
            np.random.randint(1, 5001),
            f'Product_{np.random.randint(1, 500)}',
            round(np.random.uniform(10, 500), 2),
            f'2024-{np.random.randint(1, 13):02d}-{np.random.randint(1, 29):02d} {np.random.randint(0, 24):02d}:{np.random.randint(0, 60):02d}:00'
        ))
    
    cursor.executemany(
        'INSERT OR REPLACE INTO orders VALUES (?, ?, ?, ?, ?)',
        orders_data
    )
    
    conn.commit()
    conn.close()
    
    logger.info("Created sample database with 13,000 records: data/sample_database.db")


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
