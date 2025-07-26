"""
Data extraction interfaces and implementations for production systems.
Supports real-time API data, file processing, and database queries.
"""

import os
import sqlite3
import time
from abc import ABC, abstractmethod

import pandas as pd
import requests
from loguru import logger


class BaseExtractor(ABC):
    """Base class for all data extractors"""
    
    @abstractmethod
    def extract(self, *args, **kwargs) -> pd.DataFrame:
        """Extract data and return as DataFrame"""
        raise NotImplementedError
    
    def validate_source(self) -> bool:
        """Validate data source is available - default implementation"""
        return True


class AlphaVantageExtractor(BaseExtractor):
    """Production-ready Alpha Vantage API extractor with rate limiting"""
    
    def __init__(self, api_key: str):
        """Initialize Alpha Vantage extractor
        
        Args:
            api_key: Alpha Vantage API key
        """
        self.api_key = api_key
        self.base_url = "https://www.alphavantage.co/query"
        self.last_request_time = 0
        self.min_request_interval = 12  # 5 calls per minute limit
        self.timeout = 30
        self.retries = 3
        
    def _wait_for_rate_limit(self) -> None:
        """Ensure we don't exceed API rate limits"""
        time_since_last = time.time() - self.last_request_time
        if time_since_last < self.min_request_interval:
            wait_time = self.min_request_interval - time_since_last
            logger.info(f"Rate limiting: waiting {wait_time:.1f}s")
            time.sleep(wait_time)
    
    def extract(self, *args, **kwargs) -> pd.DataFrame:
        """Extract daily time series data for a stock symbol"""
        # Get symbol from args or kwargs
        symbol = args[0] if args else kwargs.get('symbol', 'AAPL')
        
        self._wait_for_rate_limit()
        
        params = {
            'function': 'TIME_SERIES_DAILY',
            'symbol': symbol,
            'apikey': self.api_key,
            'outputsize': 'compact'  # Last 100 data points
        }
        
        for attempt in range(self.retries):
            try:
                logger.info(f"Fetching {symbol} data from Alpha Vantage (attempt {attempt + 1})")
                
                response = requests.get(self.base_url, params=params, timeout=self.timeout)
                response.raise_for_status()
                
                data = response.json()
                self.last_request_time = time.time()
                
                # Check for API errors
                if "Error Message" in data:
                    raise ValueError(f"API Error: {data['Error Message']}")
                
                if "Note" in data:
                    logger.warning(f"API Note: {data['Note']}")
                    time.sleep(60)  # Wait a minute if we hit rate limit
                    continue
                
                # Extract time series data
                time_series_key = "Time Series (Daily)"
                if time_series_key not in data:
                    available_keys = list(data.keys())
                    error_msg = f"Expected key '{time_series_key}' not found. " \
                               f"Available keys: {available_keys}"
                    raise KeyError(error_msg)
                
                time_series = data[time_series_key]
                
                # Convert to DataFrame
                df = pd.DataFrame.from_dict(time_series, orient='index')
                df.index = pd.to_datetime(df.index)
                df = df.sort_index()
                
                # Clean column names
                df.columns = ['open', 'high', 'low', 'close', 'volume']
                
                # Convert to numeric
                numeric_columns = ['open', 'high', 'low', 'close', 'volume']
                for col in numeric_columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Add symbol and metadata
                df['symbol'] = symbol
                df['timestamp'] = pd.Timestamp.now()
                df['data_source'] = 'alpha_vantage'
                
                logger.info(f"Successfully extracted {len(df)} records for {symbol}")
                return df
                
            except requests.RequestException as e:
                logger.error(f"Request failed for {symbol}: {e}")
                if attempt == self.retries - 1:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff
                
            except (ValueError, KeyError) as e:
                logger.error(f"Data processing error for {symbol}: {e}")
                raise
    
    def validate_source(self) -> bool:
        """Validate Alpha Vantage API is accessible"""
        try:
            # Test with a simple query
            test_params = {
                'function': 'TIME_SERIES_DAILY',
                'symbol': 'AAPL',
                'apikey': self.api_key,
                'outputsize': 'compact'
            }
            
            response = requests.get(self.base_url, params=test_params, timeout=10)
            data = response.json()
            
            # Check if we get valid response structure
            return "Time Series (Daily)" in data or "Note" in data
            
        except Exception:
            return False


class APIExtractor(BaseExtractor):
    """Generic REST API extractor with session management"""
    
    def __init__(self, base_url: str, headers: dict = None, max_retries: int = 3):
        self.base_url = base_url
        self.max_retries = max_retries
        self.session = requests.Session()
        if headers:
            self.session.headers.update(headers)
    
    def extract(self, *args, **kwargs) -> pd.DataFrame:
        """Extract data from API endpoint"""
        endpoint = args[0] if args else kwargs.get('endpoint', '')
        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Extracting from {url} (attempt {attempt + 1})")
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                if isinstance(data, list):
                    df = pd.DataFrame(data)
                else:
                    df = pd.json_normalize(data)
                
                logger.info(f"Successfully extracted {len(df)} records from {url}")
                return df
                
            except requests.RequestException as e:
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"Request failed, retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)
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
    """Extract data from files with download capabilities"""
    
    def __init__(self, file_path: str = None, file_format: str = "csv", **kwargs):
        self.file_path = file_path
        self.file_format = file_format.lower()
        self.read_kwargs = kwargs
    
    def download_file(self, url: str, local_path: str) -> bool:
        """Download file from URL to local path"""
        try:
            logger.info(f"Downloading file from {url} to {local_path}")
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            response = requests.get(url, stream=True, timeout=120)
            response.raise_for_status()
            
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"Successfully downloaded file to {local_path}")
            return True
            
        except requests.RequestException as e:
            logger.error(f"Failed to download file from {url}: {e}")
            return False
    
    def extract(self, *args, **kwargs) -> pd.DataFrame:
        """Extract data from file, downloading if URL provided"""
        file_path = kwargs.get('file_path') or self.file_path
        file_url = kwargs.get('file_url')
        
        # Download file if URL is provided
        if file_url:
            if not self.download_file(file_url, file_path):
                raise RuntimeError(f"Failed to download file from {file_url}")
        
        try:
            logger.info(f"Extracting data from {file_path}")
            
            if self.file_format == "csv":
                df = pd.read_csv(file_path, **self.read_kwargs)
            elif self.file_format == "json":
                df = pd.read_json(file_path, **self.read_kwargs)
            elif self.file_format == "excel":
                df = pd.read_excel(file_path, **self.read_kwargs)
            elif self.file_format == "parquet":
                df = pd.read_parquet(file_path, **self.read_kwargs)
            else:
                raise ValueError(f"Unsupported file format: {self.file_format}")
            
            logger.info(f"Successfully extracted {len(df)} records from {file_path}")
            return df
            
        except (IOError, pd.errors.EmptyDataError, pd.errors.ParserError) as e:
            logger.error(f"Failed to extract data from {file_path}: {e}")
            raise
    
    def validate_source(self) -> bool:
        """Validate file exists and is readable"""
        try:
            return os.path.exists(self.file_path) and os.access(self.file_path, os.R_OK)
        except (TypeError, OSError):
            return False


class DatabaseExtractor(BaseExtractor):
    """Extract data from SQLite databases"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    def extract(self, *args, **kwargs) -> pd.DataFrame:
        """Extract data using SQL query"""
        query = args[0] if args else kwargs.get('query', 'SELECT * FROM sqlite_master')
        
        try:
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql(query, conn)
            conn.close()
            
            logger.info(f"Extracted {len(df)} records from database")
            return df
            
        except (sqlite3.Error, pd.errors.DatabaseError) as e:
            logger.error(f"Database extraction failed: {e}")
            raise
    
    def validate_source(self) -> bool:
        """Validate database file exists and is accessible"""
        try:
            if not os.path.exists(self.db_path):
                return False
            conn = sqlite3.connect(self.db_path)
            conn.close()
            return True
        except (sqlite3.Error, OSError):
            return False
