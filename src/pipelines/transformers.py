"""
Data Transformers - Transform and enrich data (Day 1 Essential)
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime
from abc import ABC, abstractmethod
from loguru import logger


class BaseTransformer(ABC):
    """Abstract base class for data transformers"""
    
    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform DataFrame"""
        pass


class DataTransformer(BaseTransformer):
    """Essential data transformation operations for Day 1"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
    
    def transform(self, df: pd.DataFrame, source_name: str = "unknown") -> pd.DataFrame:
        """Apply essential transformations to DataFrame"""
        logger.info(f"Transforming data from {source_name} ({len(df)} records)")
        
        # Create a copy to avoid modifying original
        transformed_df = df.copy()
        
        # Add metadata columns
        transformed_df = self._add_metadata(transformed_df, source_name)
        
        # Clean data
        transformed_df = self._clean_data(transformed_df)
        
        # Standardize column names
        transformed_df = self._standardize_columns(transformed_df)
        
        # Handle missing values
        transformed_df = self._handle_missing_values(transformed_df)
        
        logger.info(f"Transformation complete. Output: {len(transformed_df)} records, {len(transformed_df.columns)} columns")
        return transformed_df
    
    def _add_metadata(self, df: pd.DataFrame, source_name: str) -> pd.DataFrame:
        """Add metadata columns"""
        from datetime import datetime
        
        df['source_name'] = source_name
        df['processed_at'] = datetime.now()
        df['record_count'] = len(df)
        
        return df
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Essential data cleaning operations"""
        # Remove completely empty rows
        df = df.dropna(how='all')
        
        # Strip whitespace from string columns
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.strip()
        
        return df
    
    def _standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names"""
        # Convert to lowercase and replace spaces/special chars with underscores
        df.columns = (df.columns
                      .str.lower()
                      .str.replace(' ', '_')
                      .str.replace('[^a-zA-Z0-9_]', '_', regex=True)
                      .str.replace('_+', '_', regex=True)
                      .str.strip('_'))
        
        return df
    
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values with basic strategies"""
        for col in df.columns:
            if df[col].dtype in ['int64', 'float64']:
                # Fill numeric columns with median
                df[col] = df[col].fillna(df[col].median())
            elif df[col].dtype == 'object':
                # Fill string columns with 'unknown'
                df[col] = df[col].fillna('unknown')
        
        return df
