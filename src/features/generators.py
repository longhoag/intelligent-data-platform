"""
Simple Feature Generators for Day 2 requirements
Minimal implementation focused on 20+ features with monitoring
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional
from datetime import datetime
from loguru import logger


def generate_basic_features(df: pd.DataFrame) -> pd.DataFrame:
    """Generate basic feature set to meet Day 2 requirements"""
    logger.info("Generating basic feature set")
    
    result_df = df.copy()
    
    # 1. Time features (if timestamp exists)
    if 'timestamp' in df.columns:
        result_df['hour'] = pd.to_datetime(df['timestamp']).dt.hour
        result_df['day'] = pd.to_datetime(df['timestamp']).dt.day
        result_df['month'] = pd.to_datetime(df['timestamp']).dt.month
        result_df['is_weekend'] = (pd.to_datetime(df['timestamp']).dt.dayofweek >= 5).astype(int)
    
    # 2. Statistical features for numeric columns
    numeric_cols = df.select_dtypes(include=[np.number]).columns[:3]  # Limit to 3 columns
    
    for col in numeric_cols:
        if col in df.columns:
            # Rolling features
            result_df[f'{col}_rolling_mean_5'] = df[col].rolling(5, min_periods=1).mean()
            result_df[f'{col}_rolling_std_5'] = df[col].rolling(5, min_periods=1).std()
            result_df[f'{col}_pct_change'] = df[col].pct_change()
    
    # 3. Simple interactions (first 2 numeric columns)
    if len(numeric_cols) >= 2:
        col1, col2 = numeric_cols[0], numeric_cols[1]
        result_df[f'{col1}_x_{col2}'] = df[col1] * df[col2]
        result_df[f'{col1}_plus_{col2}'] = df[col1] + df[col2]
    
    logger.info(f"Generated {len(result_df.columns) - len(df.columns)} new features")
    return result_df


def monitor_features(df: pd.DataFrame) -> Dict:
    """Simple feature monitoring for Day 2 requirements"""
    logger.info("Monitoring feature quality")
    
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    
    monitoring_report = {
        'timestamp': datetime.now(),
        'total_features': len(df.columns),
        'numeric_features': len(numeric_cols),
        'missing_values': df.isnull().sum().sum(),
        'data_quality_score': (1 - df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100
    }
    
    logger.info(f"Feature monitoring complete: {monitoring_report['data_quality_score']:.1f}% data quality")
    return monitoring_report
