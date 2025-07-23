"""
DataLoaders - Load processed data to destinations (Day 1 Essential)
"""

import pandas as pd
import os
from typing import Dict, List, Any, Optional
from abc import ABC, abstractmethod
from datetime import datetime
from loguru import logger


class BaseLoader(ABC):
    """Abstract base class for data loaders"""
    
    @abstractmethod
    def load(self, df: pd.DataFrame, destination: str) -> str:
        """Load DataFrame to destination and return path"""
        pass


class CSVLoader(BaseLoader):
    """Essential CSV loader for Day 1"""
    
    def __init__(self, output_dir: str = "data/processed"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
    
    def load(self, df: pd.DataFrame, filename: str) -> str:
        """Load DataFrame to CSV file and return the file path"""
        try:
            # Add timestamp to filename if not already present
            if not filename.endswith('.csv'):
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"{filename}_{timestamp}.csv"
            
            filepath = os.path.join(self.output_dir, filename)
            
            logger.info(f"Loading {len(df)} records to {filepath}")
            df.to_csv(filepath, index=False)
            
            # Verify file was created
            if os.path.exists(filepath):
                file_size = os.path.getsize(filepath)
                logger.info(f"Successfully loaded data to {filepath} ({file_size} bytes)")
                return filepath
            else:
                logger.error(f"Failed to create file {filepath}")
                raise Exception(f"File creation failed: {filepath}")
                
        except Exception as e:
            logger.error(f"Failed to load data to {filename}: {e}")
            raise
