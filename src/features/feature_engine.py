"""
Day 2: Feature Engineering Engine
Automated feature generation system for financial data
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from loguru import logger

# Feature engineering libraries (with fallbacks)
try:
    from sklearn.feature_selection import SelectKBest, mutual_info_regression
    from sklearn.ensemble import RandomForestRegressor
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False
    logger.warning("scikit-learn not available, using basic implementations")


@dataclass
class FeatureConfig:
    """Configuration for feature engineering pipeline"""
    # Core settings
    sliding_window_sizes: List[int] = field(default_factory=lambda: [3, 5, 10, 20])
    max_features: int = 30
    selection_method: str = 'rf_importance'  # 'rf_importance', 'correlation'


@dataclass
class FeatureImportance:
    """Feature importance tracking"""
    feature_name: str
    importance_score: float
    method: str


@dataclass
class FeatureReport:
    """Feature engineering report"""
    total_features_generated: int
    total_features_selected: int
    feature_importance: List[FeatureImportance]
    execution_time: float
    timestamp: datetime


class FinancialFeatureEngine:
    """Automated feature engineering engine for financial data"""
    
    def __init__(self, config: Optional[FeatureConfig] = None):
        self.config = config or FeatureConfig()
        self.feature_names_: List[str] = []
        self.selected_features_: List[str] = []
        self.feature_importance_: List[FeatureImportance] = []
        
    def generate_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generate comprehensive feature set from financial data"""
        start_time = datetime.now()
        logger.info(f"Starting feature generation for {len(df)} records with {len(df.columns)} base features")
        
        # Create copy to avoid modifying original data
        feature_df = df.copy()
        original_cols = list(df.columns)
        
        # 1. Time-based features
        feature_df = self._generate_time_features(feature_df)
            
        # 2. Statistical rolling window features
        feature_df = self._generate_statistical_features(feature_df, original_cols)
            
        # 3. Technical indicators (financial specific)
        feature_df = self._generate_technical_indicators(feature_df)
        
        # 4. Simple interaction features
        feature_df = self._generate_interaction_features(feature_df, original_cols)
        
        # Store feature names
        self.feature_names_ = [col for col in feature_df.columns if col not in original_cols]
        
        execution_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"Generated {len(self.feature_names_)} new features in {execution_time:.2f} seconds")
        
        return feature_df
    
    def select_features(self, df: pd.DataFrame, target_col: str) -> pd.DataFrame:
        """Select best features using Random Forest or correlation"""
        logger.info(f"Starting feature selection from {len(df.columns)} features")
        
        # Prepare data - only use numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if target_col not in numeric_cols:
            logger.warning(f"Target column {target_col} is not numeric, falling back to correlation selection")
            return self._correlation_based_selection(df, target_col)
        
        # Remove target from features and keep only numeric columns
        feature_cols = [col for col in numeric_cols if col != target_col]
        X = df[feature_cols]
        y = df[target_col]
        
        # Handle missing values
        X = X.fillna(X.mean())
        y = y.fillna(y.mean())
        
        # Feature selection based on method
        if self.config.selection_method == 'rf_importance' and HAS_SKLEARN:
            # Use Random Forest for feature importance
            rf = RandomForestRegressor(n_estimators=50, random_state=42, n_jobs=-1)
            rf.fit(X, y)
            feature_importance = pd.DataFrame({
                'feature': X.columns,
                'importance': rf.feature_importances_
            }).sort_values('importance', ascending=False)
            
            selected_features = feature_importance.head(self.config.max_features)['feature'].tolist()
            self.selected_features_ = selected_features + [target_col]
            
            # Store feature importance
            for _, row in feature_importance.head(10).iterrows():
                self.feature_importance_.append(FeatureImportance(
                    feature_name=row['feature'],
                    importance_score=row['importance'],
                    method='random_forest'
                ))
            
            logger.info(f"Selected {len(selected_features)} features using Random Forest importance")
            return df[self.selected_features_]
        else:
            # Fallback: correlation-based selection
            return self._correlation_based_selection(df, target_col)
    
    def _generate_time_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generate basic time-based features"""
        logger.info("Generating time-based features")
        
        # Look for datetime columns or create timestamp
        date_cols = df.select_dtypes(include=['datetime64']).columns
        if len(date_cols) == 0:
            # Create synthetic timestamp if none exists
            df['timestamp'] = pd.date_range(start='2024-01-01', periods=len(df), freq='1H')
            date_cols = ['timestamp']
        
        for col in date_cols:
            if col in df.columns:
                # Extract basic time components
                df[f'{col}_hour'] = df[col].dt.hour
                df[f'{col}_day'] = df[col].dt.day
                df[f'{col}_month'] = df[col].dt.month
                df[f'{col}_quarter'] = df[col].dt.quarter
                df[f'{col}_is_weekend'] = (df[col].dt.dayofweek >= 5).astype(int)
        
        return df
    
    def _generate_statistical_features(self, df: pd.DataFrame, numeric_cols: List[str]) -> pd.DataFrame:
        """Generate statistical rolling window features"""
        logger.info(f"Generating statistical features with windows: {self.config.sliding_window_sizes}")
        
        # Get numeric columns only
        numeric_data = df.select_dtypes(include=[np.number])
        base_numeric_cols = [col for col in numeric_cols if col in numeric_data.columns][:5]  # Limit to 5 columns
        
        for col in base_numeric_cols:
            if col in df.columns:
                for window in self.config.sliding_window_sizes:
                    window = min(window, len(df) // 2)  # Ensure window size is reasonable
                    
                    if window >= 2:
                        # Core rolling statistics
                        df[f'{col}_rolling_mean_{window}'] = df[col].rolling(window, min_periods=1).mean()
                        df[f'{col}_rolling_std_{window}'] = df[col].rolling(window, min_periods=1).std()
                        df[f'{col}_change_{window}'] = df[col].pct_change(window)
        
        return df
    
    def _generate_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generate financial technical indicators"""
        logger.info("Generating technical indicators")
        
        # Look for OHLCV columns
        price_cols = {}
        for col in df.columns:
            col_lower = col.lower()
            if 'open' in col_lower:
                price_cols['open'] = col
            elif 'high' in col_lower:
                price_cols['high'] = col
            elif 'low' in col_lower:
                price_cols['low'] = col
            elif 'close' in col_lower:
                price_cols['close'] = col
            elif 'volume' in col_lower:
                price_cols['volume'] = col
        
        # Generate indicators if we have price data
        if 'close' in price_cols:
            close_col = price_cols['close']
            
            # Simple Moving Averages
            for period in [5, 10, 20, 50]:
                if period < len(df):
                    df[f'sma_{period}'] = df[close_col].rolling(period, min_periods=1).mean()
                    
            # Price ratios and relative indicators
            if 'sma_20' in df.columns:
                df['price_to_sma20'] = df[close_col] / df['sma_20']
            
            # Volatility
            df['price_volatility_10'] = df[close_col].rolling(10, min_periods=1).std()
            
            # Price momentum
            df['price_momentum_5'] = df[close_col] / df[close_col].shift(5) - 1
            df['price_momentum_10'] = df[close_col] / df[close_col].shift(10) - 1
        
        if 'high' in price_cols and 'low' in price_cols:
            # High-Low spread
            df['hl_spread'] = df[price_cols['high']] - df[price_cols['low']]
            df['hl_ratio'] = df[price_cols['high']] / df[price_cols['low']]
        
        if 'volume' in price_cols:
            # Volume indicators
            vol_col = price_cols['volume']
            df['volume_sma_10'] = df[vol_col].rolling(10, min_periods=1).mean()
            if 'volume_sma_10' in df.columns:
                df['volume_ratio'] = df[vol_col] / df['volume_sma_10']
        
        return df
    
    def _generate_interaction_features(self, df: pd.DataFrame, base_cols: List[str]) -> pd.DataFrame:
        """Generate simple interaction features between numeric columns"""
        logger.info("Generating interaction features")
        
        numeric_cols = [col for col in base_cols if col in df.select_dtypes(include=[np.number]).columns][:4]  # Limit to 4 columns
        
        interactions_created = 0
        max_interactions = 10  # Limit total interactions
        
        for i, col1 in enumerate(numeric_cols):
            for col2 in numeric_cols[i+1:]:
                if interactions_created >= max_interactions:
                    break
                    
                if col1 in df.columns and col2 in df.columns:
                    # Simple multiplicative interaction
                    df[f'{col1}_x_{col2}'] = df[col1] * df[col2]
                    interactions_created += 1
                    
            if interactions_created >= max_interactions:
                break
        
        logger.info(f"Created {interactions_created} interaction features")
        return df
    
    def _correlation_based_selection(self, df: pd.DataFrame, target_col: str) -> pd.DataFrame:
        """Fallback feature selection using correlation"""
        logger.info("Using correlation-based feature selection")
        
        numeric_df = df.select_dtypes(include=[np.number])
        if target_col not in numeric_df.columns:
            return df
            
        # Calculate correlations with target
        correlations = numeric_df.corr()[target_col].abs().sort_values(ascending=False)
        
        # Select top features
        selected_features = correlations.head(self.config.max_features + 1).index.tolist()
        
        # Store feature importance
        for feature in selected_features:
            if feature != target_col:
                self.feature_importance_.append(FeatureImportance(
                    feature_name=feature,
                    importance_score=correlations[feature],
                    method='correlation'
                ))
        
        self.selected_features_ = selected_features
        logger.info(f"Selected {len(selected_features)-1} features using correlation")
        
        return df[selected_features]
    
    def fit_transform(self, df: pd.DataFrame, target_col: Optional[str] = None) -> Tuple[pd.DataFrame, FeatureReport]:
        """Complete feature engineering pipeline"""
        start_time = datetime.now()
        logger.info("Starting complete feature engineering pipeline")
        
        # Generate features
        feature_df = self.generate_features(df)
        
        # Select features if target is provided
        if target_col and target_col in feature_df.columns:
            feature_df = self.select_features(feature_df, target_col)
        
        # Generate report
        execution_time = (datetime.now() - start_time).total_seconds()
        report = FeatureReport(
            total_features_generated=len(self.feature_names_),
            total_features_selected=len(self.selected_features_) if self.selected_features_ else len(feature_df.columns),
            feature_importance=self.feature_importance_,
            execution_time=execution_time,
            timestamp=datetime.now()
        )
        
        logger.info(f"Feature engineering completed in {execution_time:.2f} seconds")
        logger.info(f"Generated {report.total_features_generated} features, selected {report.total_features_selected}")
        
        return feature_df, report
    
    def get_feature_importance(self, top_n: int = 10) -> List[FeatureImportance]:
        """Get top N most important features"""
        sorted_importance = sorted(self.feature_importance_, 
                                 key=lambda x: x.importance_score, reverse=True)
        return sorted_importance[:top_n]


class FeatureMonitor:
    """Simple feature performance monitoring"""
    
    def __init__(self):
        self.baseline_stats: Dict[str, Dict[str, float]] = {}
    
    def establish_baseline(self, df: pd.DataFrame) -> None:
        """Establish baseline statistics for features"""
        logger.info("Establishing feature baseline statistics")
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            if col in df.columns:
                self.baseline_stats[col] = {
                    'mean': df[col].mean(),
                    'std': df[col].std(),
                    'min': df[col].min(),
                    'max': df[col].max()
                }
