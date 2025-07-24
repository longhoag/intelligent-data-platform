"""
Data Validation Framework - Day 5
Custom data quality validation without Great Expectations dependency conflicts
"""

import json
from typing import Dict, List, Any, Callable, Tuple
from datetime import datetime
from dataclasses import dataclass
import pandas as pd
import numpy as np
from scipy import stats
from loguru import logger
from prometheus_client import Counter, Histogram, Gauge


@dataclass
class ValidationResult:
    """Result of a data validation check"""
    check_name: str
    passed: bool
    details: Dict[str, Any]
    timestamp: datetime
    execution_time_ms: float
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'check_name': self.check_name,
            'passed': self.passed,
            'details': self.details,
            'timestamp': self.timestamp.isoformat(),
            'execution_time_ms': self.execution_time_ms
        }


@dataclass
class DataProfile:
    """Statistical profile of a dataset"""
    row_count: int
    column_count: int
    missing_values: Dict[str, int]
    data_types: Dict[str, str]
    numeric_stats: Dict[str, Dict[str, float]]
    categorical_stats: Dict[str, Dict[str, Any]]
    timestamp: datetime
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'row_count': self.row_count,
            'column_count': self.column_count,
            'missing_values': self.missing_values,
            'data_types': self.data_types,
            'numeric_stats': self.numeric_stats,
            'categorical_stats': self.categorical_stats,
            'timestamp': self.timestamp.isoformat()
        }


class DataValidator:
    """Core data validation engine with built-in checks"""
    
    def __init__(self, name: str = "default"):
        self.name = name
        self.validation_rules: Dict[str, Callable] = {}
        self.validation_results: List[ValidationResult] = []
        self._setup_metrics()
        self._register_builtin_rules()
        
    def _setup_metrics(self):
        """Setup Prometheus metrics for validation tracking"""
        self.validations_run = Counter(
            'data_validations_total', 'Total validations run', ['validator', 'rule'])
        self.validation_failures = Counter(
            'data_validation_failures_total', 'Total validation failures',
            ['validator', 'rule'])
        self.validation_duration = Histogram(
            'data_validation_duration_seconds', 'Validation execution time')
        self.data_quality_score = Gauge(
            'data_quality_score', 'Overall data quality score', ['validator'])
        
    def _register_builtin_rules(self):
        """Register built-in validation rules"""
        self.register_rule('completeness', self._check_completeness)
        self.register_rule('uniqueness', self._check_uniqueness)
        self.register_rule('range_bounds', self._check_range_bounds)
        self.register_rule('data_types', self._check_data_types)
        self.register_rule('pattern_match', self._check_pattern_match)
        self.register_rule('referential_integrity', self._check_referential_integrity)
        self.register_rule('statistical_bounds', self._check_statistical_bounds)
        
    def register_rule(self, name: str, validation_func: Callable):
        """Register custom validation rule"""
        self.validation_rules[name] = validation_func
        logger.info(f"Registered validation rule: {name}")
        
    def validate_dataset(self, df: pd.DataFrame,
                        rules_config: Dict[str, Any]) -> Dict[str, ValidationResult]:
        """Run validation suite on dataset"""
        logger.info(f"Starting validation suite for {len(df)} records")
        start_time = datetime.now()
        results = {}
        
        with self.validation_duration.time():
            for rule_name, rule_config in rules_config.items():
                if rule_name in self.validation_rules:
                    try:
                        rule_start = datetime.now()
                        
                        # Execute validation rule
                        passed, details = self.validation_rules[rule_name](df, rule_config)
                        
                        execution_time = (datetime.now() - rule_start).total_seconds() * 1000
                        
                        # Create result
                        result = ValidationResult(
                            check_name=rule_name,
                            passed=passed,
                            details=details,
                            timestamp=datetime.now(),
                            execution_time_ms=execution_time
                        )
                        
                        results[rule_name] = result
                        self.validation_results.append(result)
                        
                        # Update metrics
                        self.validations_run.labels(
                            validator=self.name, rule=rule_name).inc()
                        if not passed:
                            self.validation_failures.labels(
                                validator=self.name, rule=rule_name).inc()
                            
                    except (ValueError, TypeError) as e:
                        logger.error(f"Validation rule {rule_name} failed: {e}")
                        results[rule_name] = ValidationResult(
                            check_name=rule_name,
                            passed=False,
                            details={'error': str(e)},
                            timestamp=datetime.now(),
                            execution_time_ms=0
                        )
                        
        # Calculate overall quality score
        passed_count = sum(1 for r in results.values() if r.passed)
        quality_score = (passed_count / len(results)) * 100 if results else 0
        self.data_quality_score.labels(validator=self.name).set(quality_score)
        
        total_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"Validation complete: {passed_count}/{len(results)} checks passed "
                   f"({quality_score:.1f}%) in {total_time:.2f}s")
        
        return results
    
    def _check_completeness(self, df: pd.DataFrame,
                           config: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """Check data completeness (non-null values)"""
        columns = config.get('columns', df.columns)
        threshold = config.get('threshold', 0.95)  # 95% completeness required
        
        completeness_scores = {}
        failed_columns = []
        
        for col in columns:
            if col in df.columns:
                non_null_ratio = df[col].notna().sum() / len(df)
                completeness_scores[col] = non_null_ratio
                
                if non_null_ratio < threshold:
                    failed_columns.append(col)
        
        passed = len(failed_columns) == 0
        details = {
            'threshold': threshold,
            'completeness_scores': completeness_scores,
            'failed_columns': failed_columns,
            'overall_completeness': np.mean(list(completeness_scores.values()))
        }
        
        return passed, details
    
    def _check_uniqueness(self, df: pd.DataFrame,
                         config: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """Check uniqueness constraints"""
        columns = config.get('columns', [])
        
        duplicate_info = {}
        failed_columns = []
        
        for col in columns:
            if col in df.columns:
                duplicates = df[col].duplicated().sum()
                duplicate_ratio = duplicates / len(df)
                duplicate_info[col] = {
                    'duplicates': int(duplicates),
                    'duplicate_ratio': duplicate_ratio
                }
                
                if duplicates > 0:
                    failed_columns.append(col)
        
        passed = len(failed_columns) == 0
        details = {
            'duplicate_info': duplicate_info,
            'failed_columns': failed_columns
        }
        
        return passed, details
    
    def _check_range_bounds(self, df: pd.DataFrame,
                           config: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """Check numeric values are within specified ranges"""
        column_ranges = config.get('ranges', {})
        
        violations = {}
        failed_columns = []
        
        for col, bounds in column_ranges.items():
            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                min_val = bounds.get('min')
                max_val = bounds.get('max')
                
                violations_count = 0
                if min_val is not None:
                    violations_count += (df[col] < min_val).sum()
                if max_val is not None:
                    violations_count += (df[col] > max_val).sum()
                
                violations[col] = {
                    'violations': int(violations_count),
                    'violation_ratio': violations_count / len(df),
                    'min_bound': min_val,
                    'max_bound': max_val,
                    'actual_min': float(df[col].min()),
                    'actual_max': float(df[col].max())
                }
                
                if violations_count > 0:
                    failed_columns.append(col)
        
        passed = len(failed_columns) == 0
        details = {
            'violations': violations,
            'failed_columns': failed_columns
        }
        
        return passed, details
    
    def _check_data_types(self, df: pd.DataFrame,
                         config: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """Check data types match expected types"""
        expected_types = config.get('expected_types', {})
        
        type_mismatches = {}
        failed_columns = []
        
        for col, expected_type in expected_types.items():
            if col in df.columns:
                actual_type = str(df[col].dtype)
                
                # Flexible type matching
                type_match = (
                    (expected_type in ['int', 'integer'] and 'int' in actual_type) or
                    (expected_type in ['float', 'numeric'] and 'float' in actual_type) or
                    (expected_type == 'string' and 'object' in actual_type) or
                    (expected_type == 'datetime' and 'datetime' in actual_type) or
                    (expected_type == actual_type)
                )
                
                if not type_match:
                    type_mismatches[col] = {
                        'expected': expected_type,
                        'actual': actual_type
                    }
                    failed_columns.append(col)
        
        passed = len(failed_columns) == 0
        details = {
            'type_mismatches': type_mismatches,
            'failed_columns': failed_columns
        }
        
        return passed, details
    
    def _check_pattern_match(self, df: pd.DataFrame,
                            config: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """Check string patterns match regex"""
        column_patterns = config.get('patterns', {})
        
        pattern_violations = {}
        failed_columns = []
        
        for col, pattern in column_patterns.items():
            if col in df.columns:
                # Check string pattern matching
                matches = df[col].astype(str).str.match(pattern, na=False)
                violations = (~matches).sum()
                
                pattern_violations[col] = {
                    'pattern': pattern,
                    'violations': int(violations),
                    'violation_ratio': violations / len(df)
                }
                
                if violations > 0:
                    failed_columns.append(col)
        
        passed = len(failed_columns) == 0
        details = {
            'pattern_violations': pattern_violations,
            'failed_columns': failed_columns
        }
        
        return passed, details
    
    def _check_referential_integrity(self, df: pd.DataFrame,
                                     config: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """Check referential integrity constraints"""
        references = config.get('references', {})
        
        integrity_violations = {}
        failed_references = []
        
        for col, ref_values in references.items():
            if col in df.columns:
                # Check if all values exist in reference set
                violations = ~df[col].isin(ref_values)
                violation_count = violations.sum()
                
                integrity_violations[col] = {
                    'violations': int(violation_count),
                    'violation_ratio': violation_count / len(df),
                    'reference_size': len(ref_values)
                }
                
                if violation_count > 0:
                    failed_references.append(col)
        
        passed = len(failed_references) == 0
        details = {
            'integrity_violations': integrity_violations,
            'failed_references': failed_references
        }
        
        return passed, details
    
    def _check_statistical_bounds(self, df: pd.DataFrame,
                                 config: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """Check statistical outliers using z-score or IQR"""
        columns = config.get('columns', [])
        method = config.get('method', 'zscore')  # 'zscore' or 'iqr'
        threshold = config.get('threshold', 3.0)
        
        outlier_info = {}
        failed_columns = []
        
        for col in columns:
            if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                col_data = df[col].dropna()
                
                outliers = None
                if method == 'zscore':
                    z_scores = np.abs(stats.zscore(col_data))
                    outliers = z_scores > threshold
                elif method == 'iqr':
                    Q1 = col_data.quantile(0.25)
                    Q3 = col_data.quantile(0.75)
                    IQR = Q3 - Q1
                    lower_bound = Q1 - threshold * IQR
                    upper_bound = Q3 + threshold * IQR
                    outliers = (col_data < lower_bound) | (col_data > upper_bound)
                
                if outliers is not None:
                    outlier_count = outliers.sum()
                    outlier_info[col] = {
                        'method': method,
                        'threshold': threshold,
                        'outliers': int(outlier_count),
                        'outlier_ratio': outlier_count / len(col_data)
                    }
                    
                    if outlier_count > len(col_data) * 0.05:  # More than 5% outliers
                        failed_columns.append(col)
        
        passed = len(failed_columns) == 0
        details = {
            'outlier_info': outlier_info,
            'failed_columns': failed_columns
        }
        
        return passed, details


class DataProfiler:
    """Generate comprehensive data profiles"""
    
    def __init__(self):
        self.profiles: Dict[str, DataProfile] = {}
        
    def profile_dataset(self, df: pd.DataFrame, name: str = "default") -> DataProfile:
        """Generate comprehensive data profile"""
        logger.info(f"Profiling dataset '{name}' with {len(df)} records")
        
        # Basic statistics
        row_count = len(df)
        column_count = len(df.columns)
        
        # Missing values
        missing_values = df.isnull().sum().to_dict()
        
        # Data types
        data_types = df.dtypes.astype(str).to_dict()
        
        # Numeric statistics
        numeric_stats = {}
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_columns:
            col_data = df[col].dropna()
            if len(col_data) > 0:
                numeric_stats[col] = {
                    'mean': float(col_data.mean()),
                    'median': float(col_data.median()),
                    'std': float(col_data.std()),
                    'min': float(col_data.min()),
                    'max': float(col_data.max()),
                    'q25': float(col_data.quantile(0.25)),
                    'q75': float(col_data.quantile(0.75)),
                    'skewness': float(stats.skew(col_data)),
                    'kurtosis': float(stats.kurtosis(col_data))
                }
        
        # Categorical statistics
        categorical_stats = {}
        categorical_columns = df.select_dtypes(include=['object', 'category']).columns
        
        for col in categorical_columns:
            col_data = df[col].dropna()
            value_counts = col_data.value_counts()
            
            categorical_stats[col] = {
                'unique_count': len(value_counts),
                'most_frequent': value_counts.index[0] if len(value_counts) > 0 else None,
                'most_frequent_count': int(value_counts.iloc[0]) if len(value_counts) > 0 else 0,
                'cardinality_ratio': len(value_counts) / len(col_data) if len(col_data) > 0 else 0
            }
        
        # Create profile
        profile = DataProfile(
            row_count=row_count,
            column_count=column_count,
            missing_values=missing_values,
            data_types=data_types,
            numeric_stats=numeric_stats,
            categorical_stats=categorical_stats,
            timestamp=datetime.now()
        )
        
        self.profiles[name] = profile
        logger.info(f"Profile complete for '{name}': {row_count} rows, {column_count} columns")
        
        return profile
    
    def compare_profiles(self, profile1: DataProfile, profile2: DataProfile) -> Dict[str, Any]:
        """Compare two data profiles for changes"""
        comparison = {
            'row_count_change': profile2.row_count - profile1.row_count,
            'column_count_change': profile2.column_count - profile1.column_count,
            'schema_changes': [],
            'distribution_changes': []
        }
        
        # Schema changes
        old_columns = set(profile1.data_types.keys())
        new_columns = set(profile2.data_types.keys())
        
        added_columns = new_columns - old_columns
        removed_columns = old_columns - new_columns
        
        if added_columns:
            comparison['schema_changes'].append(f"Added columns: {list(added_columns)}")
        if removed_columns:
            comparison['schema_changes'].append(f"Removed columns: {list(removed_columns)}")
        
        # Type changes
        for col in old_columns.intersection(new_columns):
            if profile1.data_types[col] != profile2.data_types[col]:
                comparison['schema_changes'].append(
                    f"Type change in {col}: {profile1.data_types[col]} -> {profile2.data_types[col]}"
                )
        
        # Distribution changes for numeric columns
        for col in profile1.numeric_stats:
            if col in profile2.numeric_stats:
                old_stats = profile1.numeric_stats[col]
                new_stats = profile2.numeric_stats[col]
                
                # Check for significant changes (>20% change in mean or std)
                mean_change = abs(new_stats['mean'] - old_stats['mean']) / old_stats['mean']
                std_change = abs(new_stats['std'] - old_stats['std']) / old_stats['std']
                
                if mean_change > 0.2:
                    comparison['distribution_changes'].append(
                        f"Significant mean change in {col}: {old_stats['mean']:.3f} -> {new_stats['mean']:.3f}"
                    )
                
                if std_change > 0.2:
                    comparison['distribution_changes'].append(
                        f"Significant std change in {col}: {old_stats['std']:.3f} -> {new_stats['std']:.3f}"
                    )
        
        return comparison


class ExpectationSuite:
    """Custom expectation suite for data validation"""
    
    def __init__(self, name: str):
        self.name = name
        self.expectations: Dict[str, Dict[str, Any]] = {}
        
    def add_expectation(self, expectation_type: str, config: Dict[str, Any]):
        """Add expectation to suite"""
        self.expectations[expectation_type] = config
        logger.info(f"Added expectation '{expectation_type}' to suite '{self.name}'")
        
    def save_suite(self, filepath: str):
        """Save expectation suite to JSON file"""
        suite_data = {
            'name': self.name,
            'expectations': self.expectations,
            'created_at': datetime.now().isoformat()
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(suite_data, f, indent=2)
        
        logger.info(f"Saved expectation suite to {filepath}")
        
    @classmethod
    def load_suite(cls, filepath: str) -> 'ExpectationSuite':
        """Load expectation suite from JSON file"""
        with open(filepath, 'r', encoding='utf-8') as f:
            suite_data = json.load(f)
        
        suite = cls(suite_data['name'])
        suite.expectations = suite_data['expectations']
        
        logger.info(f"Loaded expectation suite from {filepath}")
        return suite


def create_financial_validation_suite() -> ExpectationSuite:
    """Create validation suite specifically for financial data"""
    suite = ExpectationSuite("financial_data_validation")
    
    # Completeness checks
    suite.add_expectation('completeness', {
        'columns': ['symbol', 'date', 'close', 'volume'],
        'threshold': 0.95
    })
    
    # Uniqueness checks
    suite.add_expectation('uniqueness', {
        'columns': ['symbol_date_composite']  # Assuming composite key
    })
    
    # Range bounds for financial data
    suite.add_expectation('range_bounds', {
        'ranges': {
            'close': {'min': 0, 'max': 10000},
            'volume': {'min': 0, 'max': 1000000000},
            'high': {'min': 0, 'max': 10000},
            'low': {'min': 0, 'max': 10000}
        }
    })
    
    # Data type validation
    suite.add_expectation('data_types', {
        'expected_types': {
            'symbol': 'string',
            'close': 'float',
            'volume': 'integer',
            'date': 'datetime'
        }
    })
    
    # Statistical bounds (outlier detection)
    suite.add_expectation('statistical_bounds', {
        'columns': ['close', 'volume'],
        'method': 'iqr',
        'threshold': 2.5
    })
    
    return suite


# Checkpoint functionality
class ValidationCheckpoint:
    """Validation checkpoint for automated quality checks"""
    
    def __init__(self, name: str, validator: DataValidator, suite: ExpectationSuite):
        self.name = name
        self.validator = validator
        self.suite = suite
        self.results_history: List[Dict[str, ValidationResult]] = []
        
    def run_checkpoint(self, df: pd.DataFrame) -> Dict[str, ValidationResult]:
        """Run validation checkpoint"""
        logger.info(f"Running validation checkpoint: {self.name}")
        
        # Run validation suite
        results = self.validator.validate_dataset(df, self.suite.expectations)
        
        # Store results
        self.results_history.append(results)
        
        # Keep only last 100 results
        if len(self.results_history) > 100:
            self.results_history = self.results_history[-100:]
        
        return results
    
    def get_quality_trend(self, days: int = 7) -> Dict[str, float]:
        """Get quality score trend over time"""
        if not self.results_history:
            return {}
        
        # Get recent results
        recent_results = self.results_history[-days:] if len(self.results_history) >= days else self.results_history
        
        quality_scores = []
        for results in recent_results:
            passed_count = sum(1 for r in results.values() if r.passed)
            quality_score = (passed_count / len(results)) * 100 if results else 0
            quality_scores.append(quality_score)
        
        return {
            'current_score': quality_scores[-1] if quality_scores else 0,
            'average_score': np.mean(quality_scores) if quality_scores else 0,
            'trend': np.polyfit(range(len(quality_scores)), quality_scores, 1)[0] if len(quality_scores) > 1 else 0
        }
