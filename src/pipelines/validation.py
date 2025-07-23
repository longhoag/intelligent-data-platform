"""
Data Validation - Validate data quality and integrity (Day 1 Essential)
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass
from datetime import datetime
from loguru import logger


@dataclass
class ValidationResult:
    """Result of a validation check"""
    check_name: str
    passed: bool
    message: str
    details: Optional[Dict[str, Any]] = None


@dataclass
class ValidationReport:
    """Complete validation report"""
    source_name: str
    total_checks: int
    passed_checks: int
    failed_checks: int
    success_rate: float
    results: List[ValidationResult]
    timestamp: datetime


class DataValidator:
    """Validate financial data quality and integrity"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
    
    def validate(self, df: pd.DataFrame, source_name: str = "unknown") -> ValidationReport:
        """Run all validation checks on DataFrame"""
        logger.info(f"Starting validation for {source_name} ({len(df)} records)")
        
        results = []
        
        # Basic data checks
        results.extend(self._check_basic_quality(df))
        
        # Financial data specific checks
        results.extend(self._check_financial_data(df, source_name))
        
        # Schema validation
        if source_name in self.config.get('required_columns', {}):
            results.extend(self._check_schema(df, source_name))
        
        # Data type validation
        if source_name in self.config.get('data_types', {}):
            results.extend(self._check_data_types(df, source_name))
        
        # Custom business rules
        results.extend(self._check_business_rules(df, source_name))
        
        # Generate report
        passed_checks = sum(1 for r in results if r.passed)
        failed_checks = len(results) - passed_checks
        success_rate = (passed_checks / len(results)) * 100 if results else 0
        
        report = ValidationReport(
            source_name=source_name,
            total_checks=len(results),
            passed_checks=passed_checks,
            failed_checks=failed_checks,
            success_rate=success_rate,
            results=results,
            timestamp=datetime.now()
        )
        
        logger.info(f"Validation complete: {passed_checks}/{len(results)} checks passed ({success_rate:.1f}%)")
        return report
    
    def _check_basic_quality(self, df: pd.DataFrame) -> List[ValidationResult]:
        """Basic data quality checks"""
        results = []
        
        # Check if DataFrame is not empty
        results.append(ValidationResult(
            check_name="non_empty_dataframe",
            passed=len(df) > 0,
            message=f"DataFrame has {len(df)} records",
            details={"record_count": len(df)}
        ))
        
        # Check minimum record count
        min_records = self.config.get('min_records', 10000)
        results.append(ValidationResult(
            check_name="minimum_record_count",
            passed=len(df) >= min_records,
            message=f"DataFrame has {len(df)} records (minimum: {min_records})",
            details={"record_count": len(df), "minimum_required": min_records}
        ))
        
        # Check for completely duplicate rows
        duplicate_rows = df.duplicated().sum()
        results.append(ValidationResult(
            check_name="duplicate_rows",
            passed=duplicate_rows == 0,
            message=f"Found {duplicate_rows} duplicate rows",
            details={"duplicate_count": duplicate_rows}
        ))
        
        # Check for completely empty columns
        empty_columns = [col for col in df.columns if df[col].isnull().all()]
        results.append(ValidationResult(
            check_name="empty_columns",
            passed=len(empty_columns) == 0,
            message=f"Found {len(empty_columns)} completely empty columns",
            details={"empty_columns": empty_columns}
        ))
        
        return results
    
    def _check_financial_data(self, df: pd.DataFrame, source_name: str) -> List[ValidationResult]:
        """Financial data specific validation checks"""
        results = []
        
        # OHLCV data validation (Open, High, Low, Close, Volume)
        financial_columns = ['open', 'high', 'low', 'close', 'volume']
        ohlcv_columns = [col for col in financial_columns if col in df.columns]
        
        if len(ohlcv_columns) >= 3:  # At least 3 OHLCV columns present
            # Check that High >= Low
            if 'high' in df.columns and 'low' in df.columns:
                violations = (df['high'] < df['low']).sum()
                results.append(ValidationResult(
                    check_name="high_greater_than_low",
                    passed=violations == 0,
                    message=f"Found {violations} records where High < Low",
                    details={"violations": violations}
                ))
            
            # Check that Close is between High and Low
            if all(col in df.columns for col in ['high', 'low', 'close']):
                close_violations = ((df['close'] > df['high']) | (df['close'] < df['low'])).sum()
                results.append(ValidationResult(
                    check_name="close_within_high_low_range",
                    passed=close_violations == 0,
                    message=f"Found {close_violations} records where Close is outside High-Low range",
                    details={"violations": close_violations}
                ))
            
            # Check for positive volume
            if 'volume' in df.columns:
                negative_volume = (df['volume'] < 0).sum()
                results.append(ValidationResult(
                    check_name="positive_volume",
                    passed=negative_volume == 0,
                    message=f"Found {negative_volume} records with negative volume",
                    details={"negative_volume_count": negative_volume}
                ))
        
        # Check for reasonable price ranges (not zero or negative for financial data)
        price_columns = [col for col in df.columns if any(keyword in col.lower() 
                        for keyword in ['price', 'open', 'high', 'low', 'close', 'amount', 'value'])]
        
        for col in price_columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                zero_or_negative = (df[col] <= 0).sum()
                results.append(ValidationResult(
                    check_name=f"positive_prices_{col}",
                    passed=zero_or_negative == 0,
                    message=f"Column '{col}' has {zero_or_negative} zero or negative values",
                    details={"column": col, "violations": zero_or_negative}
                ))
        
        return results
    
    def _check_schema(self, df: pd.DataFrame, source_name: str) -> List[ValidationResult]:
        """Validate DataFrame schema"""
        results = []
        required_columns = self.config.get('required_columns', {}).get(source_name, [])
        
        for column in required_columns:
            results.append(ValidationResult(
                check_name=f"required_column_{column}",
                passed=column in df.columns,
                message=f"Required column '{column}' {'found' if column in df.columns else 'missing'}",
                details={"column": column, "available_columns": list(df.columns)}
            ))
        
        return results
    
    def _check_data_types(self, df: pd.DataFrame, source_name: str) -> List[ValidationResult]:
        """Validate data types"""
        results = []
        expected_types = self.config.get('data_types', {}).get(source_name, {})
        
        for column, expected_type in expected_types.items():
            if column in df.columns:
                # Simplified type checking
                actual_type = str(df[column].dtype)
                type_match = self._types_compatible(actual_type, expected_type)
                
                results.append(ValidationResult(
                    check_name=f"data_type_{column}",
                    passed=type_match,
                    message=f"Column '{column}' type: {actual_type} (expected: {expected_type})",
                    details={"column": column, "actual_type": actual_type, "expected_type": expected_type}
                ))
        
        return results
    
    def _check_business_rules(self, df: pd.DataFrame, source_name: str) -> List[ValidationResult]:
        """Check custom business rules"""
        results = []
        
        # Check for null values in ID columns
        id_columns = [col for col in df.columns if 'id' in col.lower()]
        for col in id_columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                results.append(ValidationResult(
                    check_name=f"id_column_nulls_{col}",
                    passed=null_count == 0,
                    message=f"ID column '{col}' has {null_count} null values",
                    details={"column": col, "null_count": null_count}
                ))
        
        # Check for reasonable value ranges
        numeric_columns = df.select_dtypes(include=['int64', 'float64']).columns
        for col in numeric_columns:
            if col in df.columns and len(df[col]) > 0:
                # Check for negative IDs
                if 'id' in col.lower():
                    negative_count = (df[col] < 0).sum()
                    results.append(ValidationResult(
                        check_name=f"positive_ids_{col}",
                        passed=negative_count == 0,
                        message=f"ID column '{col}' has {negative_count} negative values",
                        details={"column": col, "negative_count": negative_count}
                    ))
        
        return results
    
    def _types_compatible(self, actual: str, expected: str) -> bool:
        """Check if actual and expected types are compatible"""
        type_mappings = {
            'int64': ['int', 'integer', 'int64'],
            'float64': ['float', 'double', 'float64', 'numeric'],
            'object': ['string', 'str', 'object', 'text'],
            'bool': ['boolean', 'bool'],
            'datetime64[ns]': ['datetime', 'timestamp', 'datetime64']
        }
        
        for pandas_type, aliases in type_mappings.items():
            if actual.startswith(pandas_type) and expected.lower() in aliases:
                return True
        
        return actual.lower() == expected.lower()
    
    def print_report(self, report: ValidationReport) -> None:
        """Print validation report in readable format"""
        print(f"\n=== Validation Report: {report.source_name} ===")
        print(f"Timestamp: {report.timestamp}")
        print(f"Total Checks: {report.total_checks}")
        print(f"Passed: {report.passed_checks}")
        print(f"Failed: {report.failed_checks}")
        print(f"Success Rate: {report.success_rate:.1f}%")
        print("\nCheck Details:")
        
        for result in report.results:
            status = "✅ PASS" if result.passed else "❌ FAIL"
            print(f"  {status} {result.check_name}: {result.message}")
        
        print("=" * 50)
