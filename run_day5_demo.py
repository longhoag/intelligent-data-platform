"""
Day 5: Data Quality & Monitoring System Demo
Comprehensive demonstration of data quality validation, drift detection, and monitoring
"""

import asyncio
import time
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np
from loguru import logger
import json

# Import quality modules
from src.quality.data_validation import (
    DataValidator, DataProfiler, ValidationCheckpoint,
    create_financial_validation_suite, ExpectationSuite
)
from src.quality.drift_detection import DriftDetector


class Day5QualityOrchestrator:
    """Orchestrator for Day 5 data quality and monitoring system"""
    
    def __init__(self):
        self.validator = DataValidator("day5_validator")
        self.drift_detector = DriftDetector("day5_drift_detector", significance_level=0.05)
        self.profiler = DataProfiler()
        
        # Create expectations directory
        self.expectations_dir = Path("expectations")
        self.expectations_dir.mkdir(exist_ok=True)
        
        logger.info("Day 5 Quality Orchestrator initialized")
    
    async def demonstrate_data_quality_system(self):
        """Main demonstration of the data quality system"""
        logger.info("🚀 Starting Day 5: Data Quality & Monitoring System Demo")
        print("=" * 70)
        
        try:
            # Step 1: Load and prepare data
            await self._load_financial_data()
            
            # Step 2: Data validation framework
            await self._demonstrate_data_validation()
            
            # Step 3: Data profiling
            await self._demonstrate_data_profiling()
            
            # Step 4: Drift detection
            await self._demonstrate_drift_detection()
            
            # Step 5: Quality monitoring
            await self._demonstrate_quality_monitoring()
            
            # Step 6: Incident response simulation
            await self._demonstrate_incident_response()
            
            # Step 7: Generate reports
            await self._generate_quality_reports()
            
            print("=" * 70)
            logger.success("🎉 Day 5 Data Quality System demonstration complete!")
            
        except Exception as e:
            logger.error(f"❌ Demo failed: {e}")
            raise
    
    async def _load_financial_data(self):
        """Load financial data for quality analysis"""
        logger.info("📊 Loading financial data for quality analysis...")
        
        # Load existing processed data
        processed_files = list(Path("data/processed").glob("day1_pipeline_output_*.csv"))
        
        if processed_files:
            # Use latest processed file
            latest_file = max(processed_files, key=lambda x: x.stat().st_mtime)
            self.reference_data = pd.read_csv(latest_file)
            logger.info(f"✅ Loaded reference data: {len(self.reference_data)} records "
                       f"from {latest_file.name}")
        else:
            # Generate sample data if no processed data available
            logger.info("🔄 No processed data found, generating sample financial data...")
            self.reference_data = self._generate_sample_financial_data()
            logger.info(f"✅ Generated sample data: {len(self.reference_data)} records")
        
        # Create current data with some drift and quality issues
        self.current_data = self._create_drifted_data(self.reference_data)
        logger.info(f"✅ Created current data with simulated drift: "
                   f"{len(self.current_data)} records")
    
    async def _demonstrate_data_validation(self):
        """Demonstrate comprehensive data validation"""
        logger.info("🔍 Demonstrating data validation framework...")
        
        # Create financial validation suite
        suite = create_financial_validation_suite()
        
        # Save suite for reuse
        suite_path = self.expectations_dir / "financial_validation_suite.json"
        suite.save_suite(str(suite_path))
        logger.info(f"💾 Saved validation suite to {suite_path}")
        
        # Create validation checkpoint
        checkpoint = ValidationCheckpoint("financial_checkpoint", self.validator, suite)
        
        # Run validation on reference data
        logger.info("🔄 Running validation on reference data...")
        ref_results = checkpoint.run_checkpoint(self.reference_data)
        
        # Run validation on current data
        logger.info("🔄 Running validation on current data...")
        curr_results = checkpoint.run_checkpoint(self.current_data)
        
        # Display results
        ref_passed = sum(1 for r in ref_results.values() if r.passed)
        curr_passed = sum(1 for r in curr_results.values() if r.passed)
        
        print(f"\n📊 Validation Results:")
        ref_pct = ref_passed/len(ref_results)*100
        curr_pct = curr_passed/len(curr_results)*100
        print(f"   Reference Data: {ref_passed}/{len(ref_results)} checks passed "
              f"({ref_pct:.1f}%)")
        print(f"   Current Data:   {curr_passed}/{len(curr_results)} checks passed "
              f"({curr_pct:.1f}%)")
        
        # Show failed checks for current data
        failed_checks = [name for name, result in curr_results.items() if not result.passed]
        if failed_checks:
            print(f"   ❌ Failed checks: {', '.join(failed_checks)}")
        
        # Store results
        self.validation_results = {
            'reference': ref_results,
            'current': curr_results,
            'checkpoint': checkpoint
        }
        
        logger.success("✅ Data validation demonstration complete")
    
    async def _demonstrate_data_profiling(self):
        """Demonstrate data profiling capabilities"""
        logger.info("📈 Demonstrating data profiling...")
        
        # Profile reference data
        ref_profile = self.profiler.profile_dataset(self.reference_data, "reference_data")
        
        # Profile current data
        curr_profile = self.profiler.profile_dataset(self.current_data, "current_data")
        
        # Compare profiles
        profile_comparison = self.profiler.compare_profiles(ref_profile, curr_profile)
        
        print(f"\n📊 Data Profile Comparison:")
        print(f"   Reference: {ref_profile.row_count} rows, {ref_profile.column_count} columns")
        print(f"   Current:   {curr_profile.row_count} rows, {curr_profile.column_count} columns")
        print(f"   Row change: {profile_comparison['row_count_change']:+d}")
        
        if profile_comparison['schema_changes']:
            print(f"   🔄 Schema changes: {len(profile_comparison['schema_changes'])}")
            for change in profile_comparison['schema_changes'][:3]:  # Show first 3
                print(f"      - {change}")
        
        if profile_comparison['distribution_changes']:
            print(f"   📈 Distribution changes: {len(profile_comparison['distribution_changes'])}")
            for change in profile_comparison['distribution_changes'][:3]:  # Show first 3
                print(f"      - {change}")
        
        # Store profiles
        self.profiles = {
            'reference': ref_profile,
            'current': curr_profile,
            'comparison': profile_comparison
        }
        
        logger.success("✅ Data profiling demonstration complete")
    
    async def _demonstrate_drift_detection(self):
        """Demonstrate drift detection capabilities"""
        logger.info("📊 Demonstrating drift detection...")
        
        # Select numeric columns for drift detection
        numeric_columns = self.reference_data.select_dtypes(include=[np.number]).columns.tolist()
        drift_features = numeric_columns[:5]  # Test first 5 numeric features
        
        print(f"\n🔍 Testing drift on features: {', '.join(drift_features)}")
        
        # Set reference distributions
        for feature in drift_features:
            if feature in self.reference_data.columns:
                self.drift_detector.set_reference_distribution(self.reference_data, feature)
        
        # Detect drift using multiple methods
        drift_methods = ["ks_test", "js_divergence", "population_stability"]
        drift_results = {}
        
        for method in drift_methods:
            print(f"\n📈 Running {method.upper()} drift detection...")
            method_results = {}
            
            for feature in drift_features:
                if feature in self.current_data.columns:
                    try:
                        result = self.drift_detector.detect_drift(
                            self.current_data, feature, method)
                        method_results[feature] = result
                        
                        status = "🚨 DRIFT DETECTED" if result.drift_detected else "✅ No Drift"
                        print(f"      {feature}: {status} (score: {result.drift_score:.4f})")
                        
                    except Exception as e:
                        logger.warning(f"Drift detection failed for {feature} with {method}: {e}")
            
            drift_results[method] = method_results
        
        # Multivariate drift detection
        try:
            multivariate_result = self.drift_detector.detect_multivariate_drift(
                self.current_data, drift_features, method="energy"
            )
            
            drift_detected = multivariate_result.drift_detected
            status = "🚨 MULTIVARIATE DRIFT" if drift_detected else "✅ No Multivariate Drift"
            score = multivariate_result.drift_score
            print(f"\n📊 Multivariate Analysis: {status} (score: {score:.4f})")
            
        except Exception as e:
            logger.warning(f"Multivariate drift detection failed: {e}")
        
        # Store results
        self.drift_results = drift_results
        
        logger.success("✅ Drift detection demonstration complete")
    
    async def _demonstrate_quality_monitoring(self):
        """Demonstrate quality monitoring and alerting"""
        logger.info("📊 Demonstrating quality monitoring...")
        
        # Simulate continuous monitoring
        print(f"\n🔄 Simulating continuous quality monitoring...")
        
        monitoring_results = []
        
        for i in range(5):  # Simulate 5 monitoring cycles
            print(f"   Cycle {i+1}/5: ", end="")
            
            # Create slightly different data for each cycle
            cycle_data = self._create_monitoring_data(self.current_data, cycle=i)
            
            # Quick validation
            suite = create_financial_validation_suite()
            cycle_results = self.validator.validate_dataset(cycle_data, suite.expectations)
            
            # Calculate quality score
            passed = sum(1 for r in cycle_results.values() if r.passed)
            quality_score = (passed / len(cycle_results)) * 100
            
            # Quick drift check
            drift_count = 0
            if len(self.drift_detector.reference_distributions) > 0:
                test_features = list(self.drift_detector.reference_distributions.keys())[:3]
                for feature in test_features:
                    if feature in cycle_data.columns:
                        try:
                            result = self.drift_detector.detect_drift(
                                cycle_data, feature, "ks_test")
                            if result.drift_detected:
                                drift_count += 1
                        except:
                            pass
            
            # Alert conditions
            alerts = []
            if quality_score < 80:
                alerts.append("Quality")
            if drift_count > 1:
                alerts.append("Drift")
            
            status = f"Quality: {quality_score:.1f}%"
            if alerts:
                status += f" ⚠️ ALERTS: {', '.join(alerts)}"
            
            print(status)
            
            monitoring_results.append({
                'cycle': i+1,
                'quality_score': quality_score,
                'drift_detections': drift_count,
                'alerts': alerts,
                'timestamp': datetime.now() + timedelta(minutes=i*5)  # Simulate time progression
            })
            
            time.sleep(0.5)  # Brief pause for demonstration
        
        # Store monitoring results
        self.monitoring_results = monitoring_results
        
        logger.success("✅ Quality monitoring demonstration complete")
    
    async def _demonstrate_incident_response(self):
        """Demonstrate automated incident response"""
        logger.info("🚨 Demonstrating incident response system...")
        
        # Simulate critical data quality incident
        print(f"\n🚨 SIMULATING CRITICAL DATA QUALITY INCIDENT")
        
        # Create severely degraded data
        incident_data = self._create_incident_data(self.current_data)
        
        # Run emergency validation
        suite = create_financial_validation_suite()
        incident_results = self.validator.validate_dataset(incident_data, suite.expectations)
        
        # Calculate incident severity
        failed_critical_checks = 0
        critical_checks = ['completeness', 'range_bounds', 'data_types']
        
        for check_name in critical_checks:
            if check_name in incident_results and not incident_results[check_name].passed:
                failed_critical_checks += 1
        
        passed_checks = sum(1 for r in incident_results.values() if r.passed)
        quality_score = (passed_checks / len(incident_results)) * 100
        
        print(f"   📊 Incident Assessment:")
        print(f"      Quality Score: {quality_score:.1f}% (Critical threshold: 60%)")
        print(f"      Failed Critical Checks: {failed_critical_checks}/{len(critical_checks)}")
        
        # Incident response actions
        if quality_score < 60 or failed_critical_checks >= 2:
            print(f"   🚨 CRITICAL INCIDENT DETECTED - Initiating Response Protocol")
            print(f"      ✅ Data pipeline paused")
            print(f"      ✅ Stakeholders notified")
            print(f"      ✅ Fallback to last known good data")
            print(f"      ✅ Root cause analysis initiated")
            
            # Generate incident report
            incident_report = {
                'incident_id': f"INC-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
                'timestamp': datetime.now().isoformat(),
                'severity': 'CRITICAL',
                'quality_score': quality_score,
                'failed_checks': failed_critical_checks,
                'affected_records': len(incident_data),
                'response_actions': [
                    'Pipeline paused',
                    'Stakeholders notified',
                    'Fallback activated',
                    'Investigation started'
                ]
            }
            
            # Save incident report
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            incident_path = (self.expectations_dir.parent / "logs" /
                           f"incident_report_{timestamp}.json")
            incident_path.parent.mkdir(exist_ok=True)
            
            with open(incident_path, 'w') as f:
                json.dump(incident_report, f, indent=2)
            
            print(f"      📝 Incident report saved: {incident_path.name}")
        
        else:
            print(f"   ✅ Quality degradation within acceptable limits")
        
        logger.success("✅ Incident response demonstration complete")
    
    async def _generate_quality_reports(self):
        """Generate comprehensive quality reports"""
        logger.info("📊 Generating quality reports...")
        
        # Create comprehensive quality report
        quality_report = {
            'report_metadata': {
                'generated_at': datetime.now().isoformat(),
                'report_type': 'Day 5 Quality Assessment',
                'data_period': '2024-01-01 to 2024-12-31',
                'version': '1.0'
            },
            'data_summary': {
                'reference_records': len(self.reference_data),
                'current_records': len(self.current_data),
                'features_analyzed': len(self.reference_data.columns)
            },
            'validation_summary': {
                'total_checks': len(self.validation_results['current']),
                'passed_checks': sum(1 for r in self.validation_results['current'].values() 
                                   if r.passed),
                'quality_score': (sum(1 for r in self.validation_results['current'].values() 
                                    if r.passed) / len(self.validation_results['current']) * 100)
            },
            'drift_summary': {
                'methods_tested': len(self.drift_results),
                'features_tested': len(next(iter(self.drift_results.values()))),
                'drift_detections': sum(
                    sum(1 for r in method_results.values() if r.drift_detected)
                    for method_results in self.drift_results.values()
                )
            },
            'monitoring_summary': {
                'monitoring_cycles': len(self.monitoring_results),
                'avg_quality_score': np.mean([r['quality_score'] for r in self.monitoring_results]),
                'total_alerts': sum(len(r['alerts']) for r in self.monitoring_results)
            },
            'recommendations': [
                "Implement automated data quality checks in production pipeline",
                "Set up real-time drift monitoring for critical features",
                "Establish quality score thresholds and alerting rules",
                "Create data quality dashboards for stakeholder visibility",
                "Develop incident response playbooks for quality issues"
            ]
        }
        
        # Save main quality report
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_path = Path("data/output") / f"day5_quality_report_{timestamp}.json"
        report_path.parent.mkdir(exist_ok=True)
        
        with open(report_path, 'w') as f:
            json.dump(quality_report, f, indent=2)
        
        # Generate summary statistics
        validation_summary = quality_report['validation_summary']
        drift_summary = quality_report['drift_summary']
        monitoring_summary = quality_report['monitoring_summary']
        
        print(f"\n📊 QUALITY ASSESSMENT SUMMARY")
        print(f"   🔍 Validation: {validation_summary['passed_checks']}/"
              f"{validation_summary['total_checks']} checks passed "
              f"({validation_summary['quality_score']:.1f}%)")
        print(f"   📈 Drift Detection: {drift_summary['drift_detections']} "
              f"drift instances detected")
        print(f"   📊 Monitoring: {monitoring_summary['avg_quality_score']:.1f}% "
              f"average quality score")
        print(f"   📝 Report saved: {report_path.name}")
        
        logger.success("✅ Quality reports generated successfully")
    
    def _generate_sample_financial_data(self) -> pd.DataFrame:
        """Generate sample financial data for demonstration"""
        np.random.seed(42)
        
        dates = pd.date_range(start='2024-01-01', periods=1000, freq='D')
        symbols = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN'] * 200
        
        data = []
        for i, (date, symbol) in enumerate(zip(dates, symbols)):
            base_price = 100 + np.random.normal(0, 10)
            base_price += i * 0.01  # Trend
            
            open_price = base_price + np.random.normal(0, 1)
            high_price = max(open_price, base_price) + abs(np.random.normal(0, 2))
            low_price = min(open_price, base_price) - abs(np.random.normal(0, 2))
            close_price = base_price + np.random.normal(0, 1)
            volume = max(100000, int(np.random.lognormal(12, 0.5)))
            
            data.append({
                'date': date,
                'symbol': symbol,
                'open': round(max(0.01, open_price), 2),
                'high': round(max(0.01, high_price), 2),
                'low': round(max(0.01, low_price), 2),
                'close': round(max(0.01, close_price), 2),
                'volume': volume,
                'adjusted_close': round(max(0.01, close_price * np.random.uniform(0.98, 1.02)), 2)
            })
        
        return pd.DataFrame(data)
    
    def _create_drifted_data(self, reference_data: pd.DataFrame) -> pd.DataFrame:
        """Create data with simulated drift and quality issues"""
        drifted_data = reference_data.copy()
        
        # Add systematic bias to prices (drift)
        price_cols = ['open', 'high', 'low', 'close', 'adjusted_close']
        for col in price_cols:
            if col in drifted_data.columns:
                drifted_data[col] = drifted_data[col] * 1.15  # 15% price increase
        
        # Add more missing values
        for col in drifted_data.columns:
            if np.random.random() < 0.3:  # 30% chance per column
                missing_indices = np.random.choice(
                    drifted_data.index,
                    size=int(len(drifted_data) * 0.05),  # 5% missing
                    replace=False
                )
                drifted_data.loc[missing_indices, col] = np.nan
        
        # Add some outliers
        if 'volume' in drifted_data.columns:
            outlier_indices = np.random.choice(drifted_data.index, size=20, replace=False)
            drifted_data.loc[outlier_indices, 'volume'] *= 10  # 10x volume spikes
        
        return drifted_data
    
    def _create_monitoring_data(self, base_data: pd.DataFrame, cycle: int) -> pd.DataFrame:
        """Create monitoring data with gradual degradation"""
        monitoring_data = base_data.copy().sample(frac=0.8)  # Sample 80% of data
        
        # Gradual quality degradation
        degradation_factor = 1 + (cycle * 0.1)  # Increase degradation each cycle
        
        # Add more missing values over time
        missing_rate = 0.02 * degradation_factor
        for col in monitoring_data.select_dtypes(include=[np.number]).columns:
            missing_count = int(len(monitoring_data) * missing_rate)
            if missing_count > 0:
                missing_indices = np.random.choice(
                    monitoring_data.index,
                    size=missing_count,
                    replace=False
                )
                monitoring_data.loc[missing_indices, col] = np.nan
        
        return monitoring_data
    
    def _create_incident_data(self, base_data: pd.DataFrame) -> pd.DataFrame:
        """Create severely degraded data for incident simulation"""
        incident_data = base_data.copy()
        
        # Severe quality issues
        # 1. Massive missing values
        for col in incident_data.columns:
            missing_indices = np.random.choice(
                incident_data.index, 
                size=int(len(incident_data) * 0.4),  # 40% missing
                replace=False
            )
            incident_data.loc[missing_indices, col] = np.nan
        
        # 2. Invalid ranges
        if 'close' in incident_data.columns:
            # Negative prices
            negative_indices = np.random.choice(incident_data.index, size=50, replace=False)
            incident_data.loc[negative_indices, 'close'] = -abs(incident_data.loc[negative_indices, 'close'])
        
        # 3. Type corruption
        if 'volume' in incident_data.columns:
            # String values in numeric column
            corruption_indices = np.random.choice(incident_data.index, size=20, replace=False)
            incident_data.loc[corruption_indices, 'volume'] = 'ERROR'
        
        return incident_data


async def main():
    """Main function to run Day 5 demonstration"""
    orchestrator = Day5QualityOrchestrator()
    await orchestrator.demonstrate_data_quality_system()


if __name__ == "__main__":
    asyncio.run(main())
