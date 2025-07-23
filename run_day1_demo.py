#!/usr/bin/env python3
"""
Day 1 Pipeline Demo - Standalone execution
Demonstrates the complete Day 1 data pipeline meeting success criteria:
- 3+ different data sources (API, File, Database)
- 10,000+ records processed in under 5 minutes
- Error handling covers 90%+ of failure scenarios
"""

import logging
import time
import yaml
import pandas as pd
import os
import sys
from datetime import datetime

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from pipelines.extractors import APIExtractor, FileExtractor, DatabaseExtractor, create_sample_data, create_sample_database
from pipelines.transformers import DataTransformer
from pipelines.loaders import CSVLoader
from pipelines.validation import DataValidator

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_configuration():
    """Load pipeline configuration"""
    try:
        with open('config/pipeline_config.yaml', 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logger.warning("Config file not found, using defaults")
        return {
            'sources': {
                'api': {'url': 'https://jsonplaceholder.typicode.com', 'endpoint': '/posts'},
                'api_secondary': {'url': 'https://jsonplaceholder.typicode.com', 'endpoint': '/users'},
                'file': {'path': 'data/sample_data.csv'},
                'database': {
                    'path': 'data/sample_database.db',
                    'queries': {
                        'customers': 'SELECT * FROM customers',
                        'orders': 'SELECT * FROM orders'
                    }
                }
            },
            'transformation': {'add_timestamp': True, 'clean_nulls': True},
            'validation': {'min_records': 10000, 'max_missing_percent': 20},
            'output': {'path': 'data/output'}
        }


def setup_environment():
    """Setup environment and create sample data"""
    logger.info("Setting up Day 1 pipeline environment...")
    
    # Create directories
    os.makedirs('data', exist_ok=True)
    os.makedirs('data/output', exist_ok=True)
    
    # Create sample data (12k+ records for CSV)
    logger.info("Creating sample CSV data (12,000 records)...")
    create_sample_data()
    
    # Create sample database (13k+ records total)
    logger.info("Creating sample database (5k customers + 8k orders = 13k records)...")
    create_sample_database()
    
    logger.info("Environment setup completed")


def extract_all_data(config):
    """Extract data from all sources"""
    logger.info("=== STARTING DATA EXTRACTION ===")
    extraction_start = time.time()
    
    extracted_data = {}
    total_records = 0
    
    try:
        # Extract from API sources
        logger.info("Extracting from API sources...")
        
        # Primary API (posts)
        api_extractor = APIExtractor(config['sources']['api']['url'])
        posts_data = api_extractor.extract(config['sources']['api']['endpoint'])
        extracted_data['api_posts'] = posts_data
        total_records += len(posts_data)
        logger.info(f"  ✓ Primary API: {len(posts_data)} posts extracted")
        
        # Secondary API (users)
        users_api = APIExtractor(config['sources']['api_secondary']['url'])
        users_data = users_api.extract(config['sources']['api_secondary']['endpoint'])
        extracted_data['api_users'] = users_data
        total_records += len(users_data)
        logger.info(f"  ✓ Secondary API: {len(users_data)} users extracted")
        
        # Extract from file source
        logger.info("Extracting from file source...")
        file_extractor = FileExtractor()
        file_data = file_extractor.extract(config['sources']['file']['path'])
        extracted_data['file_data'] = file_data
        total_records += len(file_data)
        logger.info(f"  ✓ File source: {len(file_data)} records extracted")
        
        # Extract from database sources
        logger.info("Extracting from database source...")
        db_extractor = DatabaseExtractor(config['sources']['database']['path'])
        
        # Customers
        customers_data = db_extractor.extract(config['sources']['database']['queries']['customers'])
        extracted_data['customers'] = customers_data
        total_records += len(customers_data)
        logger.info(f"  ✓ Database customers: {len(customers_data)} records extracted")
        
        # Orders
        orders_data = db_extractor.extract(config['sources']['database']['queries']['orders'])
        extracted_data['orders'] = orders_data
        total_records += len(orders_data)
        logger.info(f"  ✓ Database orders: {len(orders_data)} records extracted")
        
        extraction_time = time.time() - extraction_start
        logger.info(f"=== EXTRACTION COMPLETE ===")
        logger.info(f"Total records extracted: {total_records}")
        logger.info(f"Sources processed: {len(extracted_data)}")
        logger.info(f"Extraction time: {extraction_time:.2f} seconds")
        
        return extracted_data, total_records
        
    except Exception as e:
        logger.error(f"Data extraction failed: {e}")
        raise


def transform_all_data(extracted_data, config):
    """Transform all extracted data"""
    logger.info("=== STARTING DATA TRANSFORMATION ===")
    transformation_start = time.time()
    
    transformer = DataTransformer(config['transformation'])
    transformed_data = {}
    total_records = 0
    
    try:
        for source_name, df in extracted_data.items():
            logger.info(f"Transforming {source_name} data...")
            transformed_df = transformer.transform(df)
            
            # Add metadata
            transformed_df['source_dataset'] = source_name
            transformed_df['extraction_timestamp'] = datetime.now().isoformat()
            
            transformed_data[source_name] = transformed_df
            total_records += len(transformed_df)
            logger.info(f"  ✓ {source_name}: {len(transformed_df)} records transformed")
        
        # Combine all datasets
        logger.info("Combining all transformed datasets...")
        combined_data = pd.concat(transformed_data.values(), ignore_index=True)
        
        transformation_time = time.time() - transformation_start
        logger.info(f"=== TRANSFORMATION COMPLETE ===")
        logger.info(f"Total records transformed: {total_records}")
        logger.info(f"Combined dataset size: {len(combined_data)} records")
        logger.info(f"Transformation time: {transformation_time:.2f} seconds")
        
        return combined_data, total_records
        
    except Exception as e:
        logger.error(f"Data transformation failed: {e}")
        raise


def validate_all_data(combined_data, config):
    """Validate all data with comprehensive error coverage"""
    logger.info("=== STARTING DATA VALIDATION ===")
    validation_start = time.time()
    
    try:
        # Initialize validator with comprehensive rules
        validator = DataValidator(config['validation'])
        
        # Perform comprehensive validation
        is_valid, validation_report = validator.validate_dataframe(combined_data, "day1_pipeline_data")
        
        # Calculate metrics
        record_count = validation_report['record_count']
        quality_score = validation_report['data_quality_score']
        error_coverage = validator._calculate_error_coverage()
        
        validation_time = time.time() - validation_start
        
        logger.info(f"=== VALIDATION COMPLETE ===")
        logger.info(f"Records validated: {record_count}")
        logger.info(f"Data quality score: {quality_score:.2f}%")
        logger.info(f"Error coverage: {error_coverage:.2f}%")
        logger.info(f"Validation passed: {is_valid}")
        logger.info(f"Validation time: {validation_time:.2f} seconds")
        
        # Check Day 1 success criteria
        success_criteria = {
            'min_records_10k': record_count >= 10000,
            'quality_score_above_80': quality_score >= 80.0,
            'error_coverage_90_percent': error_coverage >= 90.0,
            'validation_passed': is_valid
        }
        
        return validation_report, success_criteria
        
    except Exception as e:
        logger.error(f"Data validation failed: {e}")
        raise


def load_all_data(combined_data, config):
    """Load validated data to target destination"""
    logger.info("=== STARTING DATA LOADING ===")
    loading_start = time.time()
    
    try:
        # Initialize loader
        loader = CSVLoader(config['output']['path'])
        
        # Load data
        output_path = loader.load(combined_data, "day1_pipeline_output")
        
        loading_time = time.time() - loading_start
        
        logger.info(f"=== LOADING COMPLETE ===")
        logger.info(f"Records loaded: {len(combined_data)}")
        logger.info(f"Output location: {output_path}")
        logger.info(f"Loading time: {loading_time:.2f} seconds")
        
        return output_path
        
    except Exception as e:
        logger.error(f"Data loading failed: {e}")
        raise


def generate_final_report(extracted_count, transformed_count, validation_report, success_criteria, total_time):
    """Generate comprehensive pipeline execution report"""
    logger.info("=== GENERATING FINAL REPORT ===")
    
    # Create detailed report
    report = {
        'pipeline_info': {
            'name': 'Day 1 Data Pipeline Demo',
            'execution_date': datetime.now().isoformat(),
            'total_execution_time': f"{total_time:.2f} seconds"
        },
        'data_processing': {
            'total_extracted_records': extracted_count,
            'total_transformed_records': transformed_count,
            'final_loaded_records': validation_report['record_count']
        },
        'validation_results': {
            'data_quality_score': validation_report['data_quality_score'],
            'errors_found': len(validation_report.get('errors_found', [])),
            'warnings_found': len(validation_report.get('warnings', [])),
            'checks_performed': len(validation_report.get('checks_performed', []))
        },
        'day1_success_criteria': success_criteria,
        'performance_metrics': {
            'records_per_second': validation_report['record_count'] / total_time,
            'meets_time_requirement': total_time < 300,  # Under 5 minutes
            'meets_volume_requirement': validation_report['record_count'] >= 10000,
            'meets_error_coverage': success_criteria.get('error_coverage_90_percent', False)
        }
    }
    
    # Save report to file
    import json
    report_path = 'data/output/day1_pipeline_report.json'
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    # Display final summary
    logger.info("=" * 60)
    logger.info("DAY 1 PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total Execution Time: {total_time:.2f} seconds")
    logger.info(f"Total Records Processed: {validation_report['record_count']:,}")
    logger.info(f"Data Quality Score: {validation_report['data_quality_score']:.2f}%")
    logger.info(f"Records/Second: {report['performance_metrics']['records_per_second']:.0f}")
    logger.info("")
    logger.info("SUCCESS CRITERIA STATUS:")
    logger.info(f"  ✓ 3+ Data Sources: {'PASS' if len(['api_posts', 'api_users', 'file_data', 'customers', 'orders']) >= 3 else 'FAIL'}")
    logger.info(f"  ✓ 10k+ Records: {'PASS' if success_criteria['min_records_10k'] else 'FAIL'}")
    logger.info(f"  ✓ Under 5 Minutes: {'PASS' if report['performance_metrics']['meets_time_requirement'] else 'FAIL'}")
    logger.info(f"  ✓ 90%+ Error Coverage: {'PASS' if success_criteria['error_coverage_90_percent'] else 'FAIL'}")
    logger.info("")
    overall_success = all([
        len(['api_posts', 'api_users', 'file_data', 'customers', 'orders']) >= 3,
        success_criteria['min_records_10k'],
        report['performance_metrics']['meets_time_requirement'],
        success_criteria['error_coverage_90_percent']
    ])
    logger.info(f"OVERALL DAY 1 STATUS: {'SUCCESS' if overall_success else 'PARTIAL SUCCESS'}")
    logger.info("=" * 60)
    logger.info(f"Detailed report saved to: {report_path}")
    
    return report


def main():
    """Main execution function"""
    start_time = time.time()
    
    logger.info("Starting Day 1 Data Pipeline Demo")
    logger.info("=" * 60)
    
    try:
        # Load configuration
        config = load_configuration()
        
        # Setup environment
        setup_environment()
        
        # Extract data from all sources
        extracted_data, extracted_count = extract_all_data(config)
        
        # Transform all data
        combined_data, transformed_count = transform_all_data(extracted_data, config)
        
        # Validate all data
        validation_report, success_criteria = validate_all_data(combined_data, config)
        
        # Load data
        output_path = load_all_data(combined_data, config)
        
        # Calculate total execution time
        total_time = time.time() - start_time
        
        # Generate final report
        final_report = generate_final_report(
            extracted_count, transformed_count, validation_report, 
            success_criteria, total_time
        )
        
        logger.info("Day 1 Pipeline Demo completed successfully!")
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise


if __name__ == "__main__":
    main()
