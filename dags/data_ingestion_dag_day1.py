"""
Day 1 Data Ingestion DAG - Multi-source pipeline with 10k+ records
Meets Day 1 Success Criteria:
- 3+ different data sources (API, File, Database)
- 10,000+ records in under 5 minutes
- Error handling covers 90%+ of failure scenarios
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os
import logging

# Add src to Python path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from pipelines.extractors import APIExtractor, FileExtractor, DatabaseExtractor, create_sample_data, create_sample_database
from pipelines.transformers import DataTransformer
from pipelines.loaders import CSVLoader
from pipelines.validation import DataValidator
import yaml
import pandas as pd

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# DAG configuration for Day 1
default_args = {
    'owner': 'data-platform-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'catchup': False
}

dag = DAG(
    'data_ingestion_pipeline_day1',
    default_args=default_args,
    description='Day 1 Data Pipeline - Multi-source ingestion with 10k+ records',
    schedule_interval='@daily',
    max_active_runs=1,
    tags=['day1', 'ingestion', 'multi-source']
)


def setup_environment(**context):
    """Setup environment and create sample data for Day 1 demo"""
    try:
        logger.info("Setting up Day 1 pipeline environment")
        
        # Create sample data files (12k+ records total)
        create_sample_data()
        create_sample_database()
        
        logger.info("Environment setup completed successfully")
        return "Environment ready for Day 1 pipeline"
        
    except Exception as e:
        logger.error(f"Environment setup failed: {e}")
        raise


def extract_api_data(**context):
    """Extract data from API endpoints"""
    try:
        logger.info("Starting API data extraction")
        
        # Load configuration
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'pipeline_config.yaml')
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Extract from primary API (JSONPlaceholder - posts)
        api_extractor = APIExtractor(config['sources']['api']['url'])
        posts_data = api_extractor.extract(config['sources']['api']['endpoint'])
        logger.info(f"Extracted {len(posts_data)} posts from primary API")
        
        # Extract from secondary API (JSONPlaceholder - users)  
        users_api = APIExtractor(config['sources']['api_secondary']['url'])
        users_data = users_api.extract(config['sources']['api_secondary']['endpoint'])
        logger.info(f"Extracted {len(users_data)} users from secondary API")
        
        # Save extracted data
        posts_data.to_csv("/tmp/api_posts_data.csv", index=False)
        users_data.to_csv("/tmp/api_users_data.csv", index=False)
        
        total_api_records = len(posts_data) + len(users_data)
        logger.info(f"API extraction completed. Total records: {total_api_records}")
        
        # Push to XCom for next tasks
        context['task_instance'].xcom_push(key='api_records_count', value=total_api_records)
        return f"API extraction successful: {total_api_records} records"
        
    except Exception as e:
        logger.error(f"API extraction failed: {e}")
        raise


def extract_file_data(**context):
    """Extract data from file sources"""
    try:
        logger.info("Starting file data extraction")
        
        # Load configuration
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'pipeline_config.yaml')
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Extract from CSV file (12k records)
        file_extractor = FileExtractor()
        file_data = file_extractor.extract(config['sources']['file']['path'])
        logger.info(f"Extracted {len(file_data)} records from CSV file")
        
        # Save extracted data
        file_data.to_csv("/tmp/file_data_extracted.csv", index=False)
        
        # Push to XCom
        context['task_instance'].xcom_push(key='file_records_count', value=len(file_data))
        return f"File extraction successful: {len(file_data)} records"
        
    except Exception as e:
        logger.error(f"File extraction failed: {e}")
        raise


def extract_database_data(**context):
    """Extract data from database sources"""
    try:
        logger.info("Starting database data extraction")
        
        # Load configuration
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'pipeline_config.yaml')
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Extract from SQLite database (13k records total)
        db_extractor = DatabaseExtractor(config['sources']['database']['path'])
        
        # Extract customers data (5k records)
        customers_data = db_extractor.extract(config['sources']['database']['queries']['customers'])
        logger.info(f"Extracted {len(customers_data)} customer records")
        
        # Extract orders data (8k records)
        orders_data = db_extractor.extract(config['sources']['database']['queries']['orders'])
        logger.info(f"Extracted {len(orders_data)} order records")
        
        # Save extracted data
        customers_data.to_csv("/tmp/customers_data.csv", index=False)
        orders_data.to_csv("/tmp/orders_data.csv", index=False)
        
        total_db_records = len(customers_data) + len(orders_data)
        
        # Push to XCom
        context['task_instance'].xcom_push(key='db_records_count', value=total_db_records)
        return f"Database extraction successful: {total_db_records} records"
        
    except Exception as e:
        logger.error(f"Database extraction failed: {e}")
        raise


def transform_data(**context):
    """Transform extracted data"""
    try:
        logger.info("Starting data transformation")
        
        # Load configuration
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'pipeline_config.yaml')
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        transformer = DataTransformer(config['transformation'])
        
        # Load all extracted data files
        import pandas as pd
        
        # Combine all datasets
        combined_data = pd.DataFrame()
        
        # Load and transform each dataset
        datasets = [
            ("/tmp/api_posts_data.csv", "api_posts"),
            ("/tmp/api_users_data.csv", "api_users"), 
            ("/tmp/file_data_extracted.csv", "file_data"),
            ("/tmp/customers_data.csv", "customers"),
            ("/tmp/orders_data.csv", "orders")
        ]
        
        transformed_datasets = []
        total_transformed_records = 0
        
        for file_path, dataset_name in datasets:
            if os.path.exists(file_path):
                df = pd.read_csv(file_path)
                transformed_df = transformer.transform(df)
                
                # Add source identifier
                transformed_df['source_dataset'] = dataset_name
                transformed_df['extraction_timestamp'] = datetime.now().isoformat()
                
                transformed_datasets.append(transformed_df)
                total_transformed_records += len(transformed_df)
                logger.info(f"Transformed {len(transformed_df)} records from {dataset_name}")
        
        # Combine all transformed data
        if transformed_datasets:
            combined_data = pd.concat(transformed_datasets, ignore_index=True)
            
            # Save combined transformed data
            combined_data.to_csv("/tmp/transformed_combined_data.csv", index=False)
            
            logger.info(f"Data transformation completed. Total records: {total_transformed_records}")
            
            # Push to XCom
            context['task_instance'].xcom_push(key='transformed_records_count', value=total_transformed_records)
            return f"Transformation successful: {total_transformed_records} records processed"
        else:
            raise Exception("No data found to transform")
        
    except Exception as e:
        logger.error(f"Data transformation failed: {e}")
        raise


def validate_data(**context):
    """Validate transformed data with comprehensive error coverage"""
    try:
        logger.info("Starting comprehensive data validation")
        
        # Load configuration
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'pipeline_config.yaml')
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Load transformed data
        import pandas as pd
        combined_data = pd.read_csv("/tmp/transformed_combined_data.csv")
        
        # Initialize validator with comprehensive rules
        validator = DataValidator(config['validation'])
        
        # Perform validation
        is_valid, validation_report = validator.validate_dataframe(combined_data, "combined_pipeline_data")
        
        # Save validation report
        import json
        with open("/tmp/validation_report.json", 'w') as f:
            json.dump(validation_report, f, indent=2)
        
        # Check Day 1 success criteria
        record_count = validation_report['record_count']
        quality_score = validation_report['data_quality_score']
        error_coverage = validator._calculate_error_coverage()
        
        success_criteria = {
            'min_records_10k': record_count >= 10000,
            'quality_score_above_80': quality_score >= 80.0,
            'error_coverage_90_percent': error_coverage >= 90.0,
            'validation_passed': is_valid
        }
        
        logger.info(f"Day 1 Success Criteria Check:")
        logger.info(f"  Records: {record_count} (≥10k: {success_criteria['min_records_10k']})")
        logger.info(f"  Quality Score: {quality_score}% (≥80%: {success_criteria['quality_score_above_80']})")
        logger.info(f"  Error Coverage: {error_coverage}% (≥90%: {success_criteria['error_coverage_90_percent']})")
        logger.info(f"  Validation Passed: {success_criteria['validation_passed']}")
        
        # Push results to XCom
        context['task_instance'].xcom_push(key='validation_results', value=validation_report)
        context['task_instance'].xcom_push(key='success_criteria', value=success_criteria)
        
        if not all(success_criteria.values()):
            logger.warning("Some Day 1 success criteria not met")
        else:
            logger.info("All Day 1 success criteria met!")
        
        return f"Validation completed. Records: {record_count}, Quality: {quality_score}%, Coverage: {error_coverage}%"
        
    except Exception as e:
        logger.error(f"Data validation failed: {e}")
        raise


def load_data(**context):
    """Load validated data to target location"""
    try:
        logger.info("Starting data loading")
        
        # Load configuration
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'pipeline_config.yaml')
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Load validated data
        import pandas as pd
        validated_data = pd.read_csv("/tmp/transformed_combined_data.csv")
        
        # Initialize loader
        loader = CSVLoader(config['output']['path'])
        
        # Load data
        output_path = loader.load(validated_data, "day1_pipeline_output")
        logger.info(f"Data loaded successfully to: {output_path}")
        
        # Push final count to XCom
        context['task_instance'].xcom_push(key='final_records_count', value=len(validated_data))
        
        return f"Data loading successful: {len(validated_data)} records saved to {output_path}"
        
    except Exception as e:
        logger.error(f"Data loading failed: {e}")
        raise


def generate_pipeline_report(**context):
    """Generate final pipeline execution report"""
    try:
        logger.info("Generating pipeline execution report")
        
        # Get all XCom values
        ti = context['task_instance']
        
        # Collect metrics from all tasks
        api_records = ti.xcom_pull(task_ids='extract_api_data', key='api_records_count') or 0
        file_records = ti.xcom_pull(task_ids='extract_file_data', key='file_records_count') or 0
        db_records = ti.xcom_pull(task_ids='extract_database_data', key='db_records_count') or 0
        transformed_records = ti.xcom_pull(task_ids='transform_data', key='transformed_records_count') or 0
        final_records = ti.xcom_pull(task_ids='load_data', key='final_records_count') or 0
        
        validation_results = ti.xcom_pull(task_ids='validate_data', key='validation_results') or {}
        success_criteria = ti.xcom_pull(task_ids='validate_data', key='success_criteria') or {}
        
        # Generate comprehensive report
        pipeline_report = {
            'pipeline_name': 'Day 1 Data Ingestion Pipeline',
            'execution_date': context['ds'],
            'execution_time': datetime.now().isoformat(),
            'source_counts': {
                'api_records': api_records,
                'file_records': file_records, 
                'database_records': db_records,
                'total_extracted': api_records + file_records + db_records
            },
            'processing_counts': {
                'transformed_records': transformed_records,
                'final_loaded_records': final_records
            },
            'validation_summary': {
                'quality_score': validation_results.get('data_quality_score', 0),
                'errors_found': len(validation_results.get('errors_found', [])),
                'warnings_found': len(validation_results.get('warnings', [])),
                'checks_performed': len(validation_results.get('checks_performed', []))
            },
            'day1_success_criteria': success_criteria,
            'pipeline_status': 'SUCCESS' if all(success_criteria.values()) else 'PARTIAL_SUCCESS'
        }
        
        # Save report
        import json
        report_path = "/tmp/day1_pipeline_report.json"
        with open(report_path, 'w') as f:
            json.dump(pipeline_report, f, indent=2)
        
        logger.info("Pipeline Report Summary:")
        logger.info(f"  Total Extracted: {pipeline_report['source_counts']['total_extracted']} records")
        logger.info(f"  Final Loaded: {final_records} records")
        logger.info(f"  Data Quality: {pipeline_report['validation_summary']['quality_score']}%")
        logger.info(f"  Pipeline Status: {pipeline_report['pipeline_status']}")
        
        return f"Pipeline report generated: {report_path}"
        
    except Exception as e:
        logger.error(f"Report generation failed: {e}")
        raise


# Define task dependencies for Day 1 pipeline
setup_task = PythonOperator(
    task_id='setup_environment',
    python_callable=setup_environment,
    dag=dag
)

extract_api_task = PythonOperator(
    task_id='extract_api_data',
    python_callable=extract_api_data,
    dag=dag
)

extract_file_task = PythonOperator(
    task_id='extract_file_data', 
    python_callable=extract_file_data,
    dag=dag
)

extract_db_task = PythonOperator(
    task_id='extract_database_data',
    python_callable=extract_database_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag
)

report_task = PythonOperator(
    task_id='generate_pipeline_report',
    python_callable=generate_pipeline_report,
    dag=dag
)

# Set task dependencies
setup_task >> [extract_api_task, extract_file_task, extract_db_task]
[extract_api_task, extract_file_task, extract_db_task] >> transform_task
transform_task >> validate_task >> load_task >> report_task
