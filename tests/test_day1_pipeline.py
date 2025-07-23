"""
Test suite for Day 1 pipeline components
"""
from src.pipelines.extractors import APIExtractor, FileExtractor
from src.pipelines.transformers import DataTransformer
from src.pipelines.loaders import CSVLoader
from src.pipelines.validation import DataValidator

import pytest
import pandas as pd
import os
import tempfile
import sys
from unittest.mock import Mock, patch, MagicMock

# Add src to path for testing
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
class TestExtractors:
    """Test data extraction functionality"""
    
    def test_file_extractor_with_csv(self):
        """Test CSV file extraction"""
        # Create temporary CSV file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write('id,name,value\n1,test1,100\n2,test2,200\n')
            temp_path = f.name
        
        try:
            extractor = FileExtractor(temp_path, 'csv')
            df = extractor.extract()
            
            assert len(df) == 2
            assert list(df.columns) == ['id', 'name', 'value']
            assert df.iloc[0]['name'] == 'test1'
            
        finally:
            os.unlink(temp_path)
    
    def test_file_extractor_validation(self):
        """Test file validation"""
        # Non-existent file
        extractor = FileExtractor('non_existent_file.csv')
        assert not extractor.validate_source()
        
        # Existing file
        with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as f:
            temp_path = f.name
        
        try:
            extractor = FileExtractor(temp_path)
            assert extractor.validate_source()
        finally:
            os.unlink(temp_path)
    
    @patch('requests.Session.get')
    def test_api_extractor_success(self, mock_get):
        """Test successful API extraction"""
        # Mock successful API response
        mock_response = Mock()
        mock_response.json.return_value = [
            {'id': 1, 'name': 'user1'},
            {'id': 2, 'name': 'user2'}
        ]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        extractor = APIExtractor('https://api.example.com')
        df = extractor.extract('/users')
        
        assert len(df) == 2
        assert 'id' in df.columns
        assert 'name' in df.columns
        
    @patch('requests.Session.get')
    def test_api_extractor_retry(self, mock_get):
        """Test API retry logic"""
        # Mock failed then successful response
        mock_get.side_effect = [
            Exception("Connection error"),
            Mock(json=lambda: [{'id': 1}], raise_for_status=lambda: None)
        ]
        
        extractor = APIExtractor('https://api.example.com', max_retries=2)
        df = extractor.extract('/test')
        
        assert len(df) == 1
        assert mock_get.call_count == 2
    
    def test_create_sample_data(self):
        """Test sample data creation"""
        # Use temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            original_cwd = os.getcwd()
            os.chdir(temp_dir)
            
            try:
                create_sample_data()
                assert os.path.exists('data/sample_data.csv')
                
                # Verify content
                df = pd.read_csv('data/sample_data.csv')
                assert len(df) == 1000
                assert list(df.columns) == ['id', 'value', 'category', 'score']
                
            finally:
                os.chdir(original_cwd)


class TestTransformers:
    """Test data transformation functionality"""
    
    def test_data_transformer_basic(self):
        """Test basic transformation"""
        df = pd.DataFrame({
            'ID': [1, 2, 3],
            'Name': ['  Alice  ', 'Bob', 'Charlie'],
            'Value': [10.5, None, 30.0]
        })
        
        transformer = DataTransformer()
        result = transformer.transform(df, 'test_source')
        
        # Check column standardization
        expected_columns = ['id', 'name', 'value', 'source_name', 'processed_at', 'record_count']
        assert all(col in result.columns for col in expected_columns)
        
        # Check data cleaning
        assert result.iloc[0]['name'] == 'Alice'  # Whitespace stripped
        assert not result['value'].isnull().any()  # Missing values handled
        
    def test_data_aggregator(self):
        """Test data aggregation"""
        df1 = pd.DataFrame({'id': [1, 2], 'source': ['A', 'A']})
        df2 = pd.DataFrame({'id': [3, 4], 'source': ['B', 'B']})
        
        dataframes = {'source1': df1, 'source2': df2}
        result = DataAggregator.combine_sources(dataframes)
        
        assert len(result) == 4
        assert 'data_source' in result.columns
        assert result['data_source'].unique().tolist() == ['source1', 'source2']


class TestLoaders:
    """Test data loading functionality"""
    
    def test_csv_loader(self):
        """Test CSV loading"""
        df = pd.DataFrame({'id': [1, 2], 'value': ['a', 'b']})
        
        with tempfile.TemporaryDirectory() as temp_dir:
            loader = CSVLoader(temp_dir)
            success = loader.load(df, 'test_file.csv')
            
            assert success
            assert os.path.exists(os.path.join(temp_dir, 'test_file.csv'))
            
            # Verify loaded data
            loaded_df = pd.read_csv(os.path.join(temp_dir, 'test_file.csv'))
            assert len(loaded_df) == 2
            assert list(loaded_df.columns) == ['id', 'value']
    
    def test_memory_loader(self):
        """Test memory loading"""
        df = pd.DataFrame({'id': [1, 2], 'value': ['a', 'b']})
        
        loader = MemoryLoader()
        success = loader.load(df, 'test_key')
        
        assert success
        assert 'test_key' in loader.list_keys()
        
        retrieved_df = loader.get('test_key')
        assert retrieved_df is not None
        assert len(retrieved_df) == 2
        
        summary = loader.get_summary()
        assert 'test_key' in summary
        assert summary['test_key']['records'] == 2


class TestValidation:
    """Test data validation functionality"""
    
    def test_basic_validation(self):
        """Test basic validation checks"""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [10, 20, 30]
        })
        
        config = {
            'min_records': 2,
            'required_columns': {
                'test_source': ['id', 'name']
            },
            'data_types': {
                'test_source': {
                    'id': 'int64',
                    'name': 'string'
                }
            }
        }
        
        validator = DataValidator(config)
        report = validator.validate(df, 'test_source')
        
        assert report.total_checks > 0
        assert report.success_rate > 80  # Should pass most checks
        assert report.source_name == 'test_source'
    
    def test_validation_failures(self):
        """Test validation with failures"""
        # Empty DataFrame
        df = pd.DataFrame()
        
        config = {'min_records': 10}
        validator = DataValidator(config)
        report = validator.validate(df, 'test_source')
        
        assert report.failed_checks > 0
        assert report.success_rate < 100
        
        # Check specific failure types
        failed_check_names = [r.check_name for r in report.results if not r.passed]
        assert 'non_empty_dataframe' in failed_check_names
        assert 'minimum_record_count' in failed_check_names


class TestIntegration:
    """Integration tests for complete pipeline"""
    
    def test_end_to_end_pipeline(self):
        """Test complete ETL pipeline"""
        # Create test data
        test_data = pd.DataFrame({
            'id': range(1, 101),  # 100 records
            'name': [f'user_{i}' for i in range(1, 101)],
            'score': [i * 0.1 for i in range(1, 101)]
        })
        
        with tempfile.TemporaryDirectory() as temp_dir:
            # Save test data
            input_file = os.path.join(temp_dir, 'input.csv')
            test_data.to_csv(input_file, index=False)
            
            # Extract
            extractor = FileExtractor(input_file, 'csv')
            extracted_df = extractor.extract()
            
            # Transform
            transformer = DataTransformer()
            transformed_df = transformer.transform(extracted_df, 'test_source')
            
            # Validate
            validator = DataValidator({'min_records': 50})
            report = validator.validate(transformed_df, 'test_source')
            
            # Load
            loader = CSVLoader(temp_dir)
            success = loader.load(transformed_df, 'output.csv')
            
            # Assertions
            assert len(extracted_df) == 100
            assert len(transformed_df) >= 100  # Should have additional columns
            assert report.success_rate > 80
            assert success
            assert os.path.exists(os.path.join(temp_dir, 'output.csv'))
    
    def test_pipeline_performance(self):
        """Test pipeline can handle required volume"""
        # Create large dataset (>1000 records)
        large_data = pd.DataFrame({
            'id': range(1, 2001),  # 2000 records
            'category': ['A', 'B', 'C'] * 667,
            'value': range(1, 2001)
        })

        # Test transformation speed
        transformer = DataTransformer()
        transformed_df = transformer.transform(large_data, 'performance_test')
        
        # Should handle large datasets
        assert len(transformed_df) >= 2000
        assert 'source_name' in transformed_df.columns
        
        # Test validation speed
        validator = DataValidator()
        report = validator.validate(transformed_df, 'performance_test')
        
        assert report.total_checks > 0
        # Performance test: should complete without timeout


if __name__ == '__main__':
    # Run tests if executed directly
    pytest.main([__file__, '-v'])
