"""Tests for the XLSX Reader Kernel."""

import pytest
import pandas as pd
import tempfile
import os
from pathlib import Path

from xls_reader.core.kernel import XLSReaderKernel
from xls_reader.core.exceptions import XLSReaderError


class TestXLSReaderKernel:
    """Test cases for the XLSX Reader Kernel."""
    
    @pytest.fixture
    def kernel(self):
        """Create a kernel instance for testing."""
        return XLSReaderKernel()
    
    @pytest.fixture
    def sample_excel_file(self):
        """Create a sample Excel file for testing."""
        # Create sample data
        data = {
            'Name': ['John', 'Jane', 'Bob'],
            'Age': [25, 30, 35],
            'City': ['New York', 'Los Angeles', 'Chicago']
        }
        df = pd.DataFrame(data)
        
        # Create temporary Excel file
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            df.to_excel(tmp.name, index=False)
            yield tmp.name
        
        # Clean up
        os.unlink(tmp.name)
    
    def test_kernel_initialization(self, kernel):
        """Test that the kernel initializes correctly."""
        assert kernel is not None
        assert kernel.plugin_manager is not None
        assert kernel.config is not None
    
    def test_list_available_plugins(self, kernel):
        """Test listing available plugins."""
        plugins = kernel.list_available_plugins()
        
        assert 'readers' in plugins
        assert 'writers' in plugins
        assert isinstance(plugins['readers'], list)
        assert isinstance(plugins['writers'], list)
    
    def test_process_excel_file_parquet(self, kernel, sample_excel_file):
        """Test processing Excel file to Parquet format."""
        with tempfile.TemporaryDirectory() as temp_dir:
            results = kernel.process_excel_file(
                file_path=sample_excel_file,
                output_format="parquet",
                output_path=temp_dir
            )
            
            assert results['file_path'] == sample_excel_file
            assert results['dataframes_count'] == 1
            assert results['reader_plugin'] == 'GenericExcelReader'
            assert len(results['outputs']) == 1
            assert results['outputs'][0]['type'] == 'parquet'
            assert results['outputs'][0]['count'] == 1
    
    def test_process_excel_file_database(self, kernel, sample_excel_file):
        """Test processing Excel file to database format."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, 'test.db')
            db_config = {
                'connection_string': f'sqlite:///{db_path}',
                'table_names': ['test_table']
            }
            
            results = kernel.process_excel_file(
                file_path=sample_excel_file,
                output_format="database",
                database_config=db_config
            )
            
            assert results['file_path'] == sample_excel_file
            assert results['dataframes_count'] == 1
            assert len(results['outputs']) == 1
            assert results['outputs'][0]['type'] == 'database'
            assert results['outputs'][0]['success'] is True
    
    def test_process_excel_file_both_formats(self, kernel, sample_excel_file):
        """Test processing Excel file to both Parquet and database formats."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, 'test.db')
            db_config = {
                'connection_string': f'sqlite:///{db_path}',
                'table_names': ['test_table']
            }
            
            results = kernel.process_excel_file(
                file_path=sample_excel_file,
                output_format="both",
                output_path=temp_dir,
                database_config=db_config
            )
            
            assert results['file_path'] == sample_excel_file
            assert results['dataframes_count'] == 1
            assert len(results['outputs']) == 2
            
            # Check that both outputs are present
            output_types = [output['type'] for output in results['outputs']]
            assert 'parquet' in output_types
            assert 'database' in output_types
    
    def test_process_nonexistent_file(self, kernel):
        """Test processing a non-existent file."""
        with pytest.raises(XLSReaderError):
            kernel.process_excel_file(
                file_path="nonexistent_file.xlsx",
                output_format="parquet"
            )
    
    def test_get_plugin_info(self, kernel):
        """Test getting plugin information."""
        # Test with existing plugin
        info = kernel.get_plugin_info('GenericExcelReader', 'reader')
        assert info is not None
        assert info['name'] == 'GenericExcelReader'
        assert info['plugin_type'] == 'reader'
        
        # Test with non-existing plugin
        info = kernel.get_plugin_info('NonExistentPlugin', 'reader')
        assert info is None
    
    def test_test_plugin(self, kernel, sample_excel_file):
        """Test plugin testing functionality."""
        # Test reader plugin
        results = kernel.test_plugin('GenericExcelReader', 'reader', sample_excel_file)
        assert results['success'] is True
        
        # Test writer plugin
        results = kernel.test_plugin('SQLiteWriter', 'writer', 'sqlite:///test.db')
        assert results['success'] is True
    
    def test_plugin_discovery(self, kernel):
        """Test that plugins are discovered correctly."""
        readers = kernel.plugin_manager.list_readers()
        writers = kernel.plugin_manager.list_writers()
        
        # Should have at least the generic reader
        assert len(readers) >= 1
        
        # Should have at least SQLite writer
        assert len(writers) >= 1
        
        # Check that plugins have required attributes
        for reader in readers:
            assert hasattr(reader, 'metadata')
            assert hasattr(reader, 'can_handle')
            assert hasattr(reader, 'read_excel')
            assert hasattr(reader, 'get_schema')
            assert hasattr(reader, 'validate_data')
        
        for writer in writers:
            assert hasattr(writer, 'metadata')
            assert hasattr(writer, 'can_handle')
            assert hasattr(writer, 'write_data')
            assert hasattr(writer, 'get_supported_databases')
            assert hasattr(writer, 'test_connection')
    
    def test_configuration_loading(self):
        """Test configuration loading."""
        # Test with default configuration
        kernel = XLSReaderKernel()
        assert kernel.config is not None
        
        # Test with custom configuration file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp:
            tmp.write("""
plugin_paths:
  - "plugins/readers"
  - "plugins/writers"
logging:
  level: "DEBUG"
""")
            tmp.flush()
            
            kernel = XLSReaderKernel(config_path=tmp.name)
            assert kernel.config is not None
            assert 'plugin_paths' in kernel.config
            assert 'logging' in kernel.config
            
            os.unlink(tmp.name)
    
    def test_error_handling(self, kernel):
        """Test error handling."""
        # Test with invalid file
        with pytest.raises(XLSReaderError):
            kernel.process_excel_file(
                file_path="invalid_file.txt",
                output_format="parquet"
            )
        
        # Test with invalid output format
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            pd.DataFrame({'test': [1, 2, 3]}).to_excel(tmp.name, index=False)
            
            with pytest.raises(ValueError):
                kernel.process_excel_file(
                    file_path=tmp.name,
                    output_format="invalid_format"
                )
            
            os.unlink(tmp.name) 