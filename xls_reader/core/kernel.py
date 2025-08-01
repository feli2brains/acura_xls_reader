"""Main microkernel for the XLSX Reader system."""

import os
from pathlib import Path
from typing import List, Dict, Any, Optional, Callable
import logging
import structlog
import pandas as pd

from .plugin_manager import PluginManager
from .interfaces import ExcelReaderPlugin, DatabaseWriterPlugin, ProgressCallback, Logger
from .exceptions import (
    XLSReaderError, PluginNotFoundError, FileNotFoundError, 
    InvalidFileFormatError, DataProcessingError, DatabaseError
)
from ..utils.parquet_utils import ParquetConverter
from ..utils.config import ConfigurationManager


class XLSReaderKernel:
    """Main microkernel for XLSX processing with plugin support."""
    
    def __init__(self, config_path: Optional[str] = None, logger: Optional[Logger] = None):
        """Initialize the XLSX Reader kernel.
        
        Args:
            config_path: Path to configuration file
            logger: Logger instance (optional)
        """
        self.plugin_manager = PluginManager()
        self.config_manager = ConfigurationManager()
        self.parquet_converter = ParquetConverter()
        
        # Setup logging
        if logger:
            self.logger = logger
        else:
            self._setup_logging()
        
        # Load configuration
        if config_path:
            self.config = self.config_manager.load_config(config_path)
        else:
            self.config = {}
        
        # Setup plugin paths
        self._setup_plugin_paths()
        
        # Discover plugins
        self.plugin_manager.discover_plugins()
        
        self.logger.info("XLSX Reader Kernel initialized successfully")
    
    def _setup_logging(self) -> None:
        """Setup structured logging."""
        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.UnicodeDecoder(),
                structlog.processors.JSONRenderer()
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )
        self.logger = structlog.get_logger()
    
    def _setup_plugin_paths(self) -> None:
        """Setup plugin search paths."""
        # Add default plugin paths
        default_paths = [
            "plugins/readers",
            "plugins/writers",
            os.path.join(os.path.dirname(__file__), "..", "plugins", "readers"),
            os.path.join(os.path.dirname(__file__), "..", "plugins", "writers"),
        ]
        
        for path in default_paths:
            if os.path.exists(path):
                self.plugin_manager.add_plugin_path(path)
        
        # Add custom paths from config
        custom_paths = self.config.get("plugin_paths", [])
        for path in custom_paths:
            self.plugin_manager.add_plugin_path(path)
    
    def process_excel_file(
        self,
        file_path: str,
        output_format: str = "parquet",
        output_path: Optional[str] = None,
        database_config: Optional[Dict[str, Any]] = None,
        progress_callback: Optional[ProgressCallback] = None
    ) -> Dict[str, Any]:
        """Process an Excel file through the complete pipeline.
        
        Args:
            file_path: Path to the Excel file
            output_format: Output format ("parquet", "database", or "both")
            output_path: Path for output files (if applicable)
            database_config: Database configuration (if applicable)
            progress_callback: Progress callback function
            
        Returns:
            Dictionary with processing results
            
        Raises:
            FileNotFoundError: If Excel file doesn't exist
            PluginNotFoundError: If no suitable reader/writer found
            DataProcessingError: If data processing fails
            DatabaseError: If database operations fail
        """
        self.logger.info(f"Starting processing of Excel file: {file_path}")
        
        # Validate file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(file_path)
        
        # Find appropriate reader
        reader = self.plugin_manager.get_reader(file_path)
        if not reader:
            available_readers = [r.metadata.name for r in self.plugin_manager.list_readers()]
            raise PluginNotFoundError(
                f"No suitable reader found for {file_path}",
                f"Available readers: {available_readers}"
            )
        
        self.logger.info(f"Using reader plugin: {reader.metadata.name}")
        
        # Read Excel file
        if progress_callback:
            progress_callback("Reading Excel file", 0.1, 100, 10)
        
        try:
            dataframes = reader.read_excel(file_path, self.config.get("reader_config", {}))
            self.logger.info(f"Successfully read {len(dataframes)} dataframes")
        except Exception as e:
            self.logger.error(f"Failed to read Excel file: {e}")
            raise DataProcessingError(f"Failed to read Excel file: {e}")
        
        # Validate data
        if progress_callback:
            progress_callback("Validating data", 0.3, 100, 30)
        
        validation_errors = reader.validate_data(dataframes)
        if validation_errors:
            self.logger.warning(f"Data validation warnings: {validation_errors}")
        
        results = {
            "file_path": file_path,
            "reader_plugin": reader.metadata.name,
            "dataframes_count": len(dataframes),
            "validation_errors": validation_errors,
            "outputs": []
        }
        
        # Convert to Parquet if requested
        if output_format in ["parquet", "both"]:
            if progress_callback:
                progress_callback("Converting to Parquet", 0.5, 100, 50)
            
            parquet_results = self._convert_to_parquet(
                dataframes, output_path, reader.metadata.name
            )
            results["outputs"].append(parquet_results)
        
        # Write to database if requested
        if output_format in ["database", "both"] and database_config:
            if progress_callback:
                progress_callback("Writing to database", 0.7, 100, 70)
            
            db_results = self._write_to_database(dataframes, database_config)
            results["outputs"].append(db_results)
        
        if progress_callback:
            progress_callback("Processing complete", 1.0, 100, 100)
        
        self.logger.info(f"Successfully processed {file_path}")
        return results
    
    def _convert_to_parquet(
        self, 
        dataframes: List[pd.DataFrame], 
        output_path: Optional[str], 
        reader_name: str
    ) -> Dict[str, Any]:
        """Convert DataFrames to Parquet format.
        
        Args:
            dataframes: List of DataFrames to convert
            output_path: Output directory path
            reader_name: Name of the reader plugin
            
        Returns:
            Dictionary with conversion results
        """
        if not output_path:
            output_path = "output"
        
        os.makedirs(output_path, exist_ok=True)
        
        parquet_files = []
        for i, df in enumerate(dataframes):
            filename = f"{reader_name}_dataframe_{i}.parquet"
            filepath = os.path.join(output_path, filename)
            
            try:
                self.parquet_converter.dataframe_to_parquet(df, filepath)
                parquet_files.append(filepath)
                self.logger.info(f"Converted DataFrame {i} to {filepath}")
            except Exception as e:
                self.logger.error(f"Failed to convert DataFrame {i} to Parquet: {e}")
                raise DataProcessingError(f"Parquet conversion failed: {e}")
        
        return {
            "type": "parquet",
            "files": parquet_files,
            "count": len(parquet_files)
        }
    
    def _write_to_database(
        self, 
        dataframes: List[pd.DataFrame], 
        database_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Write DataFrames to database.
        
        Args:
            dataframes: List of DataFrames to write
            database_config: Database configuration
            
        Returns:
            Dictionary with database write results
        """
        connection_string = database_config.get("connection_string")
        if not connection_string:
            raise ValueError("Database configuration must include connection_string")
        
        # Find appropriate writer
        writer = self.plugin_manager.get_writer(connection_string)
        if not writer:
            available_writers = [w.metadata.name for w in self.plugin_manager.list_writers()]
            raise PluginNotFoundError(
                f"No suitable writer found for {connection_string}",
                f"Available writers: {available_writers}"
            )
        
        self.logger.info(f"Using writer plugin: {writer.metadata.name}")
        
        try:
            success = writer.write_data(dataframes, database_config)
            if success:
                self.logger.info(f"Successfully wrote {len(dataframes)} DataFrames to database")
                return {
                    "type": "database",
                    "writer_plugin": writer.metadata.name,
                    "success": True,
                    "dataframes_written": len(dataframes)
                }
            else:
                raise DatabaseError(f"Database write operation failed for {writer.metadata.name}")
        except Exception as e:
            self.logger.error(f"Database write failed: {e}")
            raise DatabaseError(f"Database write failed: {e}")
    
    def list_available_plugins(self) -> Dict[str, List[str]]:
        """List all available plugins.
        
        Returns:
            Dictionary with lists of reader and writer plugin names
        """
        readers = [r.metadata.name for r in self.plugin_manager.list_readers()]
        writers = [w.metadata.name for w in self.plugin_manager.list_writers()]
        
        return {
            "readers": readers,
            "writers": writers
        }
    
    def get_plugin_info(self, plugin_name: str, plugin_type: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a plugin.
        
        Args:
            plugin_name: Name of the plugin
            plugin_type: Type of plugin ("reader" or "writer")
            
        Returns:
            Plugin information dictionary or None if not found
        """
        if plugin_type == "reader":
            plugin = self.plugin_manager.get_reader_by_name(plugin_name)
        elif plugin_type == "writer":
            plugin = self.plugin_manager.get_writer_by_name(plugin_name)
        else:
            return None
        
        if not plugin:
            return None
        
        return {
            "name": plugin.metadata.name,
            "version": plugin.metadata.version,
            "description": plugin.metadata.description,
            "author": plugin.metadata.author,
            "plugin_type": plugin.metadata.plugin_type,
            "supported_formats": plugin.metadata.supported_formats,
            "configuration_schema": plugin.metadata.configuration_schema
        }
    
    def test_plugin(self, plugin_name: str, plugin_type: str, test_data: Any) -> Dict[str, Any]:
        """Test a plugin with sample data.
        
        Args:
            plugin_name: Name of the plugin to test
            plugin_type: Type of plugin ("reader" or "writer")
            test_data: Test data for the plugin
            
        Returns:
            Test results dictionary
        """
        plugin = None
        if plugin_type == "reader":
            plugin = self.plugin_manager.get_reader_by_name(plugin_name)
        elif plugin_type == "writer":
            plugin = self.plugin_manager.get_writer_by_name(plugin_name)
        
        if not plugin:
            return {"success": False, "error": f"Plugin {plugin_name} not found"}
        
        try:
            if plugin_type == "reader":
                # Test reader with file path
                if plugin.can_handle(test_data):
                    return {"success": True, "message": "Plugin can handle the file"}
                else:
                    return {"success": False, "message": "Plugin cannot handle the file"}
            elif plugin_type == "writer":
                # Test writer with connection string
                if plugin.test_connection(test_data):
                    return {"success": True, "message": "Database connection successful"}
                else:
                    return {"success": False, "message": "Database connection failed"}
        except Exception as e:
            return {"success": False, "error": str(e)} 