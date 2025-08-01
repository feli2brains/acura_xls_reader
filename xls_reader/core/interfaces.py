"""Abstract base classes and interfaces for the XLSX Reader Microkernel."""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Protocol
from pathlib import Path
import pandas as pd
from pydantic import BaseModel


class PluginMetadata(BaseModel):
    """Metadata for a plugin."""
    
    name: str
    version: str
    description: str
    author: str
    plugin_type: str  # "reader" or "writer"
    supported_formats: List[str]
    configuration_schema: Optional[Dict[str, Any]] = None


class ExcelReaderPlugin(ABC):
    """Abstract base class for Excel reader plugins."""
    
    @property
    @abstractmethod
    def metadata(self) -> PluginMetadata:
        """Return plugin metadata."""
        pass
    
    @abstractmethod
    def can_handle(self, file_path: str) -> bool:
        """Check if this plugin can handle the given Excel file.
        
        Args:
            file_path: Path to the Excel file
            
        Returns:
            True if the plugin can handle this file, False otherwise
        """
        pass
    
    @abstractmethod
    def read_excel(self, file_path: str, config: Optional[Dict[str, Any]] = None) -> List[pd.DataFrame]:
        """Read Excel file and return list of DataFrames.
        
        Args:
            file_path: Path to the Excel file
            config: Optional configuration for the reader
            
        Returns:
            List of processed DataFrames
            
        Raises:
            FileNotFoundError: If file doesn't exist
            InvalidFileFormatError: If file format is not supported
            DataProcessingError: If data processing fails
        """
        pass
    
    @abstractmethod
    def get_schema(self) -> Dict[str, Any]:
        """Get the expected schema for this Excel template.
        
        Returns:
            Dictionary describing the expected schema
        """
        pass
    
    @abstractmethod
    def validate_data(self, dataframes: List[pd.DataFrame]) -> List[str]:
        """Validate the processed data against the expected schema.
        
        Args:
            dataframes: List of DataFrames to validate
            
        Returns:
            List of validation error messages (empty if valid)
        """
        pass


class DatabaseWriterPlugin(ABC):
    """Abstract base class for database writer plugins."""
    
    @property
    @abstractmethod
    def metadata(self) -> PluginMetadata:
        """Return plugin metadata."""
        pass
    
    @abstractmethod
    def can_handle(self, connection_string: str) -> bool:
        """Check if this plugin can handle the given database connection.
        
        Args:
            connection_string: Database connection string
            
        Returns:
            True if the plugin can handle this database, False otherwise
        """
        pass
    
    @abstractmethod
    def write_data(self, dataframes: List[pd.DataFrame], config: Dict[str, Any]) -> bool:
        """Write DataFrames to the database.
        
        Args:
            dataframes: List of DataFrames to write
            config: Configuration including connection string and table names
            
        Returns:
            True if write was successful, False otherwise
            
        Raises:
            ConnectionError: If database connection fails
            WriteError: If write operation fails
        """
        pass
    
    @abstractmethod
    def get_supported_databases(self) -> List[str]:
        """Get list of supported database types.
        
        Returns:
            List of supported database type names
        """
        pass
    
    @abstractmethod
    def test_connection(self, connection_string: str) -> bool:
        """Test database connection.
        
        Args:
            connection_string: Database connection string
            
        Returns:
            True if connection is successful, False otherwise
        """
        pass


class PluginRegistry(Protocol):
    """Protocol for plugin registry."""
    
    def register_reader(self, plugin: ExcelReaderPlugin) -> None:
        """Register an Excel reader plugin."""
        pass
    
    def register_writer(self, plugin: DatabaseWriterPlugin) -> None:
        """Register a database writer plugin."""
        pass
    
    def get_reader(self, file_path: str) -> Optional[ExcelReaderPlugin]:
        """Get the appropriate reader for a file."""
        pass
    
    def get_writer(self, connection_string: str) -> Optional[DatabaseWriterPlugin]:
        """Get the appropriate writer for a connection string."""
        pass
    
    def list_readers(self) -> List[ExcelReaderPlugin]:
        """List all registered readers."""
        pass
    
    def list_writers(self) -> List[DatabaseWriterPlugin]:
        """List all registered writers."""
        pass


class ConfigurationManager(Protocol):
    """Protocol for configuration management."""
    
    def load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from file."""
        pass
    
    def validate_config(self, config: Dict[str, Any], schema: Dict[str, Any]) -> List[str]:
        """Validate configuration against schema."""
        pass
    
    def get_plugin_config(self, plugin_name: str) -> Dict[str, Any]:
        """Get configuration for a specific plugin."""
        pass


class ProgressCallback(Protocol):
    """Protocol for progress callbacks."""
    
    def __call__(self, message: str, progress: float, total: int, current: int) -> None:
        """Progress callback function.
        
        Args:
            message: Progress message
            progress: Progress percentage (0.0 to 1.0)
            total: Total number of items
            current: Current item number
        """
        pass


class Logger(Protocol):
    """Protocol for logging."""
    
    def debug(self, message: str, **kwargs: Any) -> None:
        """Log debug message."""
        pass
    
    def info(self, message: str, **kwargs: Any) -> None:
        """Log info message."""
        pass
    
    def warning(self, message: str, **kwargs: Any) -> None:
        """Log warning message."""
        pass
    
    def error(self, message: str, **kwargs: Any) -> None:
        """Log error message."""
        pass
    
    def exception(self, message: str, **kwargs: Any) -> None:
        """Log exception message."""
        pass 