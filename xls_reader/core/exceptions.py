"""Custom exceptions for the XLSX Reader Microkernel."""

from typing import Optional, Any, Dict, List


class XLSReaderError(Exception):
    """Base exception for all XLSX Reader errors."""
    
    def __init__(self, message: str, context: Optional[Dict[str, Any]] = None):
        self.message = message
        self.context = context or {}
        super().__init__(self.message)


class PluginError(XLSReaderError):
    """Base exception for plugin-related errors."""
    pass


class PluginNotFoundError(PluginError):
    """Raised when a required plugin is not found."""
    
    def __init__(self, plugin_name: str, plugin_type: str):
        message = f"Plugin '{plugin_name}' of type '{plugin_type}' not found"
        super().__init__(message, {"plugin_name": plugin_name, "plugin_type": plugin_type})


class PluginLoadError(PluginError):
    """Raised when a plugin fails to load."""
    
    def __init__(self, plugin_name: str, error: Exception):
        message = f"Failed to load plugin '{plugin_name}': {str(error)}"
        super().__init__(message, {"plugin_name": plugin_name, "original_error": error})


class PluginValidationError(PluginError):
    """Raised when a plugin fails validation."""
    
    def __init__(self, plugin_name: str, validation_errors: List[str]):
        message = f"Plugin '{plugin_name}' validation failed: {', '.join(validation_errors)}"
        super().__init__(message, {"plugin_name": plugin_name, "validation_errors": validation_errors})


class ExcelProcessingError(XLSReaderError):
    """Base exception for Excel processing errors."""
    pass


class FileNotFoundError(ExcelProcessingError):
    """Raised when the Excel file is not found."""
    
    def __init__(self, file_path: str):
        message = f"Excel file not found: {file_path}"
        super().__init__(message, {"file_path": file_path})


class InvalidFileFormatError(ExcelProcessingError):
    """Raised when the file format is invalid or unsupported."""
    
    def __init__(self, file_path: str, expected_format: str = "xlsx"):
        message = f"Invalid file format for {file_path}. Expected: {expected_format}"
        super().__init__(message, {"file_path": file_path, "expected_format": expected_format})


class DataProcessingError(ExcelProcessingError):
    """Raised when data processing fails."""
    
    def __init__(self, message: str, sheet_name: Optional[str] = None, row: Optional[int] = None):
        context = {"sheet_name": sheet_name, "row": row}
        super().__init__(message, context)


class DatabaseError(XLSReaderError):
    """Base exception for database-related errors."""
    pass


class ConnectionError(DatabaseError):
    """Raised when database connection fails."""
    
    def __init__(self, database_type: str, connection_string: str, error: Exception):
        message = f"Failed to connect to {database_type}: {str(error)}"
        super().__init__(message, {
            "database_type": database_type,
            "connection_string": connection_string,
            "original_error": error
        })


class WriteError(DatabaseError):
    """Raised when database write operation fails."""
    
    def __init__(self, database_type: str, table_name: str, error: Exception):
        message = f"Failed to write to {database_type} table '{table_name}': {str(error)}"
        super().__init__(message, {
            "database_type": database_type,
            "table_name": table_name,
            "original_error": error
        })


class ConfigurationError(XLSReaderError):
    """Raised when configuration is invalid."""
    
    def __init__(self, config_path: str, validation_errors: List[str]):
        message = f"Configuration error in {config_path}: {', '.join(validation_errors)}"
        super().__init__(message, {"config_path": config_path, "validation_errors": validation_errors})


class ParquetConversionError(XLSReaderError):
    """Raised when Parquet conversion fails."""
    
    def __init__(self, message: str, dataframe_info: Optional[Dict[str, Any]] = None):
        super().__init__(message, {"dataframe_info": dataframe_info}) 