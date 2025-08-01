"""Base database writer class for all database writer plugins."""

from abc import abstractmethod
from typing import List, Dict, Any, Optional
import pandas as pd
import logging

from ...core.interfaces import DatabaseWriterPlugin, PluginMetadata
from ...core.exceptions import ConnectionError, WriteError

logger = logging.getLogger(__name__)


class BaseDatabaseWriter(DatabaseWriterPlugin):
    """Base class for all database writer plugins."""
    
    def __init__(self):
        """Initialize the base writer."""
        self.connection = None
        self.engine = None
    
    @property
    @abstractmethod
    def metadata(self) -> PluginMetadata:
        """Return plugin metadata. Must be implemented by subclasses."""
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
    
    def _create_table_name(self, df: pd.DataFrame, config: Dict[str, Any], index: int = 0) -> str:
        """Create a table name for a DataFrame.
        
        Args:
            df: DataFrame to create table name for
            config: Configuration dictionary
            index: Index of the DataFrame
            
        Returns:
            Table name
        """
        # Check if table names are specified in config
        table_names = config.get("table_names", [])
        if index < len(table_names):
            return table_names[index]
        
        # Check if DataFrame has a sheet name attribute
        sheet_name = df.attrs.get('sheet_name', f'dataframe_{index}')
        
        # Clean sheet name for use as table name
        table_name = sheet_name.replace(' ', '_').replace('-', '_').lower()
        table_name = ''.join(c for c in table_name if c.isalnum() or c == '_')
        
        # Ensure table name starts with a letter
        if table_name and not table_name[0].isalpha():
            table_name = f"table_{table_name}"
        
        return table_name or f"dataframe_{index}"
    
    def _prepare_dataframe_for_db(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare DataFrame for database insertion.
        
        Args:
            df: DataFrame to prepare
            
        Returns:
            Prepared DataFrame
        """
        # Make a copy to avoid modifying original
        df_copy = df.copy()
        
        # Clean column names for database compatibility
        df_copy.columns = [self._clean_column_name(col) for col in df_copy.columns]
        
        # Handle data types
        for col in df_copy.columns:
            # Convert datetime columns to string if needed
            if pd.api.types.is_datetime64_any_dtype(df_copy[col]):
                df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Convert boolean columns to integer if needed
            elif pd.api.types.is_bool_dtype(df_copy[col]):
                df_copy[col] = df_copy[col].astype(int)
        
        return df_copy
    
    def _clean_column_name(self, column_name: str) -> str:
        """Clean column name for database compatibility.
        
        Args:
            column_name: Original column name
            
        Returns:
            Cleaned column name
        """
        # Convert to string and clean
        clean_name = str(column_name).strip()
        
        # Replace spaces and special characters with underscores
        clean_name = clean_name.replace(' ', '_').replace('-', '_')
        clean_name = ''.join(c for c in clean_name if c.isalnum() or c == '_')
        
        # Ensure it starts with a letter
        if clean_name and not clean_name[0].isalpha():
            clean_name = f"col_{clean_name}"
        
        # Ensure it's not empty
        if not clean_name:
            clean_name = "unnamed_column"
        
        return clean_name.lower()
    
    def _validate_config(self, config: Dict[str, Any]) -> List[str]:
        """Validate database configuration.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        required_fields = ["connection_string"]
        for field in required_fields:
            if field not in config:
                errors.append(f"Required field '{field}' is missing")
        
        return errors
    
    def _get_batch_size(self, config: Dict[str, Any]) -> int:
        """Get batch size for database operations.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Batch size
        """
        return config.get("batch_size", 1000)
    
    def _get_if_exists(self, config: Dict[str, Any]) -> str:
        """Get if_exists parameter for database operations.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            if_exists parameter
        """
        return config.get("if_exists", "replace")
    
    def _log_write_progress(self, current: int, total: int, table_name: str) -> None:
        """Log write progress.
        
        Args:
            current: Current number of rows written
            total: Total number of rows
            table_name: Name of the table being written
        """
        if total > 0:
            progress = (current / total) * 100
            logger.info(f"Writing to {table_name}: {current}/{total} rows ({progress:.1f}%)")
        else:
            logger.info(f"Writing to {table_name}: {current} rows")
    
    def _handle_write_error(self, error: Exception, table_name: str) -> None:
        """Handle write errors.
        
        Args:
            error: The error that occurred
            table_name: Name of the table being written
        """
        logger.error(f"Failed to write to table '{table_name}': {error}")
        raise WriteError(self.metadata.name, table_name, error) 