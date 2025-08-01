"""SQLite database writer plugin for the XLSX Reader Microkernel."""

from typing import List, Dict, Any
import pandas as pd
import sqlite3
import logging
from pathlib import Path

from .base_writer import BaseDatabaseWriter
from ...core.interfaces import PluginMetadata
from ...core.exceptions import ConnectionError, WriteError

logger = logging.getLogger(__name__)


class SQLiteWriter(BaseDatabaseWriter):
    """SQLite database writer plugin."""
    
    @property
    def metadata(self) -> PluginMetadata:
        """Return plugin metadata."""
        return PluginMetadata(
            name="SQLiteWriter",
            version="1.0.0",
            description="SQLite database writer plugin",
            author="XLS Reader Team",
            plugin_type="writer",
            supported_formats=["sqlite", "db"],
            configuration_schema={
                "type": "object",
                "properties": {
                    "connection_string": {"type": "string"},
                    "table_names": {"type": "array", "items": {"type": "string"}},
                    "batch_size": {"type": "integer", "default": 1000},
                    "if_exists": {"type": "string", "enum": ["fail", "replace", "append"], "default": "replace"}
                },
                "required": ["connection_string"]
            }
        )
    
    def can_handle(self, connection_string: str) -> bool:
        """Check if this plugin can handle the given database connection.
        
        Args:
            connection_string: Database connection string
            
        Returns:
            True if the plugin can handle this database, False otherwise
        """
        return connection_string.startswith(('sqlite:///', 'sqlite://', 'sqlite:')) or connection_string.endswith('.db')
    
    def write_data(self, dataframes: List[pd.DataFrame], config: Dict[str, Any]) -> bool:
        """Write DataFrames to SQLite database.
        
        Args:
            dataframes: List of DataFrames to write
            config: Configuration including connection string and table names
            
        Returns:
            True if write was successful, False otherwise
            
        Raises:
            ConnectionError: If database connection fails
            WriteError: If write operation fails
        """
        try:
            # Validate configuration
            validation_errors = self._validate_config(config)
            if validation_errors:
                raise ValueError(f"Configuration validation failed: {validation_errors}")
            
            connection_string = config["connection_string"]
            
            # Normalize connection string
            if not connection_string.startswith('sqlite://'):
                if connection_string.endswith('.db'):
                    connection_string = f"sqlite:///{connection_string}"
                else:
                    connection_string = f"sqlite:///{connection_string}.db"
            
            # Extract database path
            db_path = connection_string.replace('sqlite:///', '')
            
            # Ensure directory exists
            db_dir = Path(db_path).parent
            db_dir.mkdir(parents=True, exist_ok=True)
            
            # Connect to database
            try:
                self.connection = sqlite3.connect(db_path)
                logger.info(f"Connected to SQLite database: {db_path}")
            except Exception as e:
                raise ConnectionError("sqlite", connection_string, e)
            
            # Write each DataFrame
            batch_size = self._get_batch_size(config)
            if_exists = self._get_if_exists(config)
            
            for i, df in enumerate(dataframes):
                try:
                    # Prepare DataFrame
                    df_prepared = self._prepare_dataframe_for_db(df)
                    
                    # Create table name
                    table_name = self._create_table_name(df, config, i)
                    
                    # Write DataFrame to database
                    df_prepared.to_sql(
                        table_name,
                        self.connection,
                        if_exists=if_exists,
                        index=False,
                        method='multi',
                        chunksize=batch_size
                    )
                    
                    logger.info(f"Successfully wrote {len(df_prepared)} rows to table '{table_name}'")
                    
                except Exception as e:
                    self._handle_write_error(e, table_name)
            
            # Commit changes
            self.connection.commit()
            logger.info("Successfully committed all changes to SQLite database")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to write data to SQLite: {e}")
            if self.connection:
                self.connection.rollback()
            raise WriteError("sqlite", "unknown", e)
        
        finally:
            if self.connection:
                self.connection.close()
                self.connection = None
    
    def get_supported_databases(self) -> List[str]:
        """Get list of supported database types.
        
        Returns:
            List of supported database type names
        """
        return ["sqlite"]
    
    def test_connection(self, connection_string: str) -> bool:
        """Test SQLite database connection.
        
        Args:
            connection_string: Database connection string
            
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            # Normalize connection string
            if not connection_string.startswith('sqlite://'):
                if connection_string.endswith('.db'):
                    connection_string = f"sqlite:///{connection_string}"
                else:
                    connection_string = f"sqlite:///{connection_string}.db"
            
            # Extract database path
            db_path = connection_string.replace('sqlite:///', '')
            
            # Test connection
            conn = sqlite3.connect(db_path)
            conn.close()
            
            logger.info(f"Successfully tested SQLite connection: {db_path}")
            return True
            
        except Exception as e:
            logger.error(f"SQLite connection test failed: {e}")
            return False
    
    def create_indexes(self, table_name: str, columns: List[str]) -> None:
        """Create indexes on specified columns.
        
        Args:
            table_name: Name of the table
            columns: List of column names to index
        """
        if not self.connection:
            raise ConnectionError("sqlite", "no connection", Exception("No active connection"))
        
        try:
            cursor = self.connection.cursor()
            
            for column in columns:
                index_name = f"idx_{table_name}_{column}"
                cursor.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({column})")
            
            self.connection.commit()
            logger.info(f"Created indexes on table '{table_name}' for columns: {columns}")
            
        except Exception as e:
            logger.error(f"Failed to create indexes: {e}")
            raise WriteError("sqlite", table_name, e)
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get information about a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary with table information
        """
        if not self.connection:
            raise ConnectionError("sqlite", "no connection", Exception("No active connection"))
        
        try:
            cursor = self.connection.cursor()
            
            # Get table schema
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            
            return {
                "table_name": table_name,
                "columns": [{"name": col[1], "type": col[2], "not_null": col[3], "default": col[4]} for col in columns],
                "row_count": row_count
            }
            
        except Exception as e:
            logger.error(f"Failed to get table info: {e}")
            raise WriteError("sqlite", table_name, e) 