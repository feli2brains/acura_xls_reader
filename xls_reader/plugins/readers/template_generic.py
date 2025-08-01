"""Generic Excel reader plugin that can handle any Excel file."""

from typing import List, Dict, Any, Optional
import pandas as pd
import logging

from .base_reader import BaseExcelReader
from ...core.interfaces import PluginMetadata

logger = logging.getLogger(__name__)


class GenericExcelReader(BaseExcelReader):
    """Generic Excel reader that can handle any Excel file."""
    
    @property
    def metadata(self) -> PluginMetadata:
        """Return plugin metadata."""
        return PluginMetadata(
            name="GenericExcelReader",
            version="1.0.0",
            description="Generic Excel reader that can handle any Excel file",
            author="XLS Reader Team",
            plugin_type="reader",
            supported_formats=["xlsx", "xls"],
            configuration_schema={
                "type": "object",
                "properties": {
                    "sheet_names": {"type": "array", "items": {"type": "string"}},
                    "header_row": {"type": "integer", "default": 0},
                    "skip_rows": {"type": "array", "items": {"type": "integer"}},
                    "clean_data": {"type": "boolean", "default": True}
                }
            }
        )
    
    def can_handle(self, file_path: str) -> bool:
        """Check if this plugin can handle the given Excel file.
        
        Args:
            file_path: Path to the Excel file
            
        Returns:
            True if the plugin can handle this file, False otherwise
        """
        # Generic reader can handle any Excel file
        return file_path.lower().endswith(('.xlsx', '.xls'))
    
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
        try:
            # Validate file
            self._validate_file(file_path)
            
            # Get configuration
            config = config or {}
            sheet_names = config.get("sheet_names", [])
            header_row = config.get("header_row", 0)
            skip_rows = config.get("skip_rows", [])
            clean_data = config.get("clean_data", True)
            
            # Get all sheet names if not specified
            if not sheet_names:
                sheet_names = self._get_sheet_names(file_path)
            
            dataframes = []
            
            for sheet_name in sheet_names:
                try:
                    # Read sheet
                    df = self._read_sheet(file_path, sheet_name, header_row, skip_rows)
                    
                    # Clean data if requested
                    if clean_data:
                        df = self._clean_dataframe(df)
                    
                    # Add sheet name as metadata
                    df.attrs['sheet_name'] = sheet_name
                    df.attrs['source_file'] = file_path
                    
                    dataframes.append(df)
                    logger.info(f"Successfully read sheet '{sheet_name}' with {len(df)} rows")
                    
                except Exception as e:
                    logger.warning(f"Failed to read sheet '{sheet_name}': {e}")
                    continue
            
            if not dataframes:
                raise DataProcessingError("No sheets were successfully read")
            
            return dataframes
            
        except Exception as e:
            logger.error(f"Failed to read Excel file {file_path}: {e}")
            raise DataProcessingError(f"Failed to read Excel file: {e}")
    
    def get_schema(self) -> Dict[str, Any]:
        """Get the expected schema for this Excel template.
        
        Returns:
            Dictionary describing the expected schema
        """
        # Generic reader has no specific schema requirements
        return {
            "required_columns": [],
            "column_types": {},
            "constraints": {}
        } 