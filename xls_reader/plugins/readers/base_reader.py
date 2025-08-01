"""Base Excel reader class for all reader plugins."""

from abc import abstractmethod
from typing import List, Dict, Any, Optional
import pandas as pd
import logging

from ...core.interfaces import ExcelReaderPlugin, PluginMetadata
from ...core.exceptions import FileNotFoundError, InvalidFileFormatError, DataProcessingError
from ...utils.excel_utils import ExcelUtils

logger = logging.getLogger(__name__)


class BaseExcelReader(ExcelReaderPlugin):
    """Base class for all Excel reader plugins."""
    
    def __init__(self):
        """Initialize the base reader."""
        self.excel_utils = ExcelUtils()
    
    @property
    @abstractmethod
    def metadata(self) -> PluginMetadata:
        """Return plugin metadata. Must be implemented by subclasses."""
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
    
    def validate_data(self, dataframes: List[pd.DataFrame]) -> List[str]:
        """Validate the processed data against the expected schema.
        
        Args:
            dataframes: List of DataFrames to validate
            
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        try:
            schema = self.get_schema()
            
            for i, df in enumerate(dataframes):
                if df.empty:
                    errors.append(f"DataFrame {i} is empty")
                    continue
                
                # Validate against schema
                schema_errors = ExcelUtils.validate_dataframe_schema(df, schema)
                errors.extend([f"DataFrame {i}: {error}" for error in schema_errors])
            
            return errors
            
        except Exception as e:
            logger.error(f"Data validation failed: {e}")
            return [f"Validation error: {e}"]
    
    def _validate_file(self, file_path: str) -> None:
        """Validate that the file exists and is a valid Excel file.
        
        Args:
            file_path: Path to the Excel file
            
        Raises:
            FileNotFoundError: If file doesn't exist
            InvalidFileFormatError: If file is not a valid Excel file
        """
        if not self.excel_utils.validate_excel_file(file_path):
            raise InvalidFileFormatError(file_path)
    
    def _get_sheet_names(self, file_path: str) -> List[str]:
        """Get list of sheet names from Excel file.
        
        Args:
            file_path: Path to the Excel file
            
        Returns:
            List of sheet names
        """
        return self.excel_utils.get_sheet_names(file_path)
    
    def _read_sheet(
        self, 
        file_path: str, 
        sheet_name: str, 
        header_row: int = 0,
        skip_rows: Optional[List[int]] = None
    ) -> pd.DataFrame:
        """Read a specific sheet from Excel file.
        
        Args:
            file_path: Path to the Excel file
            sheet_name: Name of the sheet to read
            header_row: Row number to use as header (0-indexed)
            skip_rows: Additional rows to skip
            
        Returns:
            DataFrame from the sheet
        """
        return self.excel_utils.read_excel_with_header(
            file_path, sheet_name, header_row, skip_rows
        )
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean a DataFrame by removing empty rows/columns.
        
        Args:
            df: DataFrame to clean
            
        Returns:
            Cleaned DataFrame
        """
        return self.excel_utils.clean_dataframe(df)
    
    def _detect_data_types(self, df: pd.DataFrame) -> Dict[str, str]:
        """Detect data types for each column in a DataFrame.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary mapping column names to detected data types
        """
        return self.excel_utils.detect_data_types(df)
    
    def _find_header_row(
        self, 
        file_path: str, 
        sheet_name: str, 
        header_keywords: List[str]
    ) -> Optional[int]:
        """Find the header row in an Excel sheet.
        
        Args:
            file_path: Path to the Excel file
            sheet_name: Name of the sheet
            header_keywords: List of keywords to look for in header
            
        Returns:
            Row number of header (0-indexed) or None if not found
        """
        return self.excel_utils.find_header_row(file_path, sheet_name, header_keywords)
    
    def _extract_data_range(
        self,
        file_path: str,
        sheet_name: str,
        start_row: int,
        end_row: Optional[int] = None,
        start_col: int = 0,
        end_col: Optional[int] = None
    ) -> pd.DataFrame:
        """Extract a specific range of data from Excel.
        
        Args:
            file_path: Path to the Excel file
            sheet_name: Name of the sheet
            start_row: Starting row (0-indexed)
            end_row: Ending row (0-indexed, inclusive)
            start_col: Starting column (0-indexed)
            end_col: Ending column (0-indexed, inclusive)
            
        Returns:
            DataFrame with the extracted data
        """
        return self.excel_utils.extract_data_range(
            file_path, sheet_name, start_row, end_row, start_col, end_col
        ) 