"""SDM (Specification Document) Excel reader plugin."""

from typing import List, Dict, Any, Optional
import pandas as pd
import logging

from .base_reader import BaseExcelReader
from ...core.interfaces import PluginMetadata
from ...core.exceptions import DataProcessingError

logger = logging.getLogger(__name__)


class SDMReader(BaseExcelReader):
    """SDM (Specification Document) Excel reader plugin."""
    
    @property
    def metadata(self) -> PluginMetadata:
        """Return plugin metadata."""
        return PluginMetadata(
            name="SDMReader",
            version="1.0.0",
            description="SDM (Specification Document) Excel reader for technical specifications",
            author="XLS Reader Team",
            plugin_type="reader",
            supported_formats=["xlsx"],
            configuration_schema={
                "type": "object",
                "properties": {
                    "extract_company_info": {"type": "boolean", "default": True},
                    "extract_dates": {"type": "boolean", "default": True},
                    "extract_specifications": {"type": "boolean", "default": True}
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
        # Check if file contains SDM patterns
        if "SDM" in file_path.upper():
            return True
        
        # Check sheet names for SDM patterns
        try:
            sheet_names = self._get_sheet_names(file_path)
            for sheet_name in sheet_names:
                if any(pattern in sheet_name.upper() for pattern in ["SDM", "13127"]):
                    return True
        except Exception:
            pass
        
        return False
    
    def read_excel(self, file_path: str, config: Optional[Dict[str, Any]] = None) -> List[pd.DataFrame]:
        """Read SDM Excel file and return list of DataFrames.
        
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
            extract_company_info = config.get("extract_company_info", True)
            extract_dates = config.get("extract_dates", True)
            extract_specifications = config.get("extract_specifications", True)
            
            # Get sheet names
            sheet_names = self._get_sheet_names(file_path)
            
            dataframes = []
            
            for sheet_name in sheet_names:
                try:
                    # Read the entire sheet first to analyze structure
                    df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                    
                    # Extract different types of data
                    extracted_data = self._extract_sdm_data(df_raw, sheet_name, config)
                    
                    for data_type, df in extracted_data.items():
                        if not df.empty:
                            # Add metadata
                            df.attrs['sheet_name'] = sheet_name
                            df.attrs['source_file'] = file_path
                            df.attrs['data_type'] = data_type
                            df.attrs['sdm_document'] = True
                            
                            dataframes.append(df)
                            logger.info(f"Extracted {data_type} data from sheet '{sheet_name}': {len(df)} rows")
                    
                except Exception as e:
                    logger.warning(f"Failed to read sheet '{sheet_name}': {e}")
                    continue
            
            if not dataframes:
                raise DataProcessingError("No SDM data was successfully extracted")
            
            return dataframes
            
        except Exception as e:
            logger.error(f"Failed to read SDM Excel file {file_path}: {e}")
            raise DataProcessingError(f"Failed to read SDM Excel file: {e}")
    
    def _extract_sdm_data(self, df_raw: pd.DataFrame, sheet_name: str, config: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Extract different types of data from SDM sheet.
        
        Args:
            df_raw: Raw DataFrame from Excel sheet
            sheet_name: Name of the sheet
            config: Configuration dictionary
            
        Returns:
            Dictionary mapping data types to DataFrames
        """
        extracted_data = {}
        
        # Extract company information
        if config.get("extract_company_info", True):
            company_info = self._extract_company_info(df_raw)
            if not company_info.empty:
                extracted_data['company_info'] = company_info
        
        # Extract dates
        if config.get("extract_dates", True):
            dates_info = self._extract_dates(df_raw)
            if not dates_info.empty:
                extracted_data['dates'] = dates_info
        
        # Extract specifications
        if config.get("extract_specifications", True):
            specifications = self._extract_specifications(df_raw)
            if not specifications.empty:
                extracted_data['specifications'] = specifications
        
        return extracted_data
    
    def _extract_company_info(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """Extract company information from SDM sheet.
        
        Args:
            df_raw: Raw DataFrame from Excel sheet
            
        Returns:
            DataFrame with company information
        """
        company_data = []
        
        # Look for company name (usually in row 2, column F)
        for row_idx in range(min(10, len(df_raw))):
            for col_idx in range(len(df_raw.columns)):
                cell_value = df_raw.iloc[row_idx, col_idx]
                if cell_value and isinstance(cell_value, str):
                    if "INDUSTRIAL ACURA" in str(cell_value).upper():
                        company_data.append({
                            'type': 'company_name',
                            'value': str(cell_value),
                            'row': row_idx + 1,
                            'column': col_idx + 1
                        })
        
        if company_data:
            return pd.DataFrame(company_data)
        else:
            return pd.DataFrame()
    
    def _extract_dates(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """Extract date information from SDM sheet.
        
        Args:
            df_raw: Raw DataFrame from Excel sheet
            
        Returns:
            DataFrame with date information
        """
        dates_data = []
        
        # Look for date patterns
        for row_idx in range(len(df_raw)):
            for col_idx in range(len(df_raw.columns)):
                cell_value = df_raw.iloc[row_idx, col_idx]
                if cell_value and isinstance(cell_value, str):
                    cell_str = str(cell_value).upper()
                    if "FECHA" in cell_str:
                        # Look for actual date in nearby cells
                        for offset in range(-2, 3):
                            if 0 <= col_idx + offset < len(df_raw.columns):
                                date_value = df_raw.iloc[row_idx, col_idx + offset]
                                if date_value and str(date_value).strip():
                                    dates_data.append({
                                        'date_type': str(cell_value),
                                        'date_value': str(date_value),
                                        'row': row_idx + 1,
                                        'column': col_idx + offset + 1
                                    })
        
        if dates_data:
            return pd.DataFrame(dates_data)
        else:
            return pd.DataFrame()
    
    def _extract_specifications(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """Extract specification data from SDM sheet.
        
        Args:
            df_raw: Raw DataFrame from Excel sheet
            
        Returns:
            DataFrame with specification data
        """
        # Find the data section (usually starts after headers)
        data_start_row = None
        
        for row_idx in range(min(20, len(df_raw))):
            row_values = df_raw.iloc[row_idx].astype(str)
            if any("FECHA" in val.upper() for val in row_values):
                data_start_row = row_idx + 1
                break
        
        if data_start_row is None:
            return pd.DataFrame()
        
        # Extract data from the specification section
        spec_data = []
        
        for row_idx in range(data_start_row, len(df_raw)):
            row_values = df_raw.iloc[row_idx]
            if not row_values.isna().all():  # Skip completely empty rows
                row_dict = {}
                for col_idx, value in enumerate(row_values):
                    if pd.notna(value) and str(value).strip():
                        row_dict[f'col_{col_idx}'] = str(value)
                
                if row_dict:  # Only add non-empty rows
                    spec_data.append(row_dict)
        
        if spec_data:
            return pd.DataFrame(spec_data)
        else:
            return pd.DataFrame()
    
    def get_schema(self) -> Dict[str, Any]:
        """Get the expected schema for SDM Excel template.
        
        Returns:
            Dictionary describing the expected schema
        """
        return {
            "required_columns": [],
            "column_types": {},
            "constraints": {},
            "data_types": [
                "company_info",
                "dates", 
                "specifications"
            ]
        } 