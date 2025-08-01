"""Sales Report Excel reader plugin for HOJA DE VENTAS format."""

from typing import List, Dict, Any, Optional
import pandas as pd
import logging

from .base_reader import BaseExcelReader
from ...core.interfaces import PluginMetadata
from ...core.exceptions import DataProcessingError

logger = logging.getLogger(__name__)


class SalesReportReader(BaseExcelReader):
    """Sales Report Excel reader plugin for HOJA DE VENTAS format."""
    
    @property
    def metadata(self) -> PluginMetadata:
        """Return plugin metadata."""
        return PluginMetadata(
            name="SalesReportReader",
            version="1.0.0",
            description="Sales Report Excel reader for HOJA DE VENTAS format",
            author="XLS Reader Team",
            plugin_type="reader",
            supported_formats=["xlsx"],
            configuration_schema={
                "type": "object",
                "properties": {
                    "extract_client_info": {"type": "boolean", "default": True},
                    "extract_project_info": {"type": "boolean", "default": True},
                    "extract_dates": {"type": "boolean", "default": True},
                    "extract_vendor_info": {"type": "boolean", "default": True}
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
        # Check if file contains sales report patterns
        if any(keyword in file_path.upper() for keyword in ["VENTAS", "SALES", "INFORME TECNICO", "TECHNICAL REPORT"]):
            return True
        
        # Check sheet names for sales report patterns
        try:
            sheet_names = self._get_sheet_names(file_path)
            for sheet_name in sheet_names:
                if any(pattern in sheet_name.upper() for pattern in ["VENTAS", "SALES", "INFORME", "REPORT"]):
                    return True
        except Exception:
            pass
        
        return False
    
    def read_excel(self, file_path: str, config: Optional[Dict[str, Any]] = None) -> List[pd.DataFrame]:
        """Read Sales Report Excel file and return list of DataFrames.
        
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
            extract_client_info = config.get("extract_client_info", True)
            extract_project_info = config.get("extract_project_info", True)
            extract_dates = config.get("extract_dates", True)
            extract_vendor_info = config.get("extract_vendor_info", True)
            
            # Get sheet names
            sheet_names = self._get_sheet_names(file_path)
            
            dataframes = []
            
            for sheet_name in sheet_names:
                try:
                    # Read the entire sheet first to analyze structure
                    df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                    
                    # Extract different types of data
                    extracted_data = self._extract_sales_data(df_raw, sheet_name, config)
                    
                    for data_type, df in extracted_data.items():
                        if not df.empty:
                            # Add metadata
                            df.attrs['sheet_name'] = sheet_name
                            df.attrs['source_file'] = file_path
                            df.attrs['data_type'] = data_type
                            df.attrs['sales_report'] = True
                            
                            dataframes.append(df)
                            logger.info(f"Extracted {data_type} data from sheet '{sheet_name}': {len(df)} rows")
                    
                except Exception as e:
                    logger.warning(f"Failed to read sheet '{sheet_name}': {e}")
                    continue
            
            if not dataframes:
                raise DataProcessingError("No sales report data was successfully extracted")
            
            return dataframes
            
        except Exception as e:
            logger.error(f"Failed to read Sales Report Excel file {file_path}: {e}")
            raise DataProcessingError(f"Failed to read Sales Report Excel file: {e}")
    
    def _extract_sales_data(self, df_raw: pd.DataFrame, sheet_name: str, config: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Extract different types of data from Sales Report sheet.
        
        Args:
            df_raw: Raw DataFrame from Excel sheet
            sheet_name: Name of the sheet
            config: Configuration dictionary
            
        Returns:
            Dictionary mapping data types to DataFrames
        """
        extracted_data = {}
        
        # Extract client information
        if config.get("extract_client_info", True):
            client_info = self._extract_client_info(df_raw)
            if not client_info.empty:
                extracted_data['client_info'] = client_info
        
        # Extract project information
        if config.get("extract_project_info", True):
            project_info = self._extract_project_info(df_raw)
            if not project_info.empty:
                extracted_data['project_info'] = project_info
        
        # Extract dates
        if config.get("extract_dates", True):
            dates_info = self._extract_dates(df_raw)
            if not dates_info.empty:
                extracted_data['dates'] = dates_info
        
        # Extract vendor information
        if config.get("extract_vendor_info", True):
            vendor_info = self._extract_vendor_info(df_raw)
            if not vendor_info.empty:
                extracted_data['vendor_info'] = vendor_info
        
        return extracted_data
    
    def _extract_client_info(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """Extract client information from Sales Report sheet.
        
        Args:
            df_raw: Raw DataFrame from Excel sheet
            
        Returns:
            DataFrame with client information
        """
        client_data = []
        
        # Look for client information
        for row_idx in range(len(df_raw)):
            for col_idx in range(len(df_raw.columns)):
                cell_value = df_raw.iloc[row_idx, col_idx]
                if cell_value and isinstance(cell_value, str):
                    cell_str = str(cell_value).upper()
                    if "NOMBRE DEL CLIENTE" in cell_str:
                        # Look for client name in nearby cells
                        for offset in range(-2, 3):
                            if 0 <= col_idx + offset < len(df_raw.columns):
                                client_value = df_raw.iloc[row_idx, col_idx + offset]
                                if client_value and str(client_value).strip():
                                    client_data.append({
                                        'field_type': str(cell_value),
                                        'client_value': str(client_value),
                                        'row': row_idx + 1,
                                        'column': col_idx + offset + 1
                                    })
        
        if client_data:
            return pd.DataFrame(client_data)
        else:
            return pd.DataFrame()
    
    def _extract_project_info(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """Extract project information from Sales Report sheet.
        
        Args:
            df_raw: Raw DataFrame from Excel sheet
            
        Returns:
            DataFrame with project information
        """
        project_data = []
        
        # Look for project information
        for row_idx in range(len(df_raw)):
            for col_idx in range(len(df_raw.columns)):
                cell_value = df_raw.iloc[row_idx, col_idx]
                if cell_value and isinstance(cell_value, str):
                    cell_str = str(cell_value).upper()
                    if "NOMBRE DEL PROYECTO" in cell_str:
                        # Look for project name in nearby cells
                        for offset in range(-2, 3):
                            if 0 <= col_idx + offset < len(df_raw.columns):
                                project_value = df_raw.iloc[row_idx, col_idx + offset]
                                if project_value and str(project_value).strip():
                                    project_data.append({
                                        'field_type': str(cell_value),
                                        'project_value': str(project_value),
                                        'row': row_idx + 1,
                                        'column': col_idx + offset + 1
                                    })
        
        if project_data:
            return pd.DataFrame(project_data)
        else:
            return pd.DataFrame()
    
    def _extract_dates(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """Extract date information from Sales Report sheet.
        
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
                    if any(keyword in cell_str for keyword in [
                        "FECHA REQUERIDA", "FECHA DE SOLICITUD", "FECHA DE COTIZACIÓN"
                    ]):
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
    
    def _extract_vendor_info(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """Extract vendor information from Sales Report sheet.
        
        Args:
            df_raw: Raw DataFrame from Excel sheet
            
        Returns:
            DataFrame with vendor information
        """
        vendor_data = []
        
        # Look for vendor information
        for row_idx in range(len(df_raw)):
            for col_idx in range(len(df_raw.columns)):
                cell_value = df_raw.iloc[row_idx, col_idx]
                if cell_value and isinstance(cell_value, str):
                    cell_str = str(cell_value).upper()
                    if "NOMBRE DEL VENDEDOR" in cell_str:
                        # Look for vendor name in nearby cells
                        for offset in range(-2, 3):
                            if 0 <= col_idx + offset < len(df_raw.columns):
                                vendor_value = df_raw.iloc[row_idx, col_idx + offset]
                                if vendor_value and str(vendor_value).strip():
                                    vendor_data.append({
                                        'field_type': str(cell_value),
                                        'vendor_value': str(vendor_value),
                                        'row': row_idx + 1,
                                        'column': col_idx + offset + 1
                                    })
        
        if vendor_data:
            return pd.DataFrame(vendor_data)
        else:
            return pd.DataFrame()
    
    def get_schema(self) -> Dict[str, Any]:
        """Get the expected schema for Sales Report Excel template.
        
        Returns:
            Dictionary describing the expected schema
        """
        return {
            "required_columns": [],
            "column_types": {},
            "constraints": {},
            "data_types": [
                "client_info",
                "project_info",
                "dates",
                "vendor_info"
            ]
        } 