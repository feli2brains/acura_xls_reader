"""Quotation Excel reader plugin for Caratula de Cotización format."""

from typing import List, Dict, Any, Optional
import pandas as pd
import logging

from .base_reader import BaseExcelReader
from ...core.interfaces import PluginMetadata
from ...core.exceptions import DataProcessingError

logger = logging.getLogger(__name__)


class QuotationReader(BaseExcelReader):
    """Quotation Excel reader plugin for Caratula de Cotización format."""
    
    @property
    def metadata(self) -> PluginMetadata:
        """Return plugin metadata."""
        return PluginMetadata(
            name="QuotationReader",
            version="1.0.0",
            description="Quotation Excel reader for Caratula de Cotización format",
            author="XLS Reader Team",
            plugin_type="reader",
            supported_formats=["xlsx"],
            configuration_schema={
                "type": "object",
                "properties": {
                    "extract_company_info": {"type": "boolean", "default": True},
                    "extract_client_info": {"type": "boolean", "default": True},
                    "extract_quotation_details": {"type": "boolean", "default": True},
                    "extract_design_codes": {"type": "boolean", "default": True}
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
        # Check if file contains quotation patterns
        if any(keyword in file_path.upper() for keyword in ["COTIZACION", "COTIZACIÓN", "QUOTATION"]):
            return True
        
        # Check sheet names for quotation patterns
        try:
            sheet_names = self._get_sheet_names(file_path)
            for sheet_name in sheet_names:
                if any(pattern in sheet_name.upper() for pattern in ["COTIZACION", "COTIZACIÓN", "QUOTATION"]):
                    return True
        except Exception:
            pass
        
        return False
    
    def read_excel(self, file_path: str, config: Optional[Dict[str, Any]] = None) -> List[pd.DataFrame]:
        """Read Quotation Excel file and return list of DataFrames.
        
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
            extract_client_info = config.get("extract_client_info", True)
            extract_quotation_details = config.get("extract_quotation_details", True)
            extract_design_codes = config.get("extract_design_codes", True)
            
            # Get sheet names
            sheet_names = self._get_sheet_names(file_path)
            
            dataframes = []
            
            for sheet_name in sheet_names:
                try:
                    # Read the entire sheet first to analyze structure
                    df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                    
                    # Extract different types of data
                    extracted_data = self._extract_quotation_data(df_raw, sheet_name, config)
                    
                    for data_type, df in extracted_data.items():
                        if not df.empty:
                            # Add metadata
                            df.attrs['sheet_name'] = sheet_name
                            df.attrs['source_file'] = file_path
                            df.attrs['data_type'] = data_type
                            df.attrs['quotation_document'] = True
                            
                            dataframes.append(df)
                            logger.info(f"Extracted {data_type} data from sheet '{sheet_name}': {len(df)} rows")
                    
                except Exception as e:
                    logger.warning(f"Failed to read sheet '{sheet_name}': {e}")
                    continue
            
            if not dataframes:
                raise DataProcessingError("No quotation data was successfully extracted")
            
            return dataframes
            
        except Exception as e:
            logger.error(f"Failed to read Quotation Excel file {file_path}: {e}")
            raise DataProcessingError(f"Failed to read Quotation Excel file: {e}")
    
    def _extract_quotation_data(self, df_raw: pd.DataFrame, sheet_name: str, config: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Extract different types of data from Quotation sheet.
        
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
        
        # Extract client information
        if config.get("extract_client_info", True):
            client_info = self._extract_client_info(df_raw)
            if not client_info.empty:
                extracted_data['client_info'] = client_info
        
        # Extract quotation details
        if config.get("extract_quotation_details", True):
            quotation_details = self._extract_quotation_details(df_raw)
            if not quotation_details.empty:
                extracted_data['quotation_details'] = quotation_details
        
        # Extract design codes
        if config.get("extract_design_codes", True):
            design_codes = self._extract_design_codes(df_raw)
            if not design_codes.empty:
                extracted_data['design_codes'] = design_codes
        
        return extracted_data
    
    def _extract_company_info(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """Extract company information from Quotation sheet.
        
        Args:
            df_raw: Raw DataFrame from Excel sheet
            
        Returns:
            DataFrame with company information
        """
        company_data = []
        
        # Look for company name (usually in row 2, column D)
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
    
    def _extract_client_info(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """Extract client information from Quotation sheet.
        
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
                    if "CLIENTE" in cell_str:
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
    
    def _extract_quotation_details(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """Extract quotation details from Quotation sheet.
        
        Args:
            df_raw: Raw DataFrame from Excel sheet
            
        Returns:
            DataFrame with quotation details
        """
        quotation_data = []
        
        # Look for quotation format information
        for row_idx in range(len(df_raw)):
            for col_idx in range(len(df_raw.columns)):
                cell_value = df_raw.iloc[row_idx, col_idx]
                if cell_value and isinstance(cell_value, str):
                    cell_str = str(cell_value).upper()
                    if "FORMATO DE COTIZACIÓN" in cell_str:
                        quotation_data.append({
                            'type': 'quotation_format',
                            'value': str(cell_value),
                            'row': row_idx + 1,
                            'column': col_idx + 1
                        })
                    elif "FOLIO" in cell_str:
                        # Look for folio number in nearby cells
                        for offset in range(-2, 3):
                            if 0 <= col_idx + offset < len(df_raw.columns):
                                folio_value = df_raw.iloc[row_idx, col_idx + offset]
                                if folio_value and str(folio_value).strip():
                                    quotation_data.append({
                                        'type': 'folio',
                                        'value': str(folio_value),
                                        'row': row_idx + 1,
                                        'column': col_idx + offset + 1
                                    })
        
        if quotation_data:
            return pd.DataFrame(quotation_data)
        else:
            return pd.DataFrame()
    
    def _extract_design_codes(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """Extract design codes from Quotation sheet.
        
        Args:
            df_raw: Raw DataFrame from Excel sheet
            
        Returns:
            DataFrame with design codes
        """
        design_data = []
        
        # Look for design code information
        for row_idx in range(len(df_raw)):
            for col_idx in range(len(df_raw.columns)):
                cell_value = df_raw.iloc[row_idx, col_idx]
                if cell_value and isinstance(cell_value, str):
                    cell_str = str(cell_value).upper()
                    if "CÓDIGO DE DISEÑO" in cell_str or "CODIGO DE DISEÑO" in cell_str:
                        # Look for design code in nearby cells
                        for offset in range(-2, 3):
                            if 0 <= col_idx + offset < len(df_raw.columns):
                                code_value = df_raw.iloc[row_idx, col_idx + offset]
                                if code_value and str(code_value).strip():
                                    design_data.append({
                                        'type': 'design_code',
                                        'value': str(code_value),
                                        'row': row_idx + 1,
                                        'column': col_idx + offset + 1
                                    })
        
        if design_data:
            return pd.DataFrame(design_data)
        else:
            return pd.DataFrame()
    
    def get_schema(self) -> Dict[str, Any]:
        """Get the expected schema for Quotation Excel template.
        
        Returns:
            Dictionary describing the expected schema
        """
        return {
            "required_columns": [],
            "column_types": {},
            "constraints": {},
            "data_types": [
                "company_info",
                "client_info",
                "quotation_details",
                "design_codes"
            ]
        } 