"""BOM (Bill of Materials) Excel reader plugin."""

from typing import List, Dict, Any, Optional
import pandas as pd
import logging

from .base_reader import BaseExcelReader
from ...core.interfaces import PluginMetadata
from ...core.exceptions import DataProcessingError

logger = logging.getLogger(__name__)


class BOMReader(BaseExcelReader):
    """BOM (Bill of Materials) Excel reader plugin."""
    
    @property
    def metadata(self) -> PluginMetadata:
        """Return plugin metadata."""
        return PluginMetadata(
            name="BOMReader",
            version="1.0.0",
            description="BOM (Bill of Materials) Excel reader for material lists",
            author="XLS Reader Team",
            plugin_type="reader",
            supported_formats=["xlsx"],
            configuration_schema={
                "type": "object",
                "properties": {
                    "extract_materials": {"type": "boolean", "default": True},
                    "extract_quantities": {"type": "boolean", "default": True},
                    "extract_descriptions": {"type": "boolean", "default": True},
                    "extract_part_numbers": {"type": "boolean", "default": True}
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
        # Check if file contains BOM patterns
        if any(keyword in file_path.upper() for keyword in ["BOM", "BILL OF MATERIAL", "LISTA DE MATERIALES"]):
            return True
        
        # Check sheet names for BOM patterns
        try:
            sheet_names = self._get_sheet_names(file_path)
            for sheet_name in sheet_names:
                if any(pattern in sheet_name.upper() for pattern in ["BOM", "MATERIAL", "MATERIALES"]):
                    return True
        except Exception:
            pass
        
        return False
    
    def read_excel(self, file_path: str, config: Optional[Dict[str, Any]] = None) -> List[pd.DataFrame]:
        """Read BOM Excel file and return list of DataFrames.
        
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
            extract_materials = config.get("extract_materials", True)
            extract_quantities = config.get("extract_quantities", True)
            extract_descriptions = config.get("extract_descriptions", True)
            extract_part_numbers = config.get("extract_part_numbers", True)
            
            # Get sheet names
            sheet_names = self._get_sheet_names(file_path)
            
            dataframes = []
            
            for sheet_name in sheet_names:
                try:
                    # Read the entire sheet first to analyze structure
                    df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                    
                    # Extract different types of data
                    extracted_data = self._extract_bom_data(df_raw, sheet_name, config)
                    
                    for data_type, df in extracted_data.items():
                        if not df.empty:
                            # Add metadata
                            df.attrs['sheet_name'] = sheet_name
                            df.attrs['source_file'] = file_path
                            df.attrs['data_type'] = data_type
                            df.attrs['bom_document'] = True
                            
                            dataframes.append(df)
                            logger.info(f"Extracted {data_type} data from sheet '{sheet_name}': {len(df)} rows")
                    
                except Exception as e:
                    logger.warning(f"Failed to read sheet '{sheet_name}': {e}")
                    continue
            
            if not dataframes:
                raise DataProcessingError("No BOM data was successfully extracted")
            
            return dataframes
            
        except Exception as e:
            logger.error(f"Failed to read BOM Excel file {file_path}: {e}")
            raise DataProcessingError(f"Failed to read BOM Excel file: {e}")
    
    def _extract_bom_data(self, df_raw: pd.DataFrame, sheet_name: str, config: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
        """Extract different types of data from BOM sheet.
        
        Args:
            df_raw: Raw DataFrame from Excel sheet
            sheet_name: Name of the sheet
            config: Configuration dictionary
            
        Returns:
            Dictionary mapping data types to DataFrames
        """
        extracted_data = {}
        
        # Extract materials list
        if config.get("extract_materials", True):
            materials = self._extract_materials(df_raw)
            if not materials.empty:
                extracted_data['materials'] = materials
        
        # Extract quantities
        if config.get("extract_quantities", True):
            quantities = self._extract_quantities(df_raw)
            if not quantities.empty:
                extracted_data['quantities'] = quantities
        
        # Extract descriptions
        if config.get("extract_descriptions", True):
            descriptions = self._extract_descriptions(df_raw)
            if not descriptions.empty:
                extracted_data['descriptions'] = descriptions
        
        # Extract part numbers
        if config.get("extract_part_numbers", True):
            part_numbers = self._extract_part_numbers(df_raw)
            if not part_numbers.empty:
                extracted_data['part_numbers'] = part_numbers
        
        return extracted_data
    
    def _extract_materials(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """Extract materials list from BOM sheet.
        
        Args:
            df_raw: Raw DataFrame from Excel sheet
            
        Returns:
            DataFrame with materials information
        """
        materials_data = []
        
        # Find the header row (usually contains "Part Number", "Description", etc.)
        header_row = None
        for row_idx in range(min(20, len(df_raw))):
            row_values = df_raw.iloc[row_idx].astype(str)
            if any("PART NUMBER" in val.upper() or "PARTIDA" in val.upper() for val in row_values):
                header_row = row_idx
                break
        
        if header_row is None:
            return pd.DataFrame()
        
        # Extract materials data starting from the next row
        for row_idx in range(header_row + 1, len(df_raw)):
            row_values = df_raw.iloc[row_idx]
            if not row_values.isna().all():  # Skip completely empty rows
                material_dict = {}
                
                # Map columns based on header
                for col_idx, value in enumerate(row_values):
                    if pd.notna(value) and str(value).strip():
                        material_dict[f'col_{col_idx}'] = str(value)
                
                if material_dict:  # Only add non-empty rows
                    materials_data.append(material_dict)
        
        if materials_data:
            return pd.DataFrame(materials_data)
        else:
            return pd.DataFrame()
    
    def _extract_quantities(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """Extract quantities from BOM sheet.
        
        Args:
            df_raw: Raw DataFrame from Excel sheet
            
        Returns:
            DataFrame with quantities information
        """
        quantities_data = []
        
        # Look for quantity-related headers
        for row_idx in range(len(df_raw)):
            for col_idx in range(len(df_raw.columns)):
                cell_value = df_raw.iloc[row_idx, col_idx]
                if cell_value and isinstance(cell_value, str):
                    cell_str = str(cell_value).upper()
                    if any(keyword in cell_str for keyword in ["CANTIDAD", "QTY", "QUANTITY"]):
                        # Extract quantity data from this column
                        for data_row in range(row_idx + 1, len(df_raw)):
                            qty_value = df_raw.iloc[data_row, col_idx]
                            if pd.notna(qty_value) and str(qty_value).strip():
                                quantities_data.append({
                                    'quantity_type': str(cell_value),
                                    'quantity_value': str(qty_value),
                                    'row': data_row + 1,
                                    'column': col_idx + 1
                                })
        
        if quantities_data:
            return pd.DataFrame(quantities_data)
        else:
            return pd.DataFrame()
    
    def _extract_descriptions(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """Extract descriptions from BOM sheet.
        
        Args:
            df_raw: Raw DataFrame from Excel sheet
            
        Returns:
            DataFrame with descriptions information
        """
        descriptions_data = []
        
        # Look for description-related headers
        for row_idx in range(len(df_raw)):
            for col_idx in range(len(df_raw.columns)):
                cell_value = df_raw.iloc[row_idx, col_idx]
                if cell_value and isinstance(cell_value, str):
                    cell_str = str(cell_value).upper()
                    if any(keyword in cell_str for keyword in ["DESCRIPCIÓN", "DESCRIPTION", "DESCRIPCION"]):
                        # Extract description data from this column
                        for data_row in range(row_idx + 1, len(df_raw)):
                            desc_value = df_raw.iloc[data_row, col_idx]
                            if pd.notna(desc_value) and str(desc_value).strip():
                                descriptions_data.append({
                                    'description_type': str(cell_value),
                                    'description_value': str(desc_value),
                                    'row': data_row + 1,
                                    'column': col_idx + 1
                                })
        
        if descriptions_data:
            return pd.DataFrame(descriptions_data)
        else:
            return pd.DataFrame()
    
    def _extract_part_numbers(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """Extract part numbers from BOM sheet.
        
        Args:
            df_raw: Raw DataFrame from Excel sheet
            
        Returns:
            DataFrame with part numbers information
        """
        part_numbers_data = []
        
        # Look for part number-related headers
        for row_idx in range(len(df_raw)):
            for col_idx in range(len(df_raw.columns)):
                cell_value = df_raw.iloc[row_idx, col_idx]
                if cell_value and isinstance(cell_value, str):
                    cell_str = str(cell_value).upper()
                    if any(keyword in cell_str for keyword in ["PART NUMBER", "PART NUMBER /", "NO. DE PARTIDA"]):
                        # Extract part number data from this column
                        for data_row in range(row_idx + 1, len(df_raw)):
                            part_value = df_raw.iloc[data_row, col_idx]
                            if pd.notna(part_value) and str(part_value).strip():
                                part_numbers_data.append({
                                    'part_number_type': str(cell_value),
                                    'part_number_value': str(part_value),
                                    'row': data_row + 1,
                                    'column': col_idx + 1
                                })
        
        if part_numbers_data:
            return pd.DataFrame(part_numbers_data)
        else:
            return pd.DataFrame()
    
    def get_schema(self) -> Dict[str, Any]:
        """Get the expected schema for BOM Excel template.
        
        Returns:
            Dictionary describing the expected schema
        """
        return {
            "required_columns": [],
            "column_types": {},
            "constraints": {},
            "data_types": [
                "materials",
                "quantities",
                "descriptions",
                "part_numbers"
            ]
        } 