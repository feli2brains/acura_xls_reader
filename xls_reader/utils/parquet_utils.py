"""Parquet conversion utilities for the XLSX Reader Microkernel."""

import os
from pathlib import Path
from typing import List, Dict, Any, Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging

from ..core.exceptions import ParquetConversionError

logger = logging.getLogger(__name__)


class ParquetConverter:
    """Handles conversion between DataFrames and Parquet format."""
    
    def __init__(self, compression: str = "snappy"):
        """Initialize the Parquet converter.
        
        Args:
            compression: Compression algorithm to use ("snappy", "gzip", "brotli", etc.)
        """
        self.compression = compression
    
    def dataframe_to_parquet(
        self, 
        df: pd.DataFrame, 
        filepath: str, 
        index: bool = False,
        **kwargs: Any
    ) -> None:
        """Convert a DataFrame to Parquet format.
        
        Args:
            df: DataFrame to convert
            filepath: Output file path
            index: Whether to include index in the output
            **kwargs: Additional arguments for pandas to_parquet
            
        Raises:
            ParquetConversionError: If conversion fails
        """
        try:
            # Ensure output directory exists
            output_dir = os.path.dirname(filepath)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
            
            # Convert DataFrame to Parquet
            df.to_parquet(
                filepath,
                index=index,
                compression=self.compression,
                **kwargs
            )
            
            logger.info(f"Successfully converted DataFrame to {filepath}")
            
        except Exception as e:
            logger.error(f"Failed to convert DataFrame to Parquet: {e}")
            raise ParquetConversionError(
                f"Failed to convert DataFrame to Parquet: {e}",
                {"dataframe_shape": df.shape, "filepath": filepath}
            )
    
    def parquet_to_dataframe(self, filepath: str, **kwargs: Any) -> pd.DataFrame:
        """Convert a Parquet file to DataFrame.
        
        Args:
            filepath: Path to Parquet file
            **kwargs: Additional arguments for pandas read_parquet
            
        Returns:
            DataFrame from Parquet file
            
        Raises:
            ParquetConversionError: If conversion fails
        """
        try:
            df = pd.read_parquet(filepath, **kwargs)
            logger.info(f"Successfully loaded DataFrame from {filepath}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to load DataFrame from Parquet: {e}")
            raise ParquetConversionError(
                f"Failed to load DataFrame from Parquet: {e}",
                {"filepath": filepath}
            )
    
    def dataframes_to_parquet_batch(
        self, 
        dataframes: List[pd.DataFrame], 
        output_dir: str,
        filename_prefix: str = "dataframe",
        **kwargs: Any
    ) -> List[str]:
        """Convert multiple DataFrames to Parquet files.
        
        Args:
            dataframes: List of DataFrames to convert
            output_dir: Output directory
            filename_prefix: Prefix for output filenames
            **kwargs: Additional arguments for conversion
            
        Returns:
            List of created Parquet file paths
            
        Raises:
            ParquetConversionError: If any conversion fails
        """
        os.makedirs(output_dir, exist_ok=True)
        parquet_files = []
        
        for i, df in enumerate(dataframes):
            filename = f"{filename_prefix}_{i}.parquet"
            filepath = os.path.join(output_dir, filename)
            
            try:
                self.dataframe_to_parquet(df, filepath, **kwargs)
                parquet_files.append(filepath)
                
            except Exception as e:
                logger.error(f"Failed to convert DataFrame {i} to Parquet: {e}")
                # Continue with other DataFrames but log the error
                continue
        
        if not parquet_files:
            raise ParquetConversionError(
                "No DataFrames were successfully converted to Parquet",
                {"total_dataframes": len(dataframes)}
            )
        
        logger.info(f"Successfully converted {len(parquet_files)} DataFrames to Parquet")
        return parquet_files
    
    def get_parquet_metadata(self, filepath: str) -> Dict[str, Any]:
        """Get metadata from a Parquet file.
        
        Args:
            filepath: Path to Parquet file
            
        Returns:
            Dictionary with Parquet metadata
        """
        try:
            parquet_file = pq.ParquetFile(filepath)
            metadata = parquet_file.metadata
            
            return {
                "num_rows": metadata.num_rows,
                "num_columns": metadata.num_columns,
                "num_row_groups": metadata.num_row_groups,
                "file_size": os.path.getsize(filepath),
                "schema": str(metadata.schema),
                "created_by": metadata.created_by,
                "format_version": metadata.format_version
            }
            
        except Exception as e:
            logger.error(f"Failed to get Parquet metadata: {e}")
            raise ParquetConversionError(f"Failed to get Parquet metadata: {e}")
    
    def validate_parquet_file(self, filepath: str) -> bool:
        """Validate a Parquet file.
        
        Args:
            filepath: Path to Parquet file
            
        Returns:
            True if file is valid, False otherwise
        """
        try:
            # Try to read the file
            df = self.parquet_to_dataframe(filepath)
            
            # Check if DataFrame is not empty
            if df.empty:
                logger.warning(f"Parquet file {filepath} is empty")
                return False
            
            # Check for basic data integrity
            if df.isnull().all().all():
                logger.warning(f"Parquet file {filepath} contains only null values")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Parquet file {filepath} is invalid: {e}")
            return False
    
    def optimize_parquet_file(
        self, 
        input_filepath: str, 
        output_filepath: str,
        row_group_size: int = 100000
    ) -> None:
        """Optimize a Parquet file by rewriting it with better settings.
        
        Args:
            input_filepath: Path to input Parquet file
            output_filepath: Path to output optimized Parquet file
            row_group_size: Size of row groups for optimization
        """
        try:
            # Read the original file
            df = self.parquet_to_dataframe(input_filepath)
            
            # Write optimized version
            self.dataframe_to_parquet(
                df, 
                output_filepath, 
                row_group_size=row_group_size
            )
            
            logger.info(f"Successfully optimized Parquet file: {input_filepath} -> {output_filepath}")
            
        except Exception as e:
            logger.error(f"Failed to optimize Parquet file: {e}")
            raise ParquetConversionError(f"Failed to optimize Parquet file: {e}")
    
    def merge_parquet_files(
        self, 
        input_filepaths: List[str], 
        output_filepath: str
    ) -> None:
        """Merge multiple Parquet files into a single file.
        
        Args:
            input_filepaths: List of input Parquet file paths
            output_filepath: Path to output merged Parquet file
        """
        try:
            # Read all DataFrames
            dataframes = []
            for filepath in input_filepaths:
                df = self.parquet_to_dataframe(filepath)
                dataframes.append(df)
            
            # Concatenate DataFrames
            merged_df = pd.concat(dataframes, ignore_index=True)
            
            # Write merged DataFrame
            self.dataframe_to_parquet(merged_df, output_filepath)
            
            logger.info(f"Successfully merged {len(input_filepaths)} Parquet files into {output_filepath}")
            
        except Exception as e:
            logger.error(f"Failed to merge Parquet files: {e}")
            raise ParquetConversionError(f"Failed to merge Parquet files: {e}")
    
    def get_parquet_schema(self, filepath: str) -> Dict[str, Any]:
        """Get the schema of a Parquet file.
        
        Args:
            filepath: Path to Parquet file
            
        Returns:
            Dictionary with column information
        """
        try:
            df = self.parquet_to_dataframe(filepath)
            
            schema = {}
            for column in df.columns:
                schema[column] = {
                    "dtype": str(df[column].dtype),
                    "null_count": df[column].isnull().sum(),
                    "unique_count": df[column].nunique()
                }
            
            return schema
            
        except Exception as e:
            logger.error(f"Failed to get Parquet schema: {e}")
            raise ParquetConversionError(f"Failed to get Parquet schema: {e}") 