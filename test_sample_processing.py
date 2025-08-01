#!/usr/bin/env python3
"""
Test script to process one of the sample Excel files.
This demonstrates the microkernel architecture with specific plugins.
"""

import os
import sys
from pathlib import Path

# Add the project root to the path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from xls_reader import XLSReaderKernel


def test_sample_processing():
    """Test processing of sample Excel files."""
    print("🚀 Testing Sample Excel Processing")
    print("=" * 50)
    
    # Initialize the kernel
    kernel = XLSReaderKernel()
    
    # List available plugins
    plugins = kernel.list_available_plugins()
    print(f"📚 Available plugins:")
    print(f"   Readers: {plugins['readers']}")
    print(f"   Writers: {plugins['writers']}")
    print()
    
    # Test each sample file
    sample_files = [
        "samples/SDM 13127-15.xlsx",
        "samples/Caratula de Cotización.xlsx", 
        "samples/Copia de  BOM.xlsx",
        "samples/HOJA DE VENTAS.xlsx"
    ]
    
    for sample_file in sample_files:
        if not os.path.exists(sample_file):
            print(f"❌ Sample file not found: {sample_file}")
            continue
        
        print(f"🔍 Processing: {sample_file}")
        print("-" * 40)
        
        try:
            # Find appropriate reader
            reader = kernel.plugin_manager.get_reader(sample_file)
            if reader:
                print(f"✅ Found reader: {reader.metadata.name}")
                print(f"   Description: {reader.metadata.description}")
                
                # Test reading the file
                dataframes = reader.read_excel(sample_file)
                print(f"📊 Extracted {len(dataframes)} DataFrames")
                
                for i, df in enumerate(dataframes):
                    print(f"   DataFrame {i}: {len(df)} rows, {len(df.columns)} columns")
                    if hasattr(df, 'attrs') and df.attrs:
                        print(f"      Metadata: {df.attrs}")
                
                # Test validation
                validation_errors = reader.validate_data(dataframes)
                if validation_errors:
                    print(f"⚠️  Validation warnings: {len(validation_errors)}")
                    for error in validation_errors[:3]:  # Show first 3 errors
                        print(f"      - {error}")
                else:
                    print("✅ Data validation passed")
                
            else:
                print("❌ No suitable reader found")
            
        except Exception as e:
            print(f"❌ Error processing {sample_file}: {e}")
        
        print()


def test_parquet_conversion():
    """Test converting sample files to Parquet format."""
    print("\n🔄 Testing Parquet Conversion")
    print("=" * 50)
    
    kernel = XLSReaderKernel()
    
    # Test with SDM file
    sample_file = "samples/SDM 13127-15.xlsx"
    if os.path.exists(sample_file):
        print(f"📁 Converting {sample_file} to Parquet...")
        
        try:
            results = kernel.process_excel_file(
                file_path=sample_file,
                output_format="parquet",
                output_path="output/samples"
            )
            
            print(f"✅ Successfully processed {sample_file}")
            print(f"📊 DataFrames processed: {results['dataframes_count']}")
            print(f"🔌 Reader plugin: {results['reader_plugin']}")
            
            for output in results['outputs']:
                if output['type'] == 'parquet':
                    print(f"📁 Created {output['count']} Parquet files:")
                    for file in output['files']:
                        print(f"   - {file}")
            
        except Exception as e:
            print(f"❌ Error converting to Parquet: {e}")


def test_database_writing():
    """Test writing sample files to SQLite database."""
    print("\n🗄️  Testing Database Writing")
    print("=" * 50)
    
    kernel = XLSReaderKernel()
    
    # Test with Quotation file
    sample_file = "samples/Caratula de Cotización.xlsx"
    if os.path.exists(sample_file):
        print(f"📁 Writing {sample_file} to SQLite database...")
        
        try:
            db_config = {
                'connection_string': 'sqlite:///output/samples/test_quotation.db',
                'table_names': ['quotation_data']
            }
            
            results = kernel.process_excel_file(
                file_path=sample_file,
                output_format="database",
                database_config=db_config
            )
            
            print(f"✅ Successfully processed {sample_file}")
            print(f"📊 DataFrames processed: {results['dataframes_count']}")
            print(f"🔌 Reader plugin: {results['reader_plugin']}")
            
            for output in results['outputs']:
                if output['type'] == 'database':
                    print(f"🗄️  Database write successful: {output['dataframes_written']} DataFrames")
                    print(f"   Writer plugin: {output['writer_plugin']}")
            
        except Exception as e:
            print(f"❌ Error writing to database: {e}")


def main():
    """Main test function."""
    print("🧪 XLSX Reader Microkernel - Sample Processing Test")
    print("=" * 60)
    
    # Create output directory
    os.makedirs("output/samples", exist_ok=True)
    
    # Run tests
    test_sample_processing()
    test_parquet_conversion()
    test_database_writing()
    
    print("\n🎉 Test completed!")
    print("\n📁 Check the 'output/samples' directory for generated files.")


if __name__ == "__main__":
    main() 