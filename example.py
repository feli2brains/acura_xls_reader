#!/usr/bin/env python3
"""
Example script demonstrating the XLSX Reader Microkernel functionality.

This script shows how to:
1. Initialize the kernel
2. Process Excel files to Parquet format
3. Write data to SQLite database
4. List available plugins
5. Get plugin information
"""

import os
import tempfile
import pandas as pd
from pathlib import Path

from xls_reader import XLSReaderKernel


def create_sample_excel_file(file_path: str):
    """Create a sample Excel file for demonstration."""
    # Create sample sales data
    sales_data = {
        'Date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'],
        'Product': ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones'],
        'Quantity': [5, 20, 15, 8, 12],
        'Price': [1200.00, 25.50, 75.00, 300.00, 150.00],
        'Region': ['North', 'South', 'East', 'West', 'North']
    }
    
    # Create sample inventory data
    inventory_data = {
        'Product_ID': ['P001', 'P002', 'P003', 'P004', 'P005'],
        'Product_Name': ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones'],
        'Stock_Level': [50, 200, 100, 30, 80],
        'Reorder_Point': [10, 50, 25, 5, 20],
        'Supplier': ['TechCorp', 'InputDev', 'KeyTech', 'DisplayCo', 'AudioPro']
    }
    
    # Create Excel file with multiple sheets
    with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
        pd.DataFrame(sales_data).to_excel(writer, sheet_name='Sales', index=False)
        pd.DataFrame(inventory_data).to_excel(writer, sheet_name='Inventory', index=False)
    
    print(f"✅ Created sample Excel file: {file_path}")


def main():
    """Main demonstration function."""
    print("🚀 XLSX Reader Microkernel Demo")
    print("=" * 50)
    
    # Initialize the kernel
    print("\n1. Initializing XLSX Reader Kernel...")
    kernel = XLSReaderKernel()
    print("✅ Kernel initialized successfully")
    
    # List available plugins
    print("\n2. Available Plugins:")
    plugins = kernel.list_available_plugins()
    
    print("   📚 Excel Readers:")
    for reader in plugins['readers']:
        print(f"      - {reader}")
    
    print("   🗄️  Database Writers:")
    for writer in plugins['writers']:
        print(f"      - {writer}")
    
    # Create sample Excel file
    print("\n3. Creating sample Excel file...")
    with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
        sample_file = tmp.name
    
    create_sample_excel_file(sample_file)
    
    try:
        # Process to Parquet format
        print("\n4. Processing Excel file to Parquet format...")
        with tempfile.TemporaryDirectory() as temp_dir:
            results = kernel.process_excel_file(
                file_path=sample_file,
                output_format="parquet",
                output_path=temp_dir
            )
            
            print(f"✅ Successfully processed {results['dataframes_count']} DataFrames")
            print(f"📊 Reader plugin used: {results['reader_plugin']}")
            
            # List created Parquet files
            for output in results['outputs']:
                if output['type'] == 'parquet':
                    print(f"📁 Created {output['count']} Parquet files:")
                    for file in output['files']:
                        print(f"   - {file}")
        
        # Process to SQLite database
        print("\n5. Processing Excel file to SQLite database...")
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, 'demo.db')
            db_config = {
                'connection_string': f'sqlite:///{db_path}',
                'table_names': ['sales_data', 'inventory_data']
            }
            
            results = kernel.process_excel_file(
                file_path=sample_file,
                output_format="database",
                database_config=db_config
            )
            
            print(f"✅ Successfully wrote to database")
            print(f"🗄️  Writer plugin used: {results['outputs'][0]['writer_plugin']}")
            print(f"📊 DataFrames written: {results['outputs'][0]['dataframes_written']}")
        
        # Process to both formats
        print("\n6. Processing Excel file to both Parquet and database...")
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, 'demo_both.db')
            db_config = {
                'connection_string': f'sqlite:///{db_path}',
                'table_names': ['sales_data', 'inventory_data']
            }
            
            results = kernel.process_excel_file(
                file_path=sample_file,
                output_format="both",
                output_path=temp_dir,
                database_config=db_config
            )
            
            print(f"✅ Successfully processed to both formats")
            print(f"📊 Total outputs: {len(results['outputs'])}")
            
            for output in results['outputs']:
                if output['type'] == 'parquet':
                    print(f"📁 Parquet files: {output['count']}")
                elif output['type'] == 'database':
                    print(f"🗄️  Database tables: {output['dataframes_written']}")
        
        # Get plugin information
        print("\n7. Plugin Information:")
        reader_info = kernel.get_plugin_info('GenericExcelReader', 'reader')
        if reader_info:
            print(f"   📚 {reader_info['name']} v{reader_info['version']}")
            print(f"      Description: {reader_info['description']}")
            print(f"      Author: {reader_info['author']}")
            print(f"      Supported formats: {', '.join(reader_info['supported_formats'])}")
        
        writer_info = kernel.get_plugin_info('SQLiteWriter', 'writer')
        if writer_info:
            print(f"   🗄️  {writer_info['name']} v{writer_info['version']}")
            print(f"      Description: {writer_info['description']}")
            print(f"      Author: {writer_info['author']}")
            print(f"      Supported formats: {', '.join(writer_info['supported_formats'])}")
        
        # Test plugin functionality
        print("\n8. Testing plugin functionality...")
        
        # Test reader plugin
        reader_test = kernel.test_plugin('GenericExcelReader', 'reader', sample_file)
        print(f"   📚 Reader test: {'✅ PASS' if reader_test['success'] else '❌ FAIL'}")
        
        # Test writer plugin
        writer_test = kernel.test_plugin('SQLiteWriter', 'writer', 'sqlite:///test.db')
        print(f"   🗄️  Writer test: {'✅ PASS' if writer_test['success'] else '❌ FAIL'}")
        
        # Validate Excel file
        print("\n9. Validating Excel file...")
        try:
            reader = kernel.plugin_manager.get_reader(sample_file)
            if reader:
                dataframes = reader.read_excel(sample_file)
                validation_errors = reader.validate_data(dataframes)
                
                if validation_errors:
                    print("   ⚠️  Validation warnings:")
                    for error in validation_errors:
                        print(f"      - {error}")
                else:
                    print("   ✅ File validation passed")
                
                print(f"   📊 DataFrames found: {len(dataframes)}")
                for i, df in enumerate(dataframes):
                    print(f"      DataFrame {i}: {len(df)} rows, {len(df.columns)} columns")
        
        except Exception as e:
            print(f"   ❌ Validation failed: {e}")
    
    finally:
        # Clean up
        if os.path.exists(sample_file):
            os.unlink(sample_file)
    
    print("\n🎉 Demo completed successfully!")
    print("\n💡 Next steps:")
    print("   - Create your own Excel reader plugins")
    print("   - Add support for more database types")
    print("   - Customize the configuration")
    print("   - Run tests: pytest tests/")
    print("   - Use CLI: python -m xls_reader.cli --help")


if __name__ == "__main__":
    main() 