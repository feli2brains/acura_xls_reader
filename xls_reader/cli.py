"""Command-line interface for the XLSX Reader Microkernel."""

import click
import json
import yaml
from pathlib import Path
from typing import Optional
import logging

from .core.kernel import XLSReaderKernel
from .core.exceptions import XLSReaderError


@click.group()
@click.option('--config', '-c', type=click.Path(exists=True), help='Configuration file path')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
@click.pass_context
def cli(ctx, config: Optional[str], verbose: bool):
    """XLSX Reader Microkernel - A plugin-based Excel processing system."""
    # Setup logging
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Initialize kernel
    ctx.obj = XLSReaderKernel(config_path=config)


@cli.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.option('--output-format', '-f', 
              type=click.Choice(['parquet', 'database', 'both']), 
              default='parquet',
              help='Output format')
@click.option('--output-path', '-o', 
              type=click.Path(), 
              default='output',
              help='Output directory for Parquet files')
@click.option('--database-config', '-d', 
              type=click.Path(exists=True),
              help='Database configuration file')
@click.pass_obj
def process(kernel: XLSReaderKernel, file_path: str, output_format: str, 
           output_path: str, database_config: Optional[str]):
    """Process an Excel file."""
    try:
        # Load database config if provided
        db_config = None
        if database_config:
            with open(database_config, 'r') as f:
                if database_config.endswith('.yaml') or database_config.endswith('.yml'):
                    db_config = yaml.safe_load(f)
                else:
                    db_config = json.load(f)
        
        # Process the file
        results = kernel.process_excel_file(
            file_path=file_path,
            output_format=output_format,
            output_path=output_path,
            database_config=db_config
        )
        
        # Display results
        click.echo(f"✅ Successfully processed {file_path}")
        click.echo(f"📊 DataFrames processed: {results['dataframes_count']}")
        click.echo(f"🔌 Reader plugin: {results['reader_plugin']}")
        
        if results['validation_errors']:
            click.echo("⚠️  Validation warnings:")
            for error in results['validation_errors']:
                click.echo(f"   - {error}")
        
        for output in results['outputs']:
            if output['type'] == 'parquet':
                click.echo(f"📁 Parquet files created: {output['count']}")
                for file in output['files']:
                    click.echo(f"   - {file}")
            elif output['type'] == 'database':
                click.echo(f"🗄️  Database write successful: {output['dataframes_written']} DataFrames")
                click.echo(f"   Writer plugin: {output['writer_plugin']}")
        
    except XLSReaderError as e:
        click.echo(f"❌ Error: {e.message}", err=True)
        if e.context:
            click.echo(f"   Context: {e.context}", err=True)
        exit(1)
    except Exception as e:
        click.echo(f"❌ Unexpected error: {e}", err=True)
        exit(1)


@cli.command()
@click.pass_obj
def list_plugins(kernel: XLSReaderKernel):
    """List available plugins."""
    plugins = kernel.list_available_plugins()
    
    click.echo("📚 Available Plugins:")
    click.echo()
    
    click.echo("🔍 Excel Readers:")
    for reader in plugins['readers']:
        click.echo(f"   - {reader}")
    
    click.echo()
    click.echo("🗄️  Database Writers:")
    for writer in plugins['writers']:
        click.echo(f"   - {writer}")


@cli.command()
@click.argument('plugin_name')
@click.argument('plugin_type', type=click.Choice(['reader', 'writer']))
@click.pass_obj
def plugin_info(kernel: XLSReaderKernel, plugin_name: str, plugin_type: str):
    """Get detailed information about a plugin."""
    try:
        info = kernel.get_plugin_info(plugin_name, plugin_type)
        if not info:
            click.echo(f"❌ Plugin '{plugin_name}' of type '{plugin_type}' not found", err=True)
            exit(1)
        
        click.echo(f"📋 Plugin Information:")
        click.echo(f"   Name: {info['name']}")
        click.echo(f"   Version: {info['version']}")
        click.echo(f"   Description: {info['description']}")
        click.echo(f"   Author: {info['author']}")
        click.echo(f"   Type: {info['plugin_type']}")
        click.echo(f"   Supported Formats: {', '.join(info['supported_formats'])}")
        
        if info['configuration_schema']:
            click.echo(f"   Configuration Schema: {json.dumps(info['configuration_schema'], indent=2)}")
        
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        exit(1)


@cli.command()
@click.argument('plugin_name')
@click.argument('plugin_type', type=click.Choice(['reader', 'writer']))
@click.argument('test_data')
@click.pass_obj
def test_plugin(kernel: XLSReaderKernel, plugin_name: str, plugin_type: str, test_data: str):
    """Test a plugin with sample data."""
    try:
        results = kernel.test_plugin(plugin_name, plugin_type, test_data)
        
        if results['success']:
            click.echo(f"✅ Plugin test successful: {results['message']}")
        else:
            click.echo(f"❌ Plugin test failed: {results.get('error', results['message'])}", err=True)
            exit(1)
        
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        exit(1)


@cli.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.pass_obj
def validate(kernel: XLSReaderKernel, file_path: str):
    """Validate an Excel file and show information."""
    try:
        # Find appropriate reader
        reader = kernel.plugin_manager.get_reader(file_path)
        if not reader:
            click.echo(f"❌ No suitable reader found for {file_path}", err=True)
            exit(1)
        
        click.echo(f"🔍 File Analysis:")
        click.echo(f"   File: {file_path}")
        click.echo(f"   Reader: {reader.metadata.name}")
        click.echo(f"   Description: {reader.metadata.description}")
        
        # Get schema
        schema = reader.get_schema()
        if schema.get('required_columns'):
            click.echo(f"   Required Columns: {', '.join(schema['required_columns'])}")
        
        if schema.get('column_types'):
            click.echo(f"   Column Types: {schema['column_types']}")
        
        # Test reading
        click.echo(f"\n📖 Testing file reading...")
        dataframes = reader.read_excel(file_path)
        click.echo(f"   ✅ Successfully read {len(dataframes)} DataFrames")
        
        for i, df in enumerate(dataframes):
            click.echo(f"   DataFrame {i}: {len(df)} rows, {len(df.columns)} columns")
        
        # Validate data
        validation_errors = reader.validate_data(dataframes)
        if validation_errors:
            click.echo(f"\n⚠️  Validation warnings:")
            for error in validation_errors:
                click.echo(f"   - {error}")
        else:
            click.echo(f"\n✅ Data validation passed")
        
    except Exception as e:
        click.echo(f"❌ Error: {e}", err=True)
        exit(1)


@cli.command()
@click.option('--host', default='127.0.0.1', help='Host to bind to')
@click.option('--port', default=8000, help='Port to bind to')
@click.option('--debug', is_flag=True, help='Enable debug mode')
@click.pass_obj
def serve(kernel: XLSReaderKernel, host: str, port: int, debug: bool):
    """Start a web server for the XLSX Reader."""
    try:
        from flask import Flask, request, jsonify
        from werkzeug.utils import secure_filename
        import os
        
        app = Flask(__name__)
        app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
        
        @app.route('/health', methods=['GET'])
        def health():
            return jsonify({'status': 'healthy', 'plugins': kernel.list_available_plugins()})
        
        @app.route('/process', methods=['POST'])
        def process_file():
            if 'file' not in request.files:
                return jsonify({'error': 'No file provided'}), 400
            
            file = request.files['file']
            if file.filename == '':
                return jsonify({'error': 'No file selected'}), 400
            
            if not file.filename.lower().endswith(('.xlsx', '.xls')):
                return jsonify({'error': 'Invalid file format'}), 400
            
            # Save uploaded file
            filename = secure_filename(file.filename)
            filepath = os.path.join('/tmp', filename)
            file.save(filepath)
            
            try:
                # Process the file
                results = kernel.process_excel_file(
                    file_path=filepath,
                    output_format=request.form.get('output_format', 'parquet'),
                    output_path=request.form.get('output_path', 'output')
                )
                
                # Clean up
                os.remove(filepath)
                
                return jsonify(results)
                
            except Exception as e:
                # Clean up on error
                if os.path.exists(filepath):
                    os.remove(filepath)
                return jsonify({'error': str(e)}), 500
        
        @app.route('/plugins', methods=['GET'])
        def list_plugins():
            return jsonify(kernel.list_available_plugins())
        
        click.echo(f"🚀 Starting XLSX Reader server on {host}:{port}")
        app.run(host=host, port=port, debug=debug)
        
    except ImportError:
        click.echo("❌ Flask is required for web server. Install with: pip install flask", err=True)
        exit(1)
    except Exception as e:
        click.echo(f"❌ Error starting server: {e}", err=True)
        exit(1)


if __name__ == '__main__':
    cli() 