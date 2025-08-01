# XLSX Reader Microkernel

A robust, plugin-based Excel processing system with microkernel architecture for reading XLSX files and converting them to Parquet format or writing to various databases.

## 🏗️ Architecture

This project implements a **microkernel architecture** with the following components:

### Core System
- **Microkernel**: Central orchestrator that manages plugins and coordinates processing
- **Plugin Manager**: Dynamic discovery and registration of plugins
- **Configuration Manager**: YAML/JSON configuration handling
- **Exception Handling**: Comprehensive error management with custom exceptions

### Plugin System
- **Excel Reader Plugins**: Handle different Excel template formats
- **Database Writer Plugins**: Support for multiple database systems
- **Plugin Registry**: Automatic discovery and loading of plugins

### Key Features
- ✅ **Microkernel Pattern**: Minimal, stable core with extensible plugins
- ✅ **Plugin Architecture**: Easy to add new Excel templates and database writers
- ✅ **Multiple Database Support**: SQLite, MySQL, BigQuery (extensible)
- ✅ **Parquet Conversion**: Efficient data storage format
- ✅ **Containerized**: Docker support for development and production
- ✅ **Type Safety**: Full type hints and validation
- ✅ **Error Handling**: Robust exception management
- ✅ **Logging**: Structured logging with multiple levels
- ✅ **Testing**: Comprehensive test suite with pytest

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Docker (optional)

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd xls_reader

# Install dependencies
pip install -r requirements.txt

# Install the package
pip install -e .
```

### Basic Usage

```python
from xls_reader import XLSReaderKernel

# Initialize the kernel
kernel = XLSReaderKernel()

# Process an Excel file
results = kernel.process_excel_file(
    file_path="data/sample.xlsx",
    output_format="parquet",
    output_path="output/"
)

print(f"Processed {results['dataframes_count']} dataframes")
```

### Using Docker

```bash
# Build and run with Docker Compose
docker-compose up --build

# Run tests
docker-compose run xls-reader-test
```

## 📁 Project Structure

```
xls_reader/
├── core/                    # Microkernel core
│   ├── kernel.py           # Main microkernel
│   ├── plugin_manager.py   # Plugin registry
│   ├── interfaces.py       # Abstract base classes
│   └── exceptions.py       # Custom exceptions
├── plugins/
│   ├── readers/            # Excel template readers
│   │   ├── base_reader.py  # Abstract reader
│   │   ├── template_generic.py
│   │   ├── template_sales.py
│   │   ├── template_inventory.py
│   │   └── template_financial.py
│   └── writers/            # Database writers
│       ├── base_writer.py  # Abstract writer
│       ├── db_sqlite.py
│       ├── db_mysql.py
│       └── db_bigquery.py
├── utils/
│   ├── config.py           # Configuration management
│   ├── excel_utils.py      # Excel processing utilities
│   └── parquet_utils.py    # Parquet conversion utilities
├── tests/                  # Test suite
├── docker/                 # Containerization
├── requirements.txt        # Python dependencies
└── setup.py               # Package setup
```

## 🔌 Plugin Development

### Creating an Excel Reader Plugin

```python
from xls_reader.plugins.readers.base_reader import BaseExcelReader
from xls_reader.core.interfaces import PluginMetadata

class CustomExcelReader(BaseExcelReader):
    @property
    def metadata(self) -> PluginMetadata:
        return PluginMetadata(
            name="CustomExcelReader",
            version="1.0.0",
            description="Custom Excel reader for specific template",
            author="Your Name",
            plugin_type="reader",
            supported_formats=["xlsx"]
        )
    
    def can_handle(self, file_path: str) -> bool:
        # Implement your logic to detect if this plugin can handle the file
        return "custom_template" in file_path.lower()
    
    def read_excel(self, file_path: str, config: Optional[Dict[str, Any]] = None) -> List[pd.DataFrame]:
        # Implement your Excel reading logic
        # Return list of DataFrames
        pass
    
    def get_schema(self) -> Dict[str, Any]:
        # Return expected schema for validation
        return {
            "required_columns": ["column1", "column2"],
            "column_types": {"column1": "string", "column2": "numeric"}
        }
```

### Creating a Database Writer Plugin

```python
from xls_reader.plugins.writers.base_writer import BaseDatabaseWriter

class CustomDatabaseWriter(BaseDatabaseWriter):
    @property
    def metadata(self) -> PluginMetadata:
        return PluginMetadata(
            name="CustomDatabaseWriter",
            version="1.0.0",
            description="Custom database writer",
            author="Your Name",
            plugin_type="writer",
            supported_formats=["custom_db"]
        )
    
    def can_handle(self, connection_string: str) -> bool:
        return connection_string.startswith("custom://")
    
    def write_data(self, dataframes: List[pd.DataFrame], config: Dict[str, Any]) -> bool:
        # Implement your database writing logic
        pass
    
    def get_supported_databases(self) -> List[str]:
        return ["custom_db"]
    
    def test_connection(self, connection_string: str) -> bool:
        # Implement connection testing
        pass
```

## ⚙️ Configuration

### Configuration File (config.yaml)

```yaml
# Plugin paths
plugin_paths:
  - "plugins/readers"
  - "plugins/writers"

# Reader configuration
reader_config:
  header_row: 0
  skip_rows: []
  clean_data: true

# Writer configuration
writer_config:
  batch_size: 1000
  if_exists: "replace"

# Logging configuration
logging:
  level: "INFO"
  format: "json"

# Output configuration
output:
  directory: "output"
  compression: "snappy"
```

### Environment Variables

```bash
export XLS_READER_LOG_LEVEL=INFO
export XLS_READER_OUTPUT_DIR=/app/output
export XLS_READER_PLUGIN_PATHS=plugins/readers,plugins/writers
export XLS_READER_DB_CONNECTION=sqlite:///data/database.db
```

## 🧪 Testing

```bash
# Run all tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=xls_reader --cov-report=html

# Run specific test
pytest tests/test_kernel.py::test_process_excel_file -v
```

## 🐳 Docker Usage

### Development

```bash
# Start development environment
docker-compose up --build

# Run tests
docker-compose run xls-reader-test

# Access MySQL
docker-compose exec mysql mysql -u xls_user -p xls_reader
```

### Production

```bash
# Build production image
docker build -f docker/Dockerfile --target production -t xls-reader:latest .

# Run production container
docker run -v $(pwd)/data:/app/data -v $(pwd)/output:/app/output xls-reader:latest
```

## 📊 Supported Formats

### Excel Templates
- ✅ Generic Excel files (any format)
- ✅ Sales reports
- ✅ Inventory reports
- ✅ Financial reports
- 🔄 Custom templates (via plugins)

### Database Writers
- ✅ SQLite
- ✅ MySQL
- ✅ BigQuery
- 🔄 PostgreSQL (planned)
- 🔄 MongoDB (planned)

### Output Formats
- ✅ Parquet files
- ✅ Database tables
- 🔄 CSV files (planned)
- 🔄 JSON files (planned)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines

- Follow PEP 8 style guidelines
- Add type hints to all functions
- Write comprehensive docstrings
- Add tests for new features
- Update documentation as needed

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

- 📧 Email: support@xlsreader.com
- 📖 Documentation: [docs/](docs/)
- 🐛 Issues: [GitHub Issues](https://github.com/xls-reader/xls-reader/issues)
- 💬 Discussions: [GitHub Discussions](https://github.com/xls-reader/xls-reader/discussions)

## 🔄 Roadmap

- [ ] PostgreSQL writer plugin
- [ ] MongoDB writer plugin
- [ ] CSV/JSON output support
- [ ] Web UI for file processing
- [ ] REST API for remote processing
- [ ] Batch processing capabilities
- [ ] Data validation rules engine
- [ ] Performance monitoring
- [ ] Plugin marketplace

---

**Built with ❤️ using Python and microkernel architecture principles.** 