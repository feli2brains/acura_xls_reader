"""Plugin manager for dynamic plugin discovery and registration."""

import importlib
import inspect
import os
from pathlib import Path
from typing import List, Dict, Any, Optional, Type
import logging

from .interfaces import ExcelReaderPlugin, DatabaseWriterPlugin, PluginRegistry
from .exceptions import PluginNotFoundError, PluginLoadError, PluginValidationError


logger = logging.getLogger(__name__)


class PluginManager(PluginRegistry):
    """Manages plugin discovery, loading, and registration."""
    
    def __init__(self):
        """Initialize the plugin manager."""
        self._readers: Dict[str, ExcelReaderPlugin] = {}
        self._writers: Dict[str, DatabaseWriterPlugin] = {}
        self._plugin_paths: List[str] = []
    
    def add_plugin_path(self, path: str) -> None:
        """Add a path to search for plugins.
        
        Args:
            path: Directory path to search for plugins
        """
        if path not in self._plugin_paths:
            self._plugin_paths.append(path)
            logger.info(f"Added plugin path: {path}")
    
    def discover_plugins(self) -> None:
        """Discover and load all available plugins."""
        for plugin_path in self._plugin_paths:
            self._discover_plugins_in_path(plugin_path)
    
    def _discover_plugins_in_path(self, path: str) -> None:
        """Discover plugins in a specific path.
        
        Args:
            path: Path to search for plugins
        """
        if not os.path.exists(path):
            logger.warning(f"Plugin path does not exist: {path}")
            return
        
        for root, dirs, files in os.walk(path):
            # Skip __pycache__ and hidden directories
            dirs[:] = [d for d in dirs if not d.startswith('.') and d != '__pycache__']
            
            for file in files:
                if file.endswith('.py') and not file.startswith('__'):
                    self._try_load_plugin_from_file(root, file)
    
    def _try_load_plugin_from_file(self, root: str, file: str) -> None:
        """Try to load a plugin from a Python file.
        
        Args:
            root: Root directory of the file
            file: Python file name
        """
        try:
            # Construct module path
            rel_path = os.path.relpath(root, start=os.getcwd())
            module_path = rel_path.replace(os.sep, '.')
            if module_path.startswith('.'):
                module_path = module_path[1:]
            
            module_name = f"{module_path}.{file[:-3]}" if module_path else file[:-3]
            
            # Import the module
            module = importlib.import_module(module_name)
            
            # Look for plugin classes
            for name, obj in inspect.getmembers(module):
                if (inspect.isclass(obj) and 
                    issubclass(obj, (ExcelReaderPlugin, DatabaseWriterPlugin)) and
                    obj not in (ExcelReaderPlugin, DatabaseWriterPlugin) and
                    not inspect.isabstract(obj)):
                    
                    self._load_plugin_class(obj)
                    
        except Exception as e:
            logger.warning(f"Failed to load plugin from {os.path.join(root, file)}: {e}")
    
    def _load_plugin_class(self, plugin_class: Type) -> None:
        """Load a plugin class and register it.
        
        Args:
            plugin_class: The plugin class to load
        """
        try:
            # Instantiate the plugin
            plugin = plugin_class()
            
            # Validate the plugin
            validation_errors = self._validate_plugin(plugin)
            if validation_errors:
                raise PluginValidationError(plugin.metadata.name, validation_errors)
            
            # Register the plugin
            if isinstance(plugin, ExcelReaderPlugin):
                self.register_reader(plugin)
                logger.info(f"Registered reader plugin: {plugin.metadata.name}")
            elif isinstance(plugin, DatabaseWriterPlugin):
                self.register_writer(plugin)
                logger.info(f"Registered writer plugin: {plugin.metadata.name}")
                
        except Exception as e:
            logger.error(f"Failed to load plugin class {plugin_class.__name__}: {e}")
            raise PluginLoadError(plugin_class.__name__, e)
    
    def _validate_plugin(self, plugin) -> List[str]:
        """Validate a plugin.
        
        Args:
            plugin: The plugin to validate
            
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        # Check metadata
        if not hasattr(plugin, 'metadata'):
            errors.append("Plugin must have metadata property")
        else:
            metadata = plugin.metadata
            if not metadata.name:
                errors.append("Plugin metadata must have a name")
            if not metadata.version:
                errors.append("Plugin metadata must have a version")
            if not metadata.plugin_type:
                errors.append("Plugin metadata must have a plugin_type")
        
        # Check required methods
        if isinstance(plugin, ExcelReaderPlugin):
            required_methods = ['can_handle', 'read_excel', 'get_schema', 'validate_data']
        elif isinstance(plugin, DatabaseWriterPlugin):
            required_methods = ['can_handle', 'write_data', 'get_supported_databases', 'test_connection']
        else:
            errors.append("Plugin must inherit from ExcelReaderPlugin or DatabaseWriterPlugin")
            return errors
        
        for method in required_methods:
            if not hasattr(plugin, method) or not callable(getattr(plugin, method)):
                errors.append(f"Plugin must implement {method} method")
        
        return errors
    
    def register_reader(self, plugin: ExcelReaderPlugin) -> None:
        """Register an Excel reader plugin.
        
        Args:
            plugin: The reader plugin to register
        """
        self._readers[plugin.metadata.name] = plugin
        logger.debug(f"Registered reader plugin: {plugin.metadata.name}")
    
    def register_writer(self, plugin: DatabaseWriterPlugin) -> None:
        """Register a database writer plugin.
        
        Args:
            plugin: The writer plugin to register
        """
        self._writers[plugin.metadata.name] = plugin
        logger.debug(f"Registered writer plugin: {plugin.metadata.name}")
    
    def get_reader(self, file_path: str) -> Optional[ExcelReaderPlugin]:
        """Get the appropriate reader for a file.
        
        Args:
            file_path: Path to the Excel file
            
        Returns:
            The appropriate reader plugin or None if no suitable reader found
        """
        for reader in self._readers.values():
            if reader.can_handle(file_path):
                return reader
        return None
    
    def get_writer(self, connection_string: str) -> Optional[DatabaseWriterPlugin]:
        """Get the appropriate writer for a connection string.
        
        Args:
            connection_string: Database connection string
            
        Returns:
            The appropriate writer plugin or None if no suitable writer found
        """
        for writer in self._writers.values():
            if writer.can_handle(connection_string):
                return writer
        return None
    
    def list_readers(self) -> List[ExcelReaderPlugin]:
        """List all registered readers.
        
        Returns:
            List of all registered reader plugins
        """
        return list(self._readers.values())
    
    def list_writers(self) -> List[DatabaseWriterPlugin]:
        """List all registered writers.
        
        Returns:
            List of all registered writer plugins
        """
        return list(self._writers.values())
    
    def get_reader_by_name(self, name: str) -> Optional[ExcelReaderPlugin]:
        """Get a reader plugin by name.
        
        Args:
            name: Name of the reader plugin
            
        Returns:
            The reader plugin or None if not found
        """
        return self._readers.get(name)
    
    def get_writer_by_name(self, name: str) -> Optional[DatabaseWriterPlugin]:
        """Get a writer plugin by name.
        
        Args:
            name: Name of the writer plugin
            
        Returns:
            The writer plugin or None if not found
        """
        return self._writers.get(name)
    
    def unregister_reader(self, name: str) -> bool:
        """Unregister a reader plugin.
        
        Args:
            name: Name of the reader plugin to unregister
            
        Returns:
            True if plugin was unregistered, False if not found
        """
        if name in self._readers:
            del self._readers[name]
            logger.info(f"Unregistered reader plugin: {name}")
            return True
        return False
    
    def unregister_writer(self, name: str) -> bool:
        """Unregister a writer plugin.
        
        Args:
            name: Name of the writer plugin to unregister
            
        Returns:
            True if plugin was unregistered, False if not found
        """
        if name in self._writers:
            del self._writers[name]
            logger.info(f"Unregistered writer plugin: {name}")
            return True
        return False 