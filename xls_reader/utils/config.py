"""Configuration management for the XLSX Reader Microkernel."""

import os
import yaml
import json
from pathlib import Path
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, ValidationError
import logging

from ..core.exceptions import ConfigurationError

logger = logging.getLogger(__name__)


class ConfigurationManager:
    """Manages configuration loading, validation, and access."""
    
    def __init__(self, config_dir: Optional[str] = None):
        """Initialize the configuration manager.
        
        Args:
            config_dir: Directory containing configuration files
        """
        self.config_dir = config_dir or "config"
        self._config_cache: Dict[str, Any] = {}
        self._schemas: Dict[str, Dict[str, Any]] = {}
    
    def load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from file.
        
        Args:
            config_path: Path to configuration file
            
        Returns:
            Configuration dictionary
            
        Raises:
            ConfigurationError: If configuration file is invalid
        """
        if config_path in self._config_cache:
            return self._config_cache[config_path]
        
        try:
            path = Path(config_path)
            if not path.exists():
                raise ConfigurationError(config_path, [f"Configuration file not found: {config_path}"])
            
            if path.suffix.lower() in ['.yaml', '.yml']:
                with open(path, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
            elif path.suffix.lower() == '.json':
                with open(path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
            else:
                raise ConfigurationError(
                    config_path, 
                    [f"Unsupported configuration file format: {path.suffix}"]
                )
            
            # Validate configuration
            validation_errors = self._validate_config(config)
            if validation_errors:
                raise ConfigurationError(config_path, validation_errors)
            
            self._config_cache[config_path] = config
            logger.info(f"Loaded configuration from {config_path}")
            return config
            
        except Exception as e:
            if isinstance(e, ConfigurationError):
                raise
            raise ConfigurationError(config_path, [f"Failed to load configuration: {str(e)}"])
    
    def validate_config(self, config: Dict[str, Any], schema: Dict[str, Any]) -> List[str]:
        """Validate configuration against schema.
        
        Args:
            config: Configuration dictionary
            schema: Schema dictionary
            
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        # Basic structure validation
        if not isinstance(config, dict):
            errors.append("Configuration must be a dictionary")
            return errors
        
        # Required fields validation
        required_fields = schema.get("required", [])
        for field in required_fields:
            if field not in config:
                errors.append(f"Required field '{field}' is missing")
        
        # Type validation
        for field_name, field_config in schema.get("properties", {}).items():
            if field_name in config:
                field_value = config[field_name]
                expected_type = field_config.get("type")
                
                if expected_type == "string" and not isinstance(field_value, str):
                    errors.append(f"Field '{field_name}' must be a string")
                elif expected_type == "integer" and not isinstance(field_value, int):
                    errors.append(f"Field '{field_name}' must be an integer")
                elif expected_type == "boolean" and not isinstance(field_value, bool):
                    errors.append(f"Field '{field_name}' must be a boolean")
                elif expected_type == "array" and not isinstance(field_value, list):
                    errors.append(f"Field '{field_name}' must be an array")
                elif expected_type == "object" and not isinstance(field_value, dict):
                    errors.append(f"Field '{field_name}' must be an object")
        
        # Enum validation
        for field_name, field_config in schema.get("properties", {}).items():
            if field_name in config and "enum" in field_config:
                field_value = config[field_name]
                if field_value not in field_config["enum"]:
                    errors.append(
                        f"Field '{field_name}' must be one of {field_config['enum']}"
                    )
        
        return errors
    
    def _validate_config(self, config: Dict[str, Any]) -> List[str]:
        """Validate configuration against default schema.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            List of validation error messages (empty if valid)
        """
        default_schema = {
            "type": "object",
            "properties": {
                "plugin_paths": {"type": "array"},
                "reader_config": {"type": "object"},
                "writer_config": {"type": "object"},
                "logging": {"type": "object"},
                "output": {"type": "object"}
            }
        }
        
        return self.validate_config(config, default_schema)
    
    def get_plugin_config(self, plugin_name: str) -> Dict[str, Any]:
        """Get configuration for a specific plugin.
        
        Args:
            plugin_name: Name of the plugin
            
        Returns:
            Plugin configuration dictionary
        """
        # Try to load plugin-specific config file
        plugin_config_path = os.path.join(self.config_dir, f"{plugin_name}.yaml")
        if os.path.exists(plugin_config_path):
            return self.load_config(plugin_config_path)
        
        # Fall back to global config
        global_config = self._config_cache.get("global", {})
        return global_config.get("plugins", {}).get(plugin_name, {})
    
    def save_config(self, config: Dict[str, Any], config_path: str) -> None:
        """Save configuration to file.
        
        Args:
            config: Configuration dictionary
            config_path: Path to save configuration file
        """
        try:
            path = Path(config_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            
            if path.suffix.lower() in ['.yaml', '.yml']:
                with open(path, 'w', encoding='utf-8') as f:
                    yaml.dump(config, f, default_flow_style=False, indent=2)
            elif path.suffix.lower() == '.json':
                with open(path, 'w', encoding='utf-8') as f:
                    json.dump(config, f, indent=2)
            else:
                raise ValueError(f"Unsupported configuration file format: {path.suffix}")
            
            logger.info(f"Saved configuration to {config_path}")
            
        except Exception as e:
            logger.error(f"Failed to save configuration: {e}")
            raise
    
    def get_env_config(self) -> Dict[str, Any]:
        """Get configuration from environment variables.
        
        Returns:
            Configuration dictionary from environment variables
        """
        config = {}
        
        # Common environment variables
        env_mappings = {
            "XLS_READER_LOG_LEVEL": "logging.level",
            "XLS_READER_OUTPUT_DIR": "output.directory",
            "XLS_READER_PLUGIN_PATHS": "plugin_paths",
            "XLS_READER_DB_CONNECTION": "database.connection_string",
        }
        
        for env_var, config_path in env_mappings.items():
            value = os.getenv(env_var)
            if value:
                # Handle special cases
                if env_var == "XLS_READER_PLUGIN_PATHS":
                    config["plugin_paths"] = value.split(",")
                else:
                    # Set nested config value
                    keys = config_path.split(".")
                    current = config
                    for key in keys[:-1]:
                        if key not in current:
                            current[key] = {}
                        current = current[key]
                    current[keys[-1]] = value
        
        return config
    
    def merge_configs(self, *configs: Dict[str, Any]) -> Dict[str, Any]:
        """Merge multiple configuration dictionaries.
        
        Args:
            *configs: Configuration dictionaries to merge
            
        Returns:
            Merged configuration dictionary
        """
        if not configs:
            return {}
        
        result = configs[0].copy()
        
        for config in configs[1:]:
            self._deep_merge(result, config)
        
        return result
    
    def _deep_merge(self, target: Dict[str, Any], source: Dict[str, Any]) -> None:
        """Deep merge source into target.
        
        Args:
            target: Target dictionary
            source: Source dictionary
        """
        for key, value in source.items():
            if key in target and isinstance(target[key], dict) and isinstance(value, dict):
                self._deep_merge(target[key], value)
            else:
                target[key] = value 