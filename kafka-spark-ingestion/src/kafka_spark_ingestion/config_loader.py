import yaml
import os
from typing import Dict, Any

class ConfigLoader:
    """
    Loads and validates the pipeline configuration from a YAML file.
    """

    def __init__(self, config_path: str):
        """
        Initialize the ConfigLoader.

        Args:
            config_path (str): Path to the YAML configuration file.
        """
        self.config_path = config_path
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """
        Reads and parses the YAML configuration file.

        Returns:
            Dict[str, Any]: The configuration dictionary.

        Raises:
            FileNotFoundError: If the config file does not exist.
            yaml.YAMLError: If the config file is not valid YAML.
        """
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Configuration file not found at: {self.config_path}")

        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            return config
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML configuration: {e}")

    def validate(self) -> bool:
        """
        Validates the configuration against the required schema.

        Returns:
            bool: True if valid, raises ValueError otherwise.
        
        Raises:
            ValueError: If required configuration keys are missing.
        """
        required_keys = {
            "kafka": ["bootstrap_servers", "topic", "consumer_group"],
            "schema_registry": ["url"],
            "spark": ["app_name"],
            "output": ["format", "mode", "checkpoint_location"]
        }

        for section, keys in required_keys.items():
            if section not in self.config:
                raise ValueError(f"Missing required configuration section: '{section}'")
            
            for key in keys:
                if key not in self.config[section]:
                    raise ValueError(f"Missing required key '{key}' in section '{section}'")
        
        return True

    def get(self, section: str, key: str = None, default: Any = None) -> Any:
        """
        Retrieve a configuration value.

        Args:
            section (str): The configuration section (e.g., 'kafka').
            key (str, optional): The key within the section. If None, returns the whole section.
            default (Any, optional): Default value if key is not found.

        Returns:
            Any: The configuration value.
        """
        if section not in self.config:
            return default
        
        if key is None:
            return self.config[section]
        
        return self.config[section].get(key, default)
