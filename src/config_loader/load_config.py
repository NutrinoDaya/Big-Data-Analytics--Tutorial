import yaml
from typing import Dict, Any
import os

def get_config(config_path: str) -> Dict[str, Any]:
    """
    Loads and parses a YAML configuration file.

    Args:
        config_path (str): The path to the YAML configuration file.

    Returns:
        Dict[str, Any]: A dictionary containing the configuration.
    
    Raises:
        FileNotFoundError: If the config file is not found.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found at: {config_path}")
        
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

# Example of loading all configs
def load_all_configs() -> Dict[str, Any]:
    """
    Loads all configuration files from the config directory.

    Returns:
        Dict[str, Any]: A dictionary containing all configurations.
    """
    config_dir = "config"
    spark_config = get_config(os.path.join(config_dir, "spark_config.yaml"))
    paths_config = get_config(os.path.join(config_dir, "paths.yaml"))
    api_config = get_config(os.path.join(config_dir, "api_config.yaml"))
    
    return {
        "spark": spark_config,
        "paths": paths_config,
        "api": api_config
    }

if __name__ == '__main__':
    # For testing the loader
    all_configs = load_all_configs()
    print("--- Spark Config ---")
    print(all_configs['spark'])
    print("\n--- Paths Config ---")
    print(all_configs['paths'])
    print("\n--- API Config ---")
    print(all_configs['api'])

