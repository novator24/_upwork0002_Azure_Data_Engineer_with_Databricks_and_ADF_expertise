"""
Configuration management for the data engineering pipeline.
"""

import os
import json
from typing import Any, Dict, Optional


class Config:
    """Configuration manager for pipeline settings."""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration.
        
        Args:
            config_path: Path to configuration file
        """
        self.config = {}
        if config_path and os.path.exists(config_path):
            with open(config_path, 'r') as f:
                self.config = json.load(f)
        
        # Set default values
        self._set_defaults()
    
    def _set_defaults(self):
        """Set default configuration values."""
        defaults = {
            "bronze_layer_path": "s3://data-lake/bronze",
            "silver_layer_path": "s3://data-lake/silver", 
            "gold_layer_path": "s3://data-lake/gold",
            "spark_config": {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.sql.adaptive.skewJoin.enabled": "true"
            },
            "data_quality": {
                "null_threshold": 0.1,
                "duplicate_threshold": 0.05
            }
        }
        
        for key, value in defaults.items():
            if key not in self.config:
                self.config[key] = value
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        return self.config.get(key, default)
    
    def set(self, key: str, value: Any):
        """Set configuration value."""
        self.config[key] = value 