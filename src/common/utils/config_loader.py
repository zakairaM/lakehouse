# Databricks notebook source
# MAGIC %md
# MAGIC # Configuration Loader Utilities
# MAGIC
# MAGIC Provides common functions to load YAML configurations with auto-detection
# MAGIC of Databricks Asset Bundle deployment paths.
# MAGIC
# MAGIC **Usage in notebooks:**
# MAGIC ```python
# MAGIC %run /Workspace/.../src/common/utils/config_loader
# MAGIC CONFIG_BASE_PATH = setup_config_path(dbutils)
# MAGIC config_loader = ConfigLoader(CONFIG_BASE_PATH)
# MAGIC ```

# COMMAND ----------

import yaml
from typing import Dict, Any

# COMMAND ----------

def setup_config_path(dbutils) -> str:
    """
    Auto-detect bundle deployment location and return config base path.
    
    Args:
        dbutils: Databricks utilities object
        
    Returns:
        str: Base path for configuration files
        
    Example:
        CONFIG_BASE_PATH = setup_config_path(dbutils)
        # Returns: /Workspace/Users/{email}/.bundle/teamblue_lakehouse/dev/files/resources/domains/finance/billing/core
    """
    try:
        notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        bundle_base = notebook_path.split('/src/')[0]
        config_path = f"/Workspace{bundle_base}/resources/domains/finance/billing/core"
        print(f"📂 Auto-detected config path: {config_path}")
        return config_path
    except Exception as e:
        fallback_path = "/Workspace/resources/domains/finance/billing/core"
        print(f"⚠️ Using fallback config path: {fallback_path}")
        print(f"⚠️ Error: {e}")
        return fallback_path

# COMMAND ----------

def setup_transformations_path(dbutils) -> str:
    """
    Auto-detect bundle deployment location and return transformations module path.
    
    Args:
        dbutils: Databricks utilities object
        
    Returns:
        str: Path to transformations module
    """
    try:
        notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        bundle_base = notebook_path.split('/src/')[0]
        trans_path = f"/Workspace{bundle_base}/resources/domains/finance/billing/core/transformations"
        print(f"📂 Auto-detected transformations path: {trans_path}")
        return trans_path
    except Exception as e:
        fallback_path = "/Workspace/resources/domains/finance/billing/core/transformations"
        print(f"⚠️ Using fallback transformations path: {fallback_path}")
        return fallback_path

# COMMAND ----------

def load_yaml_config(config_path: str) -> Dict[str, Any]:
    """
    Load YAML configuration file.
    
    Args:
        config_path: Path to YAML file
        
    Returns:
        Dict containing configuration, or empty dict if error
    """
    try:
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)
    except Exception as e:
        print(f"Error loading config {config_path}: {e}")
        return {}

# COMMAND ----------

class ConfigLoader:
    """Config loader with base path."""
    
    def __init__(self, config_base_path: str):
        self.config_base_path = config_base_path
    
    def load_master_pipeline_config(self) -> Dict[str, Any]:
        """Load the master pipeline configuration."""
        config_path = f"{self.config_base_path}/config/pipelines/billing_master.yaml"
        return load_yaml_config(config_path)
    
    def load_connection_config(self, connection_ref: str) -> Dict[str, Any]:
        """Load connection configuration by reference."""
        if connection_ref.startswith('sftp_'):
            config_path = f"{self.config_base_path}/config/connections/SFTP/{connection_ref}.yaml"
        elif connection_ref.startswith('api_'):
            config_path = f"{self.config_base_path}/config/connections/API/{connection_ref}.yaml"
        else:
            raise ValueError(f"Unknown connection type: {connection_ref}")
        return load_yaml_config(config_path)
    
    def load_source_config(self, region: str, entity_code: str, document_type: str) -> Dict[str, Any]:
        """Load source configuration for specific entity and document type."""
        config_path = f"{self.config_base_path}/config/sources/{region.lower()}/{entity_code.lower()}/{document_type.lower()}.yaml"
        return load_yaml_config(config_path)
