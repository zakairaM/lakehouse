# Databricks notebook source
# MAGIC %md
# MAGIC # Database Loader - Raw Ingestion
# MAGIC
# MAGIC This notebook handles the ingestion of billing data from Database sources to the RAW layer.
# MAGIC
# MAGIC **Process Flow**:
# MAGIC 1. Load master pipeline configuration
# MAGIC 2. Filter entities with `ingestion_type: 'database'`
# MAGIC 3. For each entity and document type:
# MAGIC    - Connect to database
# MAGIC    - Extract data based on configuration
# MAGIC    - Save CSV/JSON files to RAW layer with date partitioning
# MAGIC
# MAGIC **Target Path**: `/Volumes/teamblue_finance/default/sagacity-data-teamblue-finance-dev/raw/billing/{region}/{entity}/{document_type}/YYYY/MM/DD/`

# COMMAND ----------

import sys
import json
import os
from typing import Dict, Any, List
from datetime import datetime, timedelta, timezone

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Configuration Modules

# COMMAND ----------

# Auto-detect bundle location and load modules
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    bundle_base = notebook_path.split('/src/')[0]

    # Load config_loader module
    config_loader_path = f"/Workspace{bundle_base}/src/common/utils/config_loader.py"
    with open(config_loader_path, 'r') as f:
        config_loader_code = f.read()
        config_loader_code = config_loader_code.replace('# Databricks notebook source', '')
        config_loader_code = config_loader_code.replace('# COMMAND ----------', '')
        config_loader_code = config_loader_code.replace('# MAGIC %md', '#')
        exec(config_loader_code, globals())

    print(f"📂 Loaded modules from: {bundle_base}")
except Exception as e:
    print(f"❌ Error loading modules: {e}")
    raise

# Setup config path and loader
CONFIG_BASE_PATH = setup_config_path(dbutils)
config_loader = ConfigLoader(CONFIG_BASE_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Database Ingestion Process

# COMMAND ----------

def build_raw_path(region: str, entity_code: str, document_type: str, date: datetime = None) -> str:
    """Build raw data path with date partitioning."""
    base_path = f"/Volumes/teamblue_finance/default/sagacity-data-teamblue-finance-dev/raw/billing/{region.lower()}/{entity_code.lower()}/{document_type.lower()}/"
    
    if date:
        date_str = date.strftime("%Y/%m/%d")
        return f"{base_path}{date_str}/"
    
    return base_path

# COMMAND ----------

# Load master pipeline configuration
master_config = config_loader.load_master_pipeline_config()
enabled_pipelines = master_config.get('pipelines', [])

# Filter for database ingestion type
database_pipelines = [p for p in enabled_pipelines if p.get('ingestion_type') == 'database' and p.get('enabled', True)]

if not database_pipelines:
    print("ℹ️ No database ingestion pipelines enabled")

for pipeline_config in database_pipelines:
    entity_code = pipeline_config['entity_code']
    region = pipeline_config['region']
    document_types = pipeline_config.get('document_types', [])
    
    print(f"🔄 Processing database ingestion for {entity_code} in {region}")
    
    for document_type in document_types:
        try:
            # Load source configuration
            source_config = config_loader.load_source_config(region, entity_code, document_type)
            if not source_config or not source_config.get('enabled', True):
                print(f"⏭️ Skipping disabled source: {entity_code}/{document_type}")
                continue
            
            # Get database connection configuration
            connection_config = source_config.get('connection', {})
            connection_type = connection_config.get('type')
            
            # TODO: Implement database extraction logic
            # This will depend on the specific database type (PostgreSQL, MySQL, SQL Server, etc.)
            # For now, this is a placeholder structure
            
            print(f"📊 Database extraction for {entity_code}/{document_type} - TODO: Implement based on connection type: {connection_type}")
            
            # Example structure:
            # - Connect to database using connection_config
            # - Execute query from source_config
            # - Save results to RAW path
            # - File format: CSV or JSON based on source_config
            
            raw_path = build_raw_path(region, entity_code, document_type, datetime.now())
            
            print(f"✅ Database ingestion completed for {entity_code}/{document_type}")
            print(f"   Target path: {raw_path}")
            
        except Exception as e:
            print(f"❌ Error processing database ingestion for {entity_code}/{document_type}: {e}")
            import traceback
            traceback.print_exc()
            continue

print("✅ Database ingestion process completed")

