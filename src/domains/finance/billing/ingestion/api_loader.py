# Databricks notebook source
# MAGIC %md
# MAGIC # API Loader - Raw Ingestion
# MAGIC
# MAGIC This notebook handles the ingestion of billing data from API sources to the RAW layer.
# MAGIC It uses a config-driven approach with the standard ConfigLoader.
# MAGIC
# MAGIC **Process Flow**:
# MAGIC 1. Load master pipeline configuration
# MAGIC 2. Filter entities with `ingestion_type: 'api'`
# MAGIC 3. For each entity and document type:
# MAGIC    - Authenticate with API
# MAGIC    - Fetch data based on configuration
# MAGIC    - Save JSON files to RAW layer with date partitioning
# MAGIC
# MAGIC **Target Path**: `/Volumes/teamblue_finance/default/sagacity-data-teamblue-finance-dev/raw/billing/{region}/{entity}/{document_type}/YYYY/MM/DD/`

# COMMAND ----------

import sys
import json
import os
import importlib.util
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

    # Load util_api module
    util_api_path = f"/Workspace{bundle_base}/src/common/utils/util_api.py"
    with open(util_api_path, 'r') as f:
        util_api_code = f.read()
        util_api_code = util_api_code.replace('# Databricks notebook source', '')
        util_api_code = util_api_code.replace('# COMMAND ----------', '')
        util_api_code = util_api_code.replace('# MAGIC %md', '#')
        exec(util_api_code, globals())

    print(f"📂 Loaded modules from: {bundle_base}")
except Exception as e:
    print(f"❌ Error loading modules: {e}")
    raise

# Setup config path and loader
CONFIG_BASE_PATH = setup_config_path(dbutils)
config_loader = ConfigLoader(CONFIG_BASE_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def build_destination_path(connection_config: Dict[str, Any], region: str, entity_code: str, document_type: str) -> str:
    """
    Build destination path with date partitioning.
    
    Args:
        connection_config: Connection configuration dictionary
        region: Region code (e.g., 'SEU')
        entity_code: Entity code (e.g., '32H')
        document_type: Document type (e.g., 'invoices')
        
    Returns:
        Full destination path with date partitioning
    """
    destination = connection_config.get('destination', {})
    base_path = destination.get('base_path', 'raw/billing')
    
    # Get current date for partitioning
    current_date = datetime.now(timezone.utc)
    year = current_date.strftime('%Y')
    month = current_date.strftime('%m')
    day = current_date.strftime('%d')
    
    # Construct full destination path: base_path/{region}/{entity}/{document_type}/YYYY/MM/DD/
    dest_path = f"/Volumes/{destination['catalog']}/{destination['schema']}/{destination['volume']}/{base_path}/{region.lower()}/{entity_code.lower()}/{document_type}/{year}/{month}/{day}/"
    
    return dest_path

def get_query_datetime(param_type: str, days_back: int = 1) -> datetime:
    """
    Get datetime for API query parameters.
    
    Args:
        param_type: Type of parameter ('single' or 'multiple')
        days_back: Number of days to look back (default: 1 for single, 2 for multiple)
        
    Returns:
        Datetime object
    """
    if param_type == 'single':
        query_dt=datetime.now() - timedelta(days=days_back)
    else:  # multiple params
       query_dt=datetime.now() - timedelta(days=days_back + 1)
    
    return query_dt.isoformat()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main API Ingestion Function

# COMMAND ----------

def process_api_ingestion(
    region: str,
    entity_code: str,
    document_type: str,
    source_config: Dict[str, Any],
    connection_config: Dict[str, Any]
) -> None:
    """
    Ingest data from API and save as JSON to RAW layer.
    
    Args:
        region: Region code
        entity_code: Entity code
        document_type: Document type
        source_config: Source configuration
        connection_config: Connection configuration
    """
    print(f"🔄 Processing API ingestion: {entity_code}/{document_type} in {region}")
    
    try:
        # Get API configuration for this entity
        source = connection_config.get('source', {})
        entity_config = source.get(entity_code, {})
        
        if not entity_config:
            print(f"❌ No API configuration found for entity {entity_code}")
            return
        
        base_url = entity_config.get('base_url', '')
        token = entity_config.get('token', '')
        auth_mode = entity_config.get('auth_mode', '')
        document_types_config = entity_config.get('document_types', {})
        endpoint = entity_config.get('endpoint', '')
        
        
        print(f"🔍 Debug - Base URL: {base_url}")
        print(f"🔍 Debug - Document types: {list(document_types_config.keys())}")
        
        # Get configuration for this specific document type
        doc_config = document_types_config.get(document_type)
        if not doc_config:
            print(f"❌ No configuration found for document type {document_type}")
            return
        
        entity_name = doc_config.get('entity')
        category = doc_config.get('category')
        params = doc_config.get('params','')
        
        # Initialize API client
        print(f"🔗 Connecting to API: {base_url}")
        client = APIClient(base_url)
        
        # Debug login parameters
        print(f"🔍 Debug - Login parameters:")
        # Authenticate
        try:
            if token == '':
                login_params = entity_config.get('login_params', {})
                payload = login_params.get('payload', {})
                login_url = login_params.get('login_url', '')
                print(f"🔍 Debug - payload: {list(payload.keys())}")
                print(f"🔍 Debug - login_url: {login_url}")
                token = client.get_token(payload,login_url)

        except Exception as auth_error:
            print(f"❌ Authentication error details: {auth_error}")
            print(f"🔍 Full login params: {login_params}")
            raise
        
        # Build query parameters
        if isinstance(params, str) and params != '':
            # Single parameter
            param_dict = {params: get_query_datetime('single', days_back=1)}
        elif isinstance(params, list):
            # Multiple parameters
            param_dict = {param: get_query_datetime('multiple', days_back=2) for param in params}
        else:
            param_dict = {}
        
        # Build record configuration
        record_config = {
            'entity': entity_name,
            'params': param_dict,
            'token': token,
            'category': category
        }
        
        print(f"📋 Fetching data for: Entity={entity_name}, Category={category}")

        # Get data from API
        record_data = client.get_data(auth_mode, token, endpoint, record_config)
        
        # Close API client
        client.close()
        
        # Build destination path
        dest_path = build_destination_path(connection_config, region, entity_code, document_type)
        
        print(f"💾 Saving to: {dest_path}")
        
        # Create directory if it doesn't exist
        try:
            dbutils.fs.mkdirs(dest_path)
        except Exception as e:
            print(f"⚠️ Warning creating directory: {e}")
        
        # Generate filename with timestamp
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        json_file_path = f"{dest_path}{document_type}_{timestamp}.json"

        record_data["_metadata"] = {
                "_raw_file_name": f"{document_type}_{timestamp}.json",
                "_raw_ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
                "_raw_ingestion_date": datetime.now(timezone.utc).date().isoformat()
                }
        
        # Convert to JSON string
        json_content = json.dumps(record_data, indent=2)
        
        # Write JSON file to DBFS
        dbutils.fs.put(json_file_path, json_content, overwrite=True)
        
        # Verify file was written
        row_count = len(record_data.get('Rows', []))
        print(f"✅ Successfully saved {row_count} records to {json_file_path}")

    except Exception as e:
        print(f"❌ Error processing API ingestion for {entity_code}/{document_type}: {e}")
        import traceback
        traceback.print_exc()
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Execution

# COMMAND ----------

def main() -> None:
    """
    Main function to process API ingestion based on configuration.
    Filters for entities with ingestion_type='api'.
    """
    print("🚀 Starting API Raw Ingestion Process.")
    print(f"⏰ Execution time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    
    # Load master pipeline configuration
    master_config = config_loader.load_master_pipeline_config()
    if not master_config or 'pipelines' not in master_config:
        print("❌ No pipeline configuration found")
        return
    
    # Filter for API ingestion only
    api_pipelines = [
        p for p in master_config.get('pipelines', [])
        if p.get('ingestion_type', '').lower() == 'api' and p.get('enabled', True)
    ]
    
    print(f"📋 Found {len(api_pipelines)} API pipelines enabled")
    
    if not api_pipelines:
        print("⚠️ No enabled API pipelines found")
        return
    
    processed_count = 0
    error_count = 0
    
    for pipeline in api_pipelines:
        entity_code = pipeline.get('entity_code', '')
        region = pipeline.get('region', '')
        document_types = pipeline.get('document_types', [])
        
        print(f"\n{'='*60}")
        print(f"📁 Processing Entity: {entity_code} | Region: {region}")
        print(f"📄 Document Types: {', '.join(document_types)}")
        print(f"{'='*60}")
        
        for document_type in document_types:
            try:
                # Load source configuration
                source_config = config_loader.load_source_config(region, entity_code, document_type)
                if not source_config or not source_config.get('enabled', True):
                    print(f"⏭️ Skipping disabled source: {entity_code}/{document_type}")
                    continue
                
                # Get connection reference
                connection_ref = source_config.get('connection', {}).get('connection_ref')
                if not connection_ref:
                    print(f"❌ No connection reference found for {entity_code}/{document_type}")
                    error_count += 1
                    continue
                
                # Load connection configuration
                connection_config = config_loader.load_connection_config(connection_ref)
                if not connection_config:
                    print(f"❌ Connection configuration not found: {connection_ref}")
                    error_count += 1
                    continue
                
                # Process API ingestion
                process_api_ingestion(region, entity_code, document_type, source_config, connection_config)
                processed_count += 1
                
            except Exception as e:
                print(f"❌ Error processing {entity_code}/{document_type}: {e}")
                error_count += 1
                continue
    
    print(f"\n{'='*60}")
    print(f"📊 API Ingestion Summary")
    print(f"{'='*60}")
    print(f"✅ Successfully processed: {processed_count}")
    print(f"❌ Errors: {error_count}")
    print(f"⏰ Completed at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("🏁 API Raw Ingestion Process Complete")

# COMMAND ----------

# Execute main function
if __name__ == "__main__":
    main()
else:
    # When imported as notebook
    main()
