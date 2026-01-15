# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Standard Loader
# MAGIC
# MAGIC This notebook converts raw billing files to Delta tables with schema enforcement using contract definitions.
# MAGIC It applies data type casting, validation, and adds technical metadata columns.
# MAGIC

# COMMAND ----------

import yaml
import json
import sys
import os
import dlt
from typing import List, Dict, Any
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType, DateType, LongType

from datetime import datetime, timezone
import hashlib

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

    # Add the transformation module to the path
    transformations_path = f"/Workspace{bundle_base}/resources/domains/finance/billing/core/transformations/"
    sys.path.append(transformations_path)

    print(f"📂 Loaded modules from: {bundle_base}")
except Exception as e:
    print(f"❌ Error loading modules: {e}")
    raise

# Setup config path and loader
CONFIG_BASE_PATH = setup_config_path(dbutils)
config_loader = ConfigLoader(CONFIG_BASE_PATH)

# Load transformation engine
from generic_transformation import ContractTransformationEngine

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Processing Functions
# MAGIC

# COMMAND ----------

def build_raw_path(region: str, entity_code: str, document_type: str) -> str:
    """Build raw data path for entity and document type."""
    # Use base path without date partitioning to avoid reprocessing same files
    raw_path = f"/Volumes/teamblue_finance/default/sagacity-data-teamblue-finance-dev/raw/billing/{region.lower()}/{entity_code.lower()}/{document_type}/"

    return raw_path

def build_bronze_table_name(region: str, entity_code: str, document_type: str) -> str:
    """Build bronze table name."""
    return f"teamblue_finance.bronze.billing_{region.lower()}_{entity_code.lower()}_{document_type}"

def create_record_hash(row) -> str:
    """Create hash for deduplication."""
    # Create a string from all non-technical columns
    row_str = str([row[col] for col in row.asDict().keys() if not col.startswith('_')])
    return hashlib.md5(row_str.encode()).hexdigest()

# COMMAND ----------

def bronze_ingestion(entity_code, region, document_type, source_config, raw_path, schema_location, file_format):
    """
    Process raw data into bronze layer with schema enforcement, technical columns, and hash for deduplication.
    """
    document_config = source_config.get('document', {})
    df = None
    
    if file_format in ['csv', 'txt']:
        csv_options = document_config.get('csv_options', {})
        df = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.includeExistingFiles", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("header", "true")
            .option("delimiter", csv_options.get('delimiter', ','))
            .option("cloudFiles.schemaLocation", schema_location)
            .option("cloudFiles.rescuedDataColumn", "_rescued_data")
            .load(raw_path)
        )
    elif file_format == 'json':
        select_rows = document_config.get('json_column_extract')
        df = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.includeExistingFiles", "true")
            .option("cloudFiles.schemaLocation", schema_location)
            .option("cloudFiles.rescuedDataColumn", "_rescued_data")
            .option("multiline", "true")
            .load(raw_path)
        )
        if select_rows:
            df = df.filter(F.col(select_rows).isNotNull() & (F.size(F.col(select_rows)) > 0))
            df = df.select(F.explode(F.col(select_rows)).alias("row"), F.col("_metadata"))
            if isinstance(df.schema["row"].dataType, StructType):
                df = df.select("row.*", "_metadata.*")
    else:
        raise ValueError(f"Unsupported file format: {file_format}")

    # Add technical columns
    df_tech = (
        df
        .withColumn("_bronze_ingestion_time", F.current_timestamp())
        .withColumn("_entity_code", F.lit(entity_code))
        .withColumn("_region", F.lit(region))
        .withColumn("_document_type", F.lit(document_type))
        .withColumn("_bronze_processing_date", F.current_date())
        .withColumn("_source_file_path", F.lit(raw_path + document_type))
    )

    # Add record hash
    df_hash = df_tech.withColumn(
        "_raw_record_hash",
        F.sha2(
            F.concat_ws(
                "|",
                *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in df.columns if not c.startswith("_")]
            ),
            256
        )
    )

    return df_hash

def create_bronze_dlt_table(entity_code, region, document_type, source_config, raw_path, schema_location, file_format, expectations):
    table_name = f"billing_{region}_{entity_code.replace('-', '_')}_{document_type}".lower()

    @dlt.table(
        name=table_name,
        comment=f"Bronze table for {entity_code}/{document_type} in {region}",
        table_properties={"quality": "bronze"},
        partition_cols=["_bronze_processing_date"]
    )
    @dlt.expect_all(expectations)
    @dlt.expect_or_drop("null_records", "_raw_record_hash IS NOT NULL")
    def table_func():
        return bronze_ingestion(entity_code, region, document_type, source_config, raw_path, schema_location, file_format)
    
    return table_func

# --------------------------
# Main Bronze Loader
# --------------------------

def main():
    print("🚀 Starting Bronze Standard Loader Process")

    # Load master pipeline configuration using config_loader
    master_config = config_loader.load_master_pipeline_config()
    enabled_entities = master_config.get('pipelines', [])

    checkpoint_path_conf_base = "/Volumes/teamblue_finance/default/sagacity-data-teamblue-finance-dev/_checkpoints/bronze/"

    for pipeline in enabled_entities:
        if not pipeline.get('enabled', True):
            print(f"⏭️ Skipping disabled pipeline: {pipeline.get('entity_code', 'N/A')}")
            continue

        entity_code = pipeline['entity_code']
        region = pipeline['region']
        document_types = pipeline.get('document_types', ['invoice'])

        for document_type in document_types:

            try:
                # Load source configuration using config_loader
                source_config = config_loader.load_source_config(region, entity_code, document_type)
                if not source_config or not source_config.get('enabled', True):
                    print(f"⏭️ Skipping disabled source: {entity_code}/{document_type}")
                    continue

                raw_path = build_raw_path(region, entity_code, document_type)
                checkpoint_path_conf = source_config.get('paths', {}).get(
                    'checkpoint_bronze',
                    f'{checkpoint_path_conf_base}{region}/{entity_code}/{document_type}'
                )
                schema_location = f"{checkpoint_path_conf}/schema/"
                file_format = source_config.get('document', {}).get('format', 'csv')
                
                expectations = source_config.get('expectations', {})  
                
                # Create the DLT table
                create_bronze_dlt_table(
                entity_code,
                region,
                document_type,
                source_config,
                raw_path,
                schema_location,
                file_format,
                expectations
                )

                print(f"✅ Defined DLT table for {entity_code}/{document_type}")

            except Exception as e:
                print(f"❌ Error defining bronze table for {entity_code}/{document_type}: {e}")
                continue

    print("🏁 Bronze Standard Loader Process Complete")


# Run main
main()
