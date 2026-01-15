# Databricks notebook source

# MAGIC %md
# MAGIC # Seed Entities Reference Table
# MAGIC
# MAGIC This notebook loads the entities.csv file into a Delta table for reference across the billing pipeline.
# MAGIC
# MAGIC **Strategy**: Uses MERGE (UPSERT) to update existing entities and insert new ones.
# MAGIC
# MAGIC **Source**: Files are read from Workspace (deployed via bundle from GitLab)

# COMMAND ----------

import importlib.util
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from delta.tables import DeltaTable

# Auto-detect bundle location and load config_loader
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    bundle_base = notebook_path.split('/src/')[0]
    config_loader_path = f"{bundle_base}/src/common/utils/config_loader.py"
    
    spec = importlib.util.spec_from_file_location("config_loader", config_loader_path)
    config_loader_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(config_loader_module)
    
    setup_config_path = config_loader_module.setup_config_path
except Exception as e:
    print(f"❌ Error loading config_loader: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Auto-detect config path using common utility
CONFIG_BASE_PATH = setup_config_path(dbutils)
ENTITIES_CSV_PATH = f"{CONFIG_BASE_PATH}/config/entities.csv"

# Target: Delta table
TARGET_CATALOG = "teamblue_finance"
TARGET_SCHEMA = "config"
TARGET_TABLE = "billing_entities"
TARGET_FULL_NAME = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}"

# Storage: Managed table location in Volume
TABLE_LOCATION = "s3://sagacity-data-teamblue-finance/dev/config/billing_entities/entities.csv"

print(f"📄 Source CSV: {ENTITIES_CSV_PATH}")
print(f"📊 Target Table: {TARGET_FULL_NAME}")
print(f"💾 Storage Location: {TABLE_LOCATION}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Entities from CSV

# COMMAND ----------

# Create schema for entities table
entities_schema = StructType([
    StructField("entity", StringType(), False),           # Primary key
    StructField("legal_entity", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("business_unit", StringType(), True),
    StructField("division", StringType(), True),
    StructField("country", StringType(), True),
    StructField("region", StringType(), True),
    StructField("continent", StringType(), True)
])

# Read entities CSV from Workspace
entities_df = spark.read \
    .option("header", "true") \
    .option("delimiter", ";") \
    .schema(entities_schema) \
    .csv(ENTITIES_CSV_PATH)

# Add audit columns
entities_df = entities_df \
    .withColumn("_created_at", F.current_timestamp()) \
    .withColumn("_updated_at", F.current_timestamp()) \
    .withColumn("_source_file", F.lit(ENTITIES_CSV_PATH))

print(f"✅ Loaded {entities_df.count()} entities from CSV")
entities_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schema

# COMMAND ----------

# Create config schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")
print(f"✅ Schema {TARGET_CATALOG}.{TARGET_SCHEMA} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create or Update Table (MERGE Strategy)

# COMMAND ----------

# Check if table exists
table_exists = spark.catalog.tableExists(TARGET_FULL_NAME)

if not table_exists:
    print(f"🆕 Table does not exist. Creating new table: {TARGET_FULL_NAME}")
    
    # Create table for the first time
    entities_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(TARGET_FULL_NAME)
    
    print(f"✅ Table created successfully")
    print(f"📊 Records inserted: {entities_df.count()}")
    
else:
    print(f"🔄 Table exists. Performing MERGE (UPSERT) operation...")
    
    # Load existing Delta table
    delta_table = DeltaTable.forName(spark, TARGET_FULL_NAME)
    
    # Perform MERGE
    delta_table.alias("target") \
        .merge(entities_df.alias("source"), "target.entity = source.entity") \
        .whenMatchedUpdate(set = {
            "legal_entity": "source.legal_entity",
            "brand": "source.brand",
            "business_unit": "source.business_unit",
            "division": "source.division",
            "country": "source.country",
            "region": "source.region",
            "continent": "source.continent",
            "_updated_at": "source._updated_at",
            "_source_file": "source._source_file"
        }) \
        .whenNotMatchedInsertAll() \
        .execute()
    
    print(f"✅ MERGE completed successfully")
    
    # Show merge statistics
    history_df = delta_table.history(1)
    operation_metrics = history_df.select("operationMetrics").collect()[0][0]
    
    if operation_metrics:
        print(f"\n📊 Merge Statistics:")
        print(f"   - Rows inserted: {operation_metrics.get('numTargetRowsInserted', 'N/A')}")
        print(f"   - Rows updated: {operation_metrics.get('numTargetRowsUpdated', 'N/A')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Data Load

# COMMAND ----------

# Verify the data was loaded correctly
result_df = spark.table(TARGET_FULL_NAME)
print(f"\n📊 Total entities in table: {result_df.count()}")

# Show sample data
print("\n📋 Sample data:")
display(result_df.orderBy("entity").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Details

# COMMAND ----------

# Show table location and properties
print("\n📁 Table Details:")
spark.sql(f"DESCRIBE DETAIL {TARGET_FULL_NAME}").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "="*60)
print("✅ Entities table seeded successfully!")
print("="*60)
print(f"📊 Total entities: {result_df.count()}")
print(f"🌍 Regions: {result_df.select('region').distinct().count()}")
print(f"🏢 Business Units: {result_df.select('business_unit').distinct().count()}")
print(f"📋 Table: {TARGET_FULL_NAME}")
print(f"💾 Location: {TABLE_LOCATION}")
print(f"📄 Source: {ENTITIES_CSV_PATH}")
print("="*60)

