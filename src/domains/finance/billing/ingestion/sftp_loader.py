# Databricks notebook source
# MAGIC %md
# MAGIC # Raw Ingestion SFTP
# MAGIC
# MAGIC This notebook handles the ingestion of billing data from SFTP sources to the raw layer using Auto Loader.

# COMMAND ----------

import sys
import importlib.util
from typing import Dict, Any
from pyspark.sql import functions as F
from datetime import datetime, timezone
import os
import zipfile
import shutil
import fnmatch

# COMMAND ----------


# Auto-detect bundle location and load config_loader module
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    bundle_base = notebook_path.split('/src/')[0]
    config_loader_path = f"/Workspace{bundle_base}/src/common/utils/config_loader.py"
    
    # Load and execute the config_loader code directly
    with open(config_loader_path, 'r') as f:
        config_loader_code = f.read()
        # Remove notebook markers
        config_loader_code = config_loader_code.replace('# Databricks notebook source', '')
        config_loader_code = config_loader_code.replace('# COMMAND ----------', '')
        config_loader_code = config_loader_code.replace('# MAGIC %md', '#')
        exec(config_loader_code, globals())
    
    print(f"📂 Loaded config_loader from: {config_loader_path}")
except Exception as e:
    print(f"❌ Error loading config_loader: {e}")
    raise

# Setup config path and loader
CONFIG_BASE_PATH = setup_config_path(dbutils)
config_loader = ConfigLoader(CONFIG_BASE_PATH)

# COMMAND ----------

def build_source_path(connection_config: Dict[str, Any], region: str, entity_code: str, document_type: str, source_config: Dict[str, Any]):
    
    """Build source path from connection configuration with template support."""
    base_path = connection_config.get('source', '')
    print(base_path)
    source = source_config.get('connection', {}).get('source_path', {})

    source_path = f"/Volumes/{base_path['catalog']}/{base_path['schema']}/{base_path['volume']}/{base_path['base_path']}/{source}"
    
    return source_path


def extract_zip_recursive(zip_path: str, extract_to: str, file_pattern: str):
    """
    Extract files from a ZIP (including nested ZIPs) matching `file_pattern`.
    Nested ZIPs are extracted directly into extract_to and removed after extraction.
    """
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for file_name in zip_ref.namelist():
            base_name = os.path.basename(file_name)
            if not base_name:
                continue

            if base_name.lower().endswith(".zip"):
                nested_zip_path = os.path.join(extract_to, base_name)
                with zip_ref.open(file_name) as src, open(nested_zip_path, 'wb') as dest:
                    shutil.copyfileobj(src, dest)
                extract_zip_recursive(nested_zip_path, extract_to, file_pattern)
                os.remove(nested_zip_path)

            elif fnmatch.fnmatch(base_name.lower(), file_pattern.lower()):
                target_file = os.path.join(extract_to, base_name)
                with zip_ref.open(file_name) as src, open(target_file, 'wb') as dest:
                    shutil.copyfileobj(src, dest)
                print(f"✅ Extracted {base_name} → {extract_to}")


def unzip_document_type(zip_file: str, source_config: dict, dest_path: str, document_type: str) -> str:
    """
    Extract files from a ZIP (including nested ZIPs) matching `file_pattern`.
    Returns the temporary path where files were extracted.
    """
    document_cfg = source_config.get("document", {})
    file_pattern = document_cfg.get("file_pattern", "*")  # default all files

    temp_path = os.path.join(dest_path, f"{document_type}_temp")
    os.makedirs(temp_path, exist_ok=True)

    extract_zip_recursive(zip_file, temp_path, file_pattern)

    return temp_path

def build_destination_path(connection_config: Dict[str, Any], region: str, entity_code: str, document_type: str) -> str:
    """
    Build destination path for RAW files with date partitioning.
    Keeps original CSV files organized by date for traceability.
    """
    destination = connection_config.get('destination', {})
    base_path = destination.get('base_path', '')
    
    current_date = datetime.now(timezone.utc)
    year, month, day = current_date.strftime('%Y'), current_date.strftime('%m'), current_date.strftime('%d')
    
    dest_path = f"/Volumes/{destination['catalog']}/{destination['schema']}/{destination['volume']}/{base_path}/{region.lower()}/{entity_code.lower()}/{document_type}/{year}/{month}/{day}/"
    
    return dest_path

def build_archive_path(source_path: str) -> str:
    """

    Args:
        source_path (str): Original path,
        keep_trailing_slash (bool): If True, keeps the trailing slash.
    Returns:
        archive path
    """
    # Normalize the path
    normalized_path = os.path.normpath(source_path)

    # Split path into components
    parts = normalized_path.split(os.sep)

    # Replace the first occurrence of 'ingress' (case-insensitive)
    replaced = False
    for i, part in enumerate(parts):
        if part.lower() == "ingress" and not replaced:
            parts[i] = "archive"
            replaced = True

    # Rebuild the path
    archive_path = os.sep + os.sep.join(parts[1:] if parts[0] == "" else parts)

    # Warning if 'ingress' was not found
    if not replaced:
        print(f"⚠️ Warning: 'ingress' folder not found in path: {source_path}")

    return archive_path

def archive_source_file(source_path: str):
    """
    Move contents from source_path to archive_path.

    Args:
        source_path (str): Original path to archive from.

    Raises:
        FileNotFoundError: If source_path does not exist.
    """
    if not os.path.exists(source_path):
        raise FileNotFoundError(f"Source path does not exist: {source_path}")
    
    archive_path = build_archive_path(source_path)
    os.makedirs(archive_path, exist_ok=True)

    for item in os.listdir(source_path):
        src_item = os.path.join(source_path, item)
        dest_item = os.path.join(archive_path, item)
        shutil.move(src_item, dest_item)

    print(f"📦 Archive completed from {source_path} → {archive_path}")

def needs_multiline_read_2(file_path, delimiter, quote, encoding):
    """
    Robust multiline detection by reading the file without multiline and checking
    if Spark detects corrupt records.
    """
    try:
        df_test = (
            spark.read
            .format("csv")
            .option("header", "true")
            .option("delimiter", delimiter)
            .option("quote", quote)
            .option("encoding", encoding)
            .option("mode", "PERMISSIVE")  # capture corrupt rows
            .option("multiLine", "false")  # test without multiline
            .load(file_path)
        )

        # If Spark had trouble parsing → multiline needed
        if "_corrupt_record" in df_test.columns:
            bad = df_test.filter(df_test["_corrupt_record"].isNotNull()).limit(1).count()
            if bad > 0:
                print("Detected Multiline read = True (Spark found corrupt rows)")
                return True

        return False

    except Exception as e:
        print("Detected Multiline read = True (exception triggered)")
        return True


def needs_multiline_2(file_path, encoding):
    """
    Detects whether there are multiline fields by analyzing only a few KB.
    Very fast.
    """
    try:
        with open(file_path, "r", encoding=encoding, errors="ignore") as f:
            sample = f.read(300000)  # read ~300KB

            # 1) Odd number of quotes → likely multiline
            if sample.count('"') % 2 != 0:
                print(f"Detected Multiline read = True")
                return True
                

            # 2) Heuristic: newline inside quoted text
            if '"\n' in sample or '\n"' in sample:
                print(f"Detected Multiline read = True")
                return True

    except Exception:
        # safe fallback
        print(f"Detected Multiline read = True")
        return True

    return False


def needs_multiline(file_path, encoding, small_file_threshold=100*1024*1024):
    """
    Detecta se o CSV precisa de multiline:
    - Para arquivos pequenos (<100MB), assume sempre True
    - Para arquivos grandes, aplica heurística de multiline
    """
    try:
        file_size = os.path.getsize(file_path)
        
        # Arquivos pequenos: sempre multiLine
        if file_size <= small_file_threshold:
            print(f"File size {file_size} bytes <= {small_file_threshold}, using multiLine=True")
            return True

        # Arquivos maiores: aplica heurística
        with open(file_path, "r", encoding=encoding, errors="ignore") as f:
            sample = f.read(300_000)  # lê ~300KB

            # Heurística refinada: contar aspas por linha
            lines = sample.splitlines()
            odd_quotes_lines = [line for line in lines if line.count('"') % 2 != 0]

            if odd_quotes_lines:
                # Checa newline dentro de aspas
                for line in odd_quotes_lines:
                    if '\n' in line or '\r' in line:
                        print("Detected multiline due to newline in quotes")
                        return True

                # Se várias linhas com aspas ímpares
                if len(odd_quotes_lines) > 1:
                    print("Detected multiline due to multiple lines with odd quotes")
                    return True

            # Procura explícita de newline dentro de aspas
            if '"\n' in sample or '\n"' in sample:
                print("Detected multiline due to explicit newline in quotes")
                return True

    except Exception:
        print("Detected multiline due to exception")
        return True

    # Caso nenhuma heurística detecte multiline
    return False


# COMMAND ----------

def process_sftp_ingestion_batch(
    region: str,
    entity_code: str,
    document_type: str,
    source_config: Dict[str, Any],
    connection_config: Dict[str, Any],
    is_last_document_type: bool
) -> None:
    """
    Process SFTP ingestion (CSV/TXT, ZIPs) for a given entity/document_type.
    """
    print(f"[Batch] Processing {entity_code}/{document_type} in {region}")
    
    # Paths
    source_path = build_source_path(connection_config, region, entity_code, document_type, source_config)
    dest_path = build_destination_path(connection_config, region, entity_code, document_type)
    os.makedirs(dest_path, exist_ok=True)

    # Document config
    document_cfg = source_config.get('document', {})
    file_format = document_cfg.get('file_format', 'csv')
    file_pattern = document_cfg.get('file_pattern', None)

    csv_opts = document_cfg.get("csv_options", {})
    header = str(csv_opts.get("header", True)).lower()
    delimiter = csv_opts.get("delimiter", ",")
    encoding = csv_opts.get("encoding", "UTF-8")
    quote = csv_opts.get("quote", '"')
    header_schema = document_cfg.get("header_schema", [])
    inferschema = csv_opts.get("inferSchema", "true")
    # ----------------------------
    # Handle ZIP extraction
    # ----------------------------

    is_zip = source_config.get("is_zip", False)
    temp_path = None
    if is_zip:
        zip_files = [f for f in os.listdir(source_path) if f.endswith(".zip")]
        if not zip_files:
            print(f"⚠️ No zip files found in {source_path}")
            return
        # Extract all ZIPs
        for zip_file_name in zip_files:
            zip_file_path = os.path.join(source_path, zip_file_name)
            print(f"📦 Unzipping {zip_file_path}")
            temp_path = unzip_document_type(zip_file_path, source_config, dest_path, document_type)
    

    # Use temp path if unzip, otherwise source_path
    process_path = temp_path if temp_path else source_path

    # ----------------------------
    # List files to process
    # ----------------------------
    all_files = os.listdir(process_path)
    files = []

    if file_pattern:
        if "*" in file_pattern or "?" in file_pattern:
            files = [f for f in all_files if fnmatch.fnmatch(f.lower(), file_pattern.lower())]
        else:
            regex = re.compile(file_pattern)
            files = [f for f in all_files if regex.match(f)]
    else:
        files = [f for f in all_files if f.endswith(".csv") or f.endswith(".txt")]

    if not files:
        print(f"⚠️ No files found in {process_path} matching pattern {file_pattern}")
        return

    print(f"📁 Found {len(files)} files to process: {files}")

    # ----------------------------
    # Process each file
    # ----------------------------
    for file_name in files:
        file_path = os.path.join(process_path, file_name)
        dest_file_path = os.path.join(dest_path, file_name)

        if os.path.exists(dest_file_path):
            print(f"⏭️ File already exists, skipping: {file_name}")
            continue
        
        multiline_flag = needs_multiline(file_path,encoding)

        # Read file
        df = (spark.read
            .format("csv")
            .option("header", header)
            .option("delimiter", delimiter)      
            .option("quote", quote)
            .option("encoding", encoding)
            .option("inferSchema", inferschema)
            .option("ignoreLeadingWhiteSpace", "false")
            .option("ignoreTrailingWhiteSpace", "false")
            .option("emptyValue", "")            # preserve empty values
            .option("treatEmptyValuesAsNulls", "true")
            .option("multiLine", "true" if multiline_flag else "false")
            .option("mode", "PERMISSIVE")       # never drop columns
            .load(file_path))

        # 2. If No header, use headerschema defined in the source config
        if header == "false":
            current_count = len(df.columns)
            expected_count = len(header_schema)
            #ONLY FOR TEST SCENARIO - REMOVE THIS WHEN WE HAVE RIGHT COL MAPPING/HEADERS
            if current_count < expected_count:
                for i in range(expected_count - current_count):
                    df = df.withColumn(f"_missing_{i}", F.lit(None).cast("string"))
            elif current_count > expected_count:
                df = df.select(df.columns[:expected_count])
            #Rename columns exactly
            df = df.toDF(*header_schema)
        
        # 3. Add metadata columns
        df = (df
            .withColumn("_raw_file_name", F.lit(file_name))
            .withColumn("_raw_ingestion_timestamp", F.current_timestamp())
            .withColumn("_raw_ingestion_date", F.to_date(F.current_timestamp())))
        
        # Write to temp folder
        dest_temp = os.path.join(dest_path, "_temp")
        shutil.rmtree(dest_temp, ignore_errors=True)
        os.makedirs(dest_temp, exist_ok=True)

        (df.coalesce(1)
        .write.mode("overwrite")
        .format("csv")
        .option("header", "true")
        .option("delimiter", delimiter)
        .save(dest_temp))

        # Move single part-0000 file to final destination
        temp_files = [f for f in os.listdir(dest_temp) if f.startswith("part-") and f.endswith(".csv")]
        if not temp_files:
            raise Exception(f"No Spark output found in temp path: {dest_temp}")

        temp_csv_file = os.path.join(dest_temp, temp_files[0])
        shutil.move(temp_csv_file, dest_file_path)
        shutil.rmtree(dest_temp)
        print(f"✅ Processed: {file_name} → {dest_file_path}")

    # ----------------------------
    # Clean up temp folder
    # ----------------------------
    if temp_path and os.path.exists(temp_path):
        shutil.rmtree(temp_path)
        print(f" Cleaned temp folder: {temp_path}")

    # ----------------------------
    # Archive source file (csv or ZIP) #UNCOMMENT WHEN PROD IMPLEMENTATION!!
    # ----------------------------

    #if is_last_document_type:
        #archive_source_file(source_path)


# COMMAND ----------

def main() -> None:
    """Main function to process SFTP ingestion using Batch Processing."""
    print("🚀 Starting SFTP Raw Ingestion Process (Batch Processing)")

    master_config = config_loader.load_master_pipeline_config()
    if not master_config or 'pipelines' not in master_config:
        print("❌ No pipeline configuration found")
        return

    pipelines = master_config['pipelines']
    print(f"📋 Found {len(pipelines)} configured pipelines")

    processed_count = 0
    error_count = 0

    for pipeline in pipelines:
        if (pipeline.get('enabled', True) is False) or (pipeline.get('ingestion_type') != 'sftp'):
            print(f"⏭️ Skipping disabled pipeline: {pipeline['entity_code']}")
            continue

        entity_code = pipeline['entity_code']
        region = pipeline['region']
        document_types = pipeline.get('document_types', ['invoice'])

        print(f"\n📁 Processing entity: {entity_code} in region: {region}")

        for i, document_type in enumerate(document_types):
            is_last_document_type = (i == len(document_types) - 1)
            try:
                source_config = config_loader.load_source_config(region, entity_code, document_type)
                if not source_config or not source_config.get('enabled', True):
                    print(f"⏭️ Skipping disabled source: {entity_code}/{document_type}")
                    continue

                connection_ref = source_config.get('connection', {}).get('connection_ref')
                if not connection_ref:
                    print(f"❌ No connection reference found")
                    error_count += 1
                    continue

                connection_config = config_loader.load_connection_config(connection_ref)
                if not connection_config or not connection_config.get('enabled', True):
                    print(f"❌ Connection not found or disabled: {connection_ref}")
                    error_count += 1
                    continue
                process_sftp_ingestion_batch(
                    region=region,
                    entity_code=entity_code,
                    document_type=document_type,
                    source_config=source_config,
                    connection_config=connection_config,
                    is_last_document_type=is_last_document_type
                )
                processed_count += 1
            

            except Exception as e:
                print(f"❌ Error processing {entity_code}/{document_type}: {e}")
                import traceback
                traceback.print_exc()
                error_count += 1
                continue

    print(f"\n📊 Processing Summary:")
    print(f"✅ Successfully processed: {processed_count}")
    print(f"❌ Errors: {error_count}")
    print("🏁 SFTP Raw Ingestion Process Complete")

# COMMAND ----------

main()
