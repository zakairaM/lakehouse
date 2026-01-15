# Billing Data Pipeline - Complete Architecture

## 📊 Overview

The Billing Data Pipeline is a comprehensive data engineering solution that processes billing data from multiple sources (SFTP, API, Database) through a multi-layer architecture, ultimately integrating with NetSuite.

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         BILLING DATA PIPELINE                            │
└─────────────────────────────────────────────────────────────────────────┘

1. INGESTION (RAW)                    →  2. BRONZE (DLT)                   
   SFTP/API/DB Loaders                   Bronze DLT Pipeline              
   ↓                                       ↓                                
   /Volumes/.../raw/billing/              bronze.billing_* tables          
   {region}/{entity}/{type}/              (Delta Tables)                   
   {yyyy}/{mm}/{dd}/*.csv                                                

3. STANDARDIZATION                  →  4. SILVER (DLT)                    
   Standardization Notebook               Silver DLT Pipeline               
   ↓                                       ↓                                
   /Volumes/.../silver/                  silver.billing_* tables          
   standard_billing_{type}s/             (Consolidated Delta)             
   {region}/{entity}/{yyyy}/{mm}/{dd}/                                   

5. INTEGRATION                        
   NetSuite Integration                  
   ↓                                      
   NetSuite API (upsertList)              
```

---

## 🔄 Pipeline Flow - Step by Step

### **STEP 1: RAW Ingestion** (`sftp_loader.py`, `api_loader.py`, `db_loader.py`)

#### What it does:
- **SFTP Loader**: Copies CSV files from SFTP volume to RAW layer
- **API Loader**: Makes HTTP calls and saves JSON/CSV to RAW
- **DB Loader**: Extracts data from databases and saves to RAW

#### Output Location:
```
/Volumes/teamblue_finance/default/sagacity-data-teamblue-finance-dev/raw/billing/
├── {region}/              # e.g., nordics, SEU
│   └── {entity_code}/     # e.g., 15E, 32H
│       └── {document_type}/   # e.g., invoice, customer
│           └── {yyyy}/
│               └── {mm}/
│                   └── {dd}/
│                       └── *.csv
```

#### How to Execute:
```python
# Option 1: Individual notebook
Notebook: sftp_loader.py
# Executes for all enabled pipelines with ingestion_type: 'sftp'

# Option 2: Via Orchestrated Job
Job: "Billing - Complete Pipeline (Orchestrated)"
Task: "sftp_ingestion"
```

#### Input:
- SFTP volume configured in `resources/domains/finance/billing/core/config/connections/SFTP/`
- Source configuration in `resources/domains/finance/billing/core/config/sources/{region}/{entity}/{type}.yaml`

#### Output:
- CSV files in RAW layer with date partitioning

---

### **STEP 2: Bronze DLT Pipeline** (`billing_dlt_pipeline_raw_to_bronze.py`)

#### What it does:
- **Continuous DLT Pipeline** that monitors RAW layer
- Automatically detects new files via **Auto Loader**
- Applies **contracts** (column mapping) from `resources/.../contracts/`
- Applies **custom transformations** (if configured) before contract mapping
- Creates Delta tables in Bronze format
- Adds technical columns: `_bronze_ingestion_time`, `_entity_code`, `_region`, `_processing_date`, `_raw_record_hash`

#### Custom Transformations:
Custom transformations are applied **before** contract mapping. They are defined in:
```
resources/domains/finance/billing/core/transformations/custom/{Region}/{entity_code}/{document_type}.py
```

The function signature should be:
```python
def apply_transformation(df, mapping_config=None):
    # Custom transformation logic
    return df
```

#### Output Location:
```
Schema: teamblue_finance.bronze
Tables: 
  - bronze.billing_nordics_15e_invoice
  - bronze.billing_nordics_15e_customer
  - bronze.billing_seu_32h_invoice
  - etc.
```

#### How to Execute:
```python
# Option 1: DLT Pipeline (continuous/triggered)
UI: Workflows → Delta Live Tables
Pipeline: "Billing Pipeline - RAW → Bronze"
Action: Start (or configure schedule)

# Option 2: Via Orchestrated Job
# NOTE: Bronze pipeline runs continuously in background
# The orchestrated job assumes it's already running
```

#### Input:
- CSV files from RAW layer (`/Volumes/.../raw/billing/`)
- Contract YAML: `resources/domains/finance/billing/core/contracts/{region}/{entity}/{type}/contract.yaml`
- Custom transformation module (if configured)

#### Output:
- Delta Bronze tables: `bronze.billing_{region}_{entity}_{type}`
- Format: Delta Lake (optimized, partitioned by `_processing_date`)

---

### **STEP 3: Standardization** (`billing_nb_bronze_to_standard.py`)

#### What it does:
- Reads Bronze tables (all enabled entities)
- Applies contract mapping to convert to DWH format (snake_case)
- Converts `target_column` (NetSuite camelCase) → snake_case column name
- Writes standardized Parquet files with date partitioning (yyyy/mm/dd)
- **Important**: Uses `overwrite` only for the current day's partition

#### Output Location:
```
/Volumes/teamblue_finance/default/sagacity-data-teamblue-finance-dev/silver/standard_billing_{type}s/
├── {region}/              # e.g., nordics
│   └── {entity_code}/     # e.g., 15e
│       └── {yyyy}/         # e.g., 2025
│           └── {mm}/       # e.g., 01
│               └── {dd}/   # e.g., 15
│                   └── part-00000-*.snappy.parquet
```

#### How to Execute:
```python
# Option 1: Individual notebook
Notebook: billing_nb_bronze_to_standard.py
Widget: document_type = "invoice"  # or leave empty for all

# Option 2: Via Orchestrated Job
Job: "Billing - Complete Pipeline (Orchestrated)"
Tasks: 
  - "standardize_customer"
  - "standardize_invoice"
  - "standardize_item"
  - "standardize_payment"
# (Run in parallel after ingestion)
```

#### Input:
- Bronze tables: `bronze.billing_*`
- Master Pipeline Config: `resources/domains/finance/billing/core/config/pipelines/billing_master.yaml`
- Contracts: `resources/domains/finance/billing/core/contracts/{region}/{entity}/{type}/contract.yaml`

#### Output:
- Standardized Parquet files in `/Volumes/.../silver/standard_billing_{type}s/`
- Partitioned by date (overwrite only for current day)
- Format: Parquet (single file per partition)
- Structure: `{region}/{entity}/{yyyy}/{mm}/{dd}/part-00000-*.snappy.parquet`

#### Characteristics:
- ✅ **Incremental**: Only current day's partition is overwritten
- ✅ **Efficient**: Doesn't rewrite millions of historical records
- ✅ **Consistent**: Uses `master_pipeline_config` to process only enabled pipelines

---

### **STEP 4: Silver DLT Pipeline** (`billing_dlt_pipeline_standard_to_silver.py`)

#### What it does:
- **DLT Pipeline** that consolidates all standardized Parquet files
- Uses **Auto Loader** with `recursiveFileLookup` to read all date partitions
- Consolidates data from **all entities** into a single Silver table
- Applies **CDC (Change Data Capture)**:
  - Deduplication by `external_id + entity_code`
  - Last-write-wins based on `_standardization_time`
  - Filters soft-deletes (`_is_deleted = true`)

#### Output Location:
```
Schema: teamblue_finance.silver
Tables:
  - silver.billing_invoice    (all entities consolidated)
  - silver.billing_customer
  - silver.billing_item
  - silver.billing_payment
```

#### How to Execute:
```python
# Option 1: DLT Pipeline (triggered)
UI: Workflows → Delta Live Tables
Pipeline: "Billing Pipeline - Standard Parquet -> Silver"
Action: Start (or configure schedule)

# Option 2: Via Orchestrated Job
Job: "Billing - Complete Pipeline (Orchestrated)"
Task: "dlt_standard_to_silver"
# Executes after all standardizations complete
```

#### Input:
- Standardized Parquet files: `/Volumes/.../silver/standard_billing_{type}s/`
- Structure: `{region}/{entity}/{yyyy}/{mm}/{dd}/*.parquet`

#### Output:
- Delta Silver tables: `silver.billing_{type}`
- Format: Delta Lake (consolidated, partitioned by `_entity_code` and `_processing_date`)
- CDC applied: only latest version of each record

#### Characteristics:
- ✅ **Consolidation**: Combines data from all entities (15E, 32H, etc.)
- ✅ **CDC**: Automatic deduplication
- ✅ **Incremental**: Auto Loader automatically detects new files

---

### **STEP 5: NetSuite Integration** (`netsuite_integration_job.py`)

#### What it does:
- Reads data from Silver
- Transforms to NetSuite format (using templates)
- Performs `upsertList` to NetSuite API
- Manages lookups (customer, item, etc.)

#### How to Execute:
```python
# Via Orchestrated Job
Job: "Billing - Complete Pipeline (Orchestrated)"
Task: "netsuite_integration"
# Executes after Silver is ready
```

---

## 🚀 How to Execute the Complete Flow

### **Option 1: Orchestrated Job (Recommended)**

```bash
# 1. Deploy (if needed)
databricks bundle deploy -t dev

# 2. Execute complete job
# Via UI:
Workflows → Jobs → "Billing - Complete Pipeline (Orchestrated)" → Run Now

# Via CLI:
databricks jobs run-now --job-id <JOB_ID>
```

**Automatic Execution Order:**
1. ✅ SFTP Ingestion → RAW
2. ✅ API Ingestion → RAW (parallel)
3. ✅ DB Ingestion → RAW (parallel)
4. ✅ Standardize Customer (parallel after ingestion)
5. ✅ Standardize Invoice (parallel)
6. ✅ Standardize Item (parallel)
7. ✅ Standardize Payment (parallel)
8. ✅ DLT Standard → Silver (after all standardizations)
9. ✅ NetSuite Integration (after Silver)

**Note**: Bronze DLT pipeline must be running continuously in background.

---

### **Option 2: Manual (Step by Step)**

```python
# 1. RAW Ingestion
Execute: sftp_loader.py
Verify: /Volumes/.../raw/billing/{region}/{entity}/{type}/{yyyy}/{mm}/{dd}/

# 2. Bronze DLT (if not running)
UI: Workflows → Delta Live Tables → "Billing Pipeline - RAW → Bronze" → Start
Verify: bronze.billing_* tables

# 3. Standardization
Execute: billing_nb_bronze_to_standard.py
Widget: document_type = "invoice"
Verify: /Volumes/.../silver/standard_billing_invoices/{region}/{entity}/{yyyy}/{mm}/{dd}/*.parquet

# 4. Silver DLT
UI: Workflows → Delta Live Tables → "Billing Pipeline - Standard Parquet -> Silver" → Start
Verify: silver.billing_invoice

# 5. NetSuite (if needed)
Execute: netsuite_integration_job.py
```

---

## 📋 Validations and Verifications

### **Verify RAW:**
```sql
-- List RAW files
SELECT * FROM csv.`/Volumes/teamblue_finance/default/sagacity-data-teamblue-finance-dev/raw/billing/nordics/15e/invoice/2025/01/15/`
LIMIT 10;
```

### **Verify Bronze:**
```sql
-- Check Bronze tables
SHOW TABLES IN teamblue_finance.bronze LIKE 'billing_*';

-- Check data
SELECT COUNT(*) FROM teamblue_finance.bronze.billing_nordics_15e_invoice;

-- Check custom transformations applied
SELECT DISTINCT _entity_code, _region, _document_type 
FROM teamblue_finance.bronze.billing_nordics_15e_invoice;
```

### **Verify Standardized Parquet Files:**
```sql
-- List standardized Parquet files
SELECT * FROM parquet.`/Volumes/teamblue_finance/default/sagacity-data-teamblue-finance-dev/silver/standard_billing_invoices/nordics/15e/2025/01/15/`
LIMIT 10;
```

### **Verify Silver:**
```sql
-- Check Silver tables
SHOW TABLES IN teamblue_finance.silver LIKE 'billing_*';

-- Check consolidated data
SELECT 
  _entity_code,
  COUNT(*) as records,
  MAX(_silver_ingestion_time) as last_update
FROM teamblue_finance.silver.billing_invoice
GROUP BY _entity_code;

-- Check CDC deduplication
SELECT 
  external_id,
  _entity_code,
  COUNT(*) as versions,
  MAX(_standardization_time) as latest_version
FROM teamblue_finance.silver.billing_invoice
GROUP BY external_id, _entity_code
HAVING COUNT(*) > 1;
```

---

## 🔧 Required Configurations

### **1. Master Pipeline Config**
File: `resources/domains/finance/billing/core/config/pipelines/billing_master.yaml`
```yaml
pipelines:
  - entity_code: "15E"
    region: "nordics"
    enabled: true
    ingestion_type: "sftp"
    document_types: ["invoice", "customer", "item", "payment"]
```

### **2. Source Config**
File: `resources/domains/finance/billing/core/config/sources/{region}/{entity}/{type}.yaml`
```yaml
version: '1.0'
entity_code: '15E'
region: 'nordics'
enabled: true

connection:
  connection_ref: 'sftp_donnington'

document:
  document_type: 'invoice'
  file_pattern: '*.csv'
  format: 'csv'
  csv_options:
    header: true
    delimiter: ','
    inferSchema: false

contract:
  contract_path: 'contracts/nordics/15E/invoice/contract.yaml'

transformations:
  custom_module: 'resources.domains.finance.billing.core.transformations.custom.nordics.15E.invoice'
  custom_function: 'apply_transformation'
```

### **3. Contract**
File: `resources/domains/finance/billing/core/contracts/{region}/{entity}/{type}/contract.yaml`
```yaml
document_type: "invoice"
region: "nordics"
entity_code: "15E"
version: "1.0"
description: "Invoice mapping for Nordics 15E"

mappings:
  - source_column: "invoice_id"
    source_type: "string"
    target_column: "Invoice.externalId"
    target_type: "string"
```

---

## ⚠️ Important Points

1. **Bronze DLT must be running continuously** to automatically process new RAW data
2. **Standardization is incremental**: Only current day's partition is overwritten
3. **Silver DLT consolidates everything**: Reads Parquet files from all entities and dates
4. **CDC in Silver**: Only the latest version of each record is maintained
5. **Orchestrated job**: Executes everything in correct sequence, but Bronze DLT must be running separately
6. **Custom transformations**: Applied before contract mapping in Bronze layer

---

## 📊 Layer Summary

| Layer | Format | Location | Partition |
|-------|--------|----------|-----------|
| **RAW** | CSV | `/Volumes/.../raw/billing/` | `{region}/{entity}/{type}/{yyyy}/{mm}/{dd}/` |
| **Bronze** | Delta | `bronze.billing_*` | `_processing_date` |
| **Standard** | Parquet | `/Volumes/.../silver/standard_billing_*/` | `{region}/{entity}/{yyyy}/{mm}/{dd}/` |
| **Silver** | Delta | `silver.billing_*` | `_entity_code`, `_processing_date` |

---

## 🔍 Key Files

| File | Type | Function |
|------|------|----------|
| `sftp_loader.py` | Notebook | RAW ingestion (SFTP) |
| `api_loader.py` | Notebook | RAW ingestion (API) |
| `db_loader.py` | Notebook | RAW ingestion (Database) |
| `billing_dlt_pipeline_raw_to_bronze.py` | **DLT Pipeline** | RAW → Bronze (with custom transformations) |
| `billing_nb_bronze_to_standard.py` | **Notebook** | Bronze → Standard Parquet |
| `billing_dlt_pipeline_standard_to_silver.py` | **DLT Pipeline** | Standard Parquet → Silver |
| `netsuite_integration_job.py` | Notebook | Silver → NetSuite |

---

## 📞 Troubleshooting

### Issue: Bronze pipeline not processing data
**Solution**: 
- Verify Bronze DLT pipeline is running
- Check RAW files exist in expected location
- Verify entity is enabled in `billing_master.yaml`
- Check contract file exists and is valid

### Issue: Custom transformation not applied
**Solution**:
- Verify `custom_module` path in source config
- Check transformation file exists and has `apply_transformation` function
- Review pipeline logs for transformation errors

### Issue: Standardization overwriting all data
**Solution**: 
- Verify date partitioning is correct
- Check that only current day's partition is being overwritten
- Review `billing_nb_bronze_to_standard.py` logic

### Issue: Silver consolidation missing entities
**Solution**:
- Verify all entities have standardized Parquet files
- Check Auto Loader is reading recursively (`recursiveFileLookup: true`)
- Verify schema evolution is enabled

---

**Last Updated**: 2025-01-15

