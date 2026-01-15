# Billing Data Pipeline - Complete Project Overview

## 1. What Is This Project?

This is a **Lakehouse Data Pipeline** built on **Databricks** that processes billing data from multiple business entities across different regions and integrates it with **NetSuite** (Oracle's ERP system for financial management).

### Business Context

The organization (**TeamBlue**) operates multiple business entities across different regions:
- **Nordics** (entity 15E - Sweden-based)
- **SEU** (Southern Europe - entities 31A, 32H)
- **Benelux** (Belgium/Netherlands - entities 11B, 11G, 11T, 11U)

Each entity has its own billing systems (different formats, different sources), and the goal is to:
1. **Ingest** billing data from these diverse sources
2. **Standardize** it into a unified format
3. **Integrate** it with NetSuite for consolidated financial reporting

---

## 2. The Data Pipeline Architecture (Medallion Pattern)

The pipeline follows the **Medallion Architecture** (Bronze → Silver → Gold), a standard in modern data lakehouse design:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DATA FLOW OVERVIEW                                   │
└─────────────────────────────────────────────────────────────────────────────┘

  [EXTERNAL SOURCES]          [RAW LAYER]         [BRONZE]         [SILVER]        [NETSUITE]
  ─────────────────          ──────────          ─────────        ──────────       ───────────
  
  ┌──────────────┐     ┌──────────────────┐   ┌───────────────┐   ┌───────────┐   ┌───────────┐
  │  SFTP Server │────▶│ CSV files        │──▶│ Delta Tables  │──▶│ Unified   │──▶│ NetSuite  │
  │  (15E, 11B)  │     │ /raw/billing/... │   │ billing_*     │   │ Tables    │   │ API       │
  └──────────────┘     └──────────────────┘   └───────────────┘   └───────────┘   └───────────┘
  
  ┌──────────────┐     ┌──────────────────┐   ┌───────────────┐
  │  REST APIs   │────▶│ JSON files       │──▶│ (same Bronze  │
  │  (32H, 11U)  │     │ /raw/billing/... │   │  process)     │
  └──────────────┘     └──────────────────┘   └───────────────┘
  
  ┌──────────────┐     ┌──────────────────┐
  │  Databases   │────▶│ Extracted data   │
  │  (future)    │     │ /raw/billing/... │
  └──────────────┘     └──────────────────┘
```

---

## 3. Step-by-Step Pipeline Flow

### **STEP 1: Data Ingestion (RAW Layer)**

**Business Purpose**: Collect raw billing data from all source systems without modification.

**What happens**:
- **SFTP Loader** (`sftp_loader.py`): Downloads CSV files from SFTP servers
- **API Loader** (`api_loader.py`): Fetches data from REST APIs (like Entersoft, Chargebee)
- **DB Loader** (`db_loader.py`): Extracts data from databases

**Output Location**:
```
/Volumes/teamblue_finance/default/.../raw/billing/
├── nordics/
│   └── 15e/
│       └── invoices/
│           └── 2025/01/15/invoice_data.csv
├── seu/
│   └── 32h-e/
│       └── invoices/
│           └── 2025/01/15/invoices_20250115.json
└── benelux/
    └── 11b/
        └── customers/
            └── 2025/01/15/customers.csv
```

**Key Features**:
- Date-partitioned folder structure (YYYY/MM/DD)
- Preserves original file format (CSV, JSON)
- Adds metadata columns (`_raw_file_name`, `_raw_ingestion_timestamp`)
- Handles ZIP file extraction automatically
- Smart multiline CSV detection for complex files

---

### **STEP 2: Bronze Layer (Delta Live Tables)**

**Business Purpose**: Convert raw files into optimized Delta Lake tables with schema enforcement.

**What happens**:
- Uses **Auto Loader** (cloudFiles) to automatically detect new files
- Adds technical metadata columns:
  - `_bronze_ingestion_time`: When the record was processed
  - `_entity_code`: Which business entity (15E, 32H, etc.)
  - `_region`: Geographic region (nordics, seu, benelux)
  - `_raw_record_hash`: SHA256 hash for deduplication
- Creates partitioned Delta tables

**Output Tables**:
```sql
teamblue_finance.bronze.billing_nordics_15e_invoices
teamblue_finance.bronze.billing_seu_32h_e_customers
teamblue_finance.bronze.billing_benelux_11b_items
```

**Key Features**:
- Streaming ingestion with Auto Loader
- Schema evolution support (new columns added automatically)
- Data quality expectations enforcement
- Partitioned by `_bronze_processing_date`
- Rescued data column for malformed records

---

### **STEP 3: Silver Layer (Standardization & Consolidation)**

**Business Purpose**: Create unified, clean data that can be used across all entities.

**What happens**:
- Reads Bronze tables
- Applies **Contract Mappings** (source → target column transformations)
- Converts field names to NetSuite format (snake_case for DWH)
- Consolidates all entities into single tables per document type
- Applies CDC (Change Data Capture) for deduplication

**Output Tables**:
```sql
teamblue_finance.silver.billing_invoice    -- All invoices from all entities
teamblue_finance.silver.billing_customer   -- All customers from all entities
teamblue_finance.silver.billing_item       -- All items from all entities
teamblue_finance.silver.billing_payment    -- All payments from all entities
```

**Key Features**:
- Unified schema across all entities
- Automatic snake_case column naming
- CDC deduplication (last-write-wins)
- Partitioned by `_entity_code` and `_processing_date`

---

### **STEP 4: NetSuite Integration**

**Business Purpose**: Push standardized data to NetSuite ERP for financial consolidation.

**What happens**:
- Reads Silver tables
- Transforms to NetSuite API format (JSON payloads)
- Performs `upsertList` operations via NetSuite SOAP/REST API
- Handles lookups (subsidiary IDs, currency codes, customer references)
- Manages batch operations with retry logic

---

## 4. Important Files Overview

### **Configuration Files**

| File | Purpose |
|------|---------|
| `resources/.../config/pipelines/billing_master.yaml` | Master configuration - defines which entities are enabled |
| `resources/.../config/sources/{region}/{entity}/{doc}.yaml` | Per-entity source configuration |
| `resources/.../config/connections/SFTP/*.yaml` | SFTP connection details |
| `resources/.../config/connections/API/*.yaml` | API connection details |
| `resources/.../config/entities.csv` | Reference table of all business entities |
| `resources/.../config/netsuite_templates/*.csv` | NetSuite object schema definitions |

### **Ingestion Files**

| File | Purpose |
|------|---------|
| `src/domains/finance/billing/ingestion/sftp_loader.py` | SFTP data ingestion notebook |
| `src/domains/finance/billing/ingestion/api_loader.py` | REST API data ingestion notebook |
| `src/domains/finance/billing/ingestion/db_loader.py` | Database extraction notebook |

### **Transformation Files**

| File | Purpose |
|------|---------|
| `src/domains/finance/billing/transformation/billing_dlt_pipeline_raw_to_bronze.py` | RAW → Bronze DLT pipeline |
| `src/domains/finance/billing/transformation/billing_dlt_pipeline_raw_to_bronze_v2.py` | Alternative Bronze DLT pipeline |
| `resources/.../transformations/generic_transformation.py` | Contract-driven transformation engine |
| `resources/.../transformations/custom/{Region}/{Entity}/*.py` | Entity-specific custom transformations |

### **Setup Files**

| File | Purpose |
|------|---------|
| `src/domains/finance/billing/setup/seed_entities.py` | Loads entity reference data to Delta table |

### **Contract Files**

| File | Purpose |
|------|---------|
| `resources/.../contracts/{region}/{entity}/{doc_type}/contract.yaml` | Field mapping definitions |

### **Utility Files**

| File | Purpose |
|------|---------|
| `src/common/utils/config_loader.py` | YAML configuration loading utility |
| `src/common/utils/util_api.py` | API client utilities |
| `src/common/utils/fs_utils.py` | File system utilities |

---

## 5. SQL Mapping & Contract Explanation

### How Contracts Work

Each entity/document type has a **contract.yaml** file that defines how source fields map to NetSuite fields:

```yaml
# Example: resources/.../contracts/nordics/15E/invoice/contract.yaml
document_type: "invoice"
region: "nordics"
entity_code: "15E"
version: "1.0"

mappings:
  # Invoice header fields
  - source_column: "invoice_id"      # Field in source CSV
    source_type: "string"            # Source data type
    target_column: "externalId"      # NetSuite field name
    target_type: "string"            # NetSuite data type

  - source_column: "customer_id"
    source_type: "string"
    target_column: "entity"
    target_type: "RecordRef"         # Reference to another NetSuite record

  - source_column: "invoice_date"
    source_type: "date"
    target_column: "tranDate"
    target_type: "dateTime"

  # Line item fields (nested in NetSuite)
  - source_column: "quantity"
    source_type: "decimal"
    target_column: "Invoice.itemList.item.quantity"
    target_type: "double"

  # Custom fields
  - source_column: "record_source_id"
    source_type: "string"
    target_column: "custbody_nb2_recsource"
    target_type: "CustomField"
```

### Data Type Mapping

| Source Type | Spark Type | NetSuite Type |
|-------------|------------|---------------|
| `string` | `StringType` | `string` |
| `date` | `DateType` | `dateTime` |
| `decimal` | `DoubleType` | `decimal` / `double` |
| `boolean` | `BooleanType` | `boolean` |
| `int` / `integer` | `IntegerType` | `int` |
| `timestamp` | `TimestampType` | `dateTime` |
| `RecordRef` | `StringType` | Reference to another record |
| `CustomField` | `StringType` | NetSuite custom field |

### Silver Layer Naming Convention

The Silver layer automatically converts NetSuite camelCase to snake_case:

| NetSuite Field | Silver Column |
|----------------|---------------|
| `Customer.externalId` | `customer.external_id` |
| `Invoice.tranDate` | `invoice.tran_date` |
| `Invoice.itemList.item.quantity` | `invoice.item_list.item.quantity` |
| `billingAddress.country.value` | `billing_address.country.value` |

### Lookup Tables

Contracts can include lookup tables for reference data:

```yaml
lookups:
  subsidiary_map:
    "15E": 36    # Nordics subsidiary ID in NetSuite
    "32H": 37    # SEU subsidiary ID
  
  currency_map:
    "SEK": 1     # Swedish Krona
    "EUR": 2     # Euro
    "USD": 3     # US Dollar
    "GBP": 4     # British Pound
```

---

## 6. Master Pipeline Configuration

The `billing_master.yaml` controls which entities are processed:

```yaml
version: '1.0'
pipelines:
  - entity_code: '15E'
    region: 'nordics'
    document_types: ['invoices']
    ingestion_type: 'sftp'
    enabled: True

  - entity_code: '32H-E'
    region: 'seu'
    document_types: ['invoices', 'payments', 'customers', 'items']
    ingestion_type: 'api'
    enabled: False
```

**Key Fields**:
- `entity_code`: Unique identifier for the business entity
- `region`: Geographic region (nordics, seu, benelux)
- `document_types`: List of document types to process
- `ingestion_type`: Source type (sftp, api, database)
- `enabled`: Whether this pipeline is active

---

## 7. Business Entity Summary

| Entity Code | Region | Ingestion Type | Document Types | Status |
|-------------|--------|----------------|----------------|--------|
| **15E** | Nordics | SFTP | invoices | **Enabled** |
| 32H-E | SEU | API | invoices, payments, customers, items | Disabled |
| 11B | Benelux | SFTP | invoices, invoice_lines, providers, items, customers | Disabled |
| 11T | Benelux | SFTP | invoices, invoice_lines, providers, items, customers | Disabled |
| 11G | Benelux | SFTP | invoices, invoice_lines, providers, items, customers | Disabled |
| 11U | Benelux | API | invoices, credit_notes, payments_refunds, customers, items | Disabled |
| 31A | SEU | SFTP | items, transactions, customers | Disabled |
| 31A-SF | SEU | SFTP | invoices, customers, items | Disabled |

---

## 8. Source Configuration Example

Each entity/document type has a source configuration file:

```yaml
# resources/.../config/sources/seu/32h-e/invoices.yaml
version: '1.0'
entity_code: '32H-E'
region: 'SEU'
enabled: true

connection:
  connection_ref: 'api_entersoft'  # Links to connections/API/api_entersoft.yaml
  
document:
  document_type: 'invoices'
  file_pattern: '*.json'
  format: 'json'
  json_options:
    multiline: true
    inferSchema: true
  json_column_extract: 'Rows'  # Extract data from this JSON array
  
contract:
  contract_path: 'contracts/SEU/32H-E/invoices/contract.yaml'
  
transformations:
  custom_module: 'resources.domains.finance.billing.core.transformations.custom.SEU.32H-E.invoice'
  custom_function: 'apply_transformation'
  
paths:
  checkpoint_bronze: '/Volumes/.../bronze/billing/seu/32h-e/invoices'
  checkpoint_silver: '/Volumes/.../silver/billing/invoices'
```

---

## 9. Custom Transformations

Entity-specific transformations can be applied before Bronze processing:

```python
# resources/.../transformations/custom/Nordics/15E/invoice.py
from pyspark.sql import functions as F

def apply_transformation(df, mapping_config=None):
    """
    Custom transformation for Nordics 15E invoice.
    
    Args:
        df: Input DataFrame from RAW
        mapping_config: Optional contract mappings
    
    Returns:
        Transformed DataFrame
    """
    # Example: Add calculated column
    df = df.withColumn("calculated_amount", F.col("amount") * F.lit(1.2))
    
    # Example: Filter invalid records
    df = df.filter(F.col("amount") > 0)
    
    return df
```

---

## 10. Deployment & Execution

### Databricks Asset Bundles

The project uses **Databricks Asset Bundles** for deployment:

```yaml
# databricks.yml
bundle:
  name: teamblue_lakehouse

targets:
  dev:
    mode: development
    workspace:
      host: https://sagacitysolutions-teamblue-workspace.cloud.databricks.com
  
  prod:
    mode: production
    workspace:
      host: https://sagacitysolutions-teamblue-workspace.cloud.databricks.com
```

### Deployment Commands

```bash
# Deploy to dev environment
databricks bundle deploy -t dev

# Deploy to production
databricks bundle deploy -t prod
```

### Execution Order (Orchestrated Job)

1. SFTP Ingestion → RAW
2. API Ingestion → RAW (parallel)
3. DB Ingestion → RAW (parallel)
4. DLT Bronze Pipeline (continuous)
5. Standardize Customer (parallel after ingestion)
6. Standardize Invoice (parallel)
7. Standardize Item (parallel)
8. Standardize Payment (parallel)
9. DLT Standard → Silver (after all standardizations)
10. NetSuite Integration (after Silver)

---

## 11. Data Validation & Quality

### DLT Expectations

Bronze tables enforce data quality using DLT expectations:

```python
@dlt.expect_all(expectations)
@dlt.expect_or_drop("null_records", "_raw_record_hash IS NOT NULL")
def table_func():
    return bronze_ingestion(...)
```

### Verification Queries

```sql
-- Check Bronze tables
SHOW TABLES IN teamblue_finance.bronze LIKE 'billing_*';

-- Check data counts
SELECT COUNT(*) FROM teamblue_finance.bronze.billing_nordics_15e_invoices;

-- Check Silver consolidation
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
  COUNT(*) as versions
FROM teamblue_finance.silver.billing_invoice
GROUP BY external_id, _entity_code
HAVING COUNT(*) > 1;
```

---

## 12. Key Takeaways

1. **Multi-tenant architecture**: One pipeline handles multiple business entities with different data formats
2. **Config-driven**: All entity configurations are in YAML files - no code changes needed to add new entities
3. **Medallion pattern**: RAW → Bronze → Silver → NetSuite follows data lakehouse best practices
4. **Contract-based mapping**: Contracts define how each entity's data maps to the unified NetSuite schema
5. **Delta Live Tables**: Uses DLT for reliable, incremental data processing with quality expectations
6. **NetSuite integration**: Final destination is Oracle NetSuite for consolidated financial reporting
7. **Extensible**: Adding new entities requires only configuration files, not code changes
8. **Traceable**: Full audit trail with technical metadata columns at each layer

---

## 13. Adding a New Entity

To add a new entity:

1. **Create Source Configuration**: `resources/.../config/sources/{region}/{entity}/{doc_type}.yaml`
2. **Create Contract File**: `resources/.../contracts/{region}/{entity}/{doc_type}/contract.yaml`
3. **Add to Master Config**: Add entry in `billing_master.yaml` with `enabled: true`
4. **Create Custom Transformation** (optional): `resources/.../transformations/custom/{Region}/{Entity}/{doc_type}.py`
5. **Deploy**: `databricks bundle deploy -t dev`

See `docs/QUICK_START.md` for detailed instructions.

---

**Last Updated**: January 2025
