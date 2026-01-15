# Quick Start Guide

## 🚀 Adding a New Entity Integration

This guide will help you quickly set up a new entity integration for the Billing Data Pipeline.

### Overview

To integrate a new entity, you need to create three configuration files:
1. **Source Configuration** - Defines how to ingest data (SFTP, API, or Database)
2. **Contract File** - Defines the mapping from source to NetSuite format
3. **Custom Transformation** (optional) - Entity-specific transformations before Bronze

### Step-by-Step Guide

#### Step 1: Create Source Configuration

Create a new source configuration file at:
```
resources/domains/finance/billing/core/config/sources/{region}/{entity_code}/{document_type}.yaml
```

**Example**: `resources/domains/finance/billing/core/config/sources/nordics/15E/invoice.yaml`

```yaml
version: '1.0'
entity_code: '15E'
region: 'nordics'
enabled: true

connection:
  connection_ref: 'sftp_donnington'  # Reference to connections/SFTP/sftp_donnington.yaml

document:
  document_type: 'invoice'
  file_pattern: '*.csv'
  format: 'csv'
  csv_options:
    header: true
    delimiter: ','
    inferSchema: false  # Use contract schema

contract:
  contract_path: 'contracts/nordics/15E/invoice/contract.yaml'

transformations:
  custom_module: 'resources.domains.finance.billing.core.transformations.custom.nordics.15E.invoice'  # Optional
  custom_function: 'apply_transformation'  # Optional, defaults to 'apply_transformation'

paths:
  checkpoint_bronze: '/Volumes/teamblue_finance/default/sagacity-data-teamblue-finance-dev/_checkpoints/bronze/billing/nordics/15E/invoice'
  checkpoint_silver: '/Volumes/teamblue_finance/default/sagacity-data-teamblue-finance-dev/_checkpoints/silver/billing/invoice'
```

**Key Configuration Options:**

- **Connection Types:**
  - `sftp_donnington` - SFTP connection (see `connections/SFTP/`)
  - `api_entersoft` - API connection (see `connections/API/`)
  - `db_template` - Database connection (see `connections/Database/`)

- **Document Formats:**
  - CSV: `format: 'csv'` with `csv_options`
  - JSON: `format: 'json'` with `json_options`

- **Custom Transformations:**
  - Optional: Path to custom transformation module
  - Function must be named `apply_transformation(df, mapping_config=None)`

#### Step 2: Create Contract File

Create a contract file at:
```
resources/domains/finance/billing/core/contracts/{region}/{entity_code}/{document_type}/contract.yaml
```

**Example**: `resources/domains/finance/billing/core/contracts/nordics/15E/invoice/contract.yaml`

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

  - source_column: "amount"
    source_type: "decimal"
    target_column: "Invoice.total"
    target_type: "double"

  - source_column: "currency"
    source_type: "string"
    target_column: "Invoice.currency"
    target_type: "RecordRef"
```

**Important Notes:**
- `target_column` uses NetSuite camelCase format (e.g., `Invoice.externalId`)
- Silver layer automatically converts to snake_case (e.g., `invoice_external_id`)
- See `MAPPING_GUIDE.md` for detailed mapping validation rules

#### Step 3: Add to Master Pipeline Configuration

Add your new entity to the master pipeline configuration:
```
resources/domains/finance/billing/core/config/pipelines/billing_master.yaml
```

**Example Entry:**
```yaml
pipelines:
  - entity_code: "15E"
    region: "nordics"
    enabled: true
    document_types:
      - "invoice"
      - "customer"
      - "item"
      - "payment"
```

#### Step 4: Create Custom Transformation (Optional)

If your entity needs custom transformations before Bronze processing, create:
```
resources/domains/finance/billing/core/transformations/custom/{Region}/{entity_code}/{document_type}.py
```

**Example**: `resources/domains/finance/billing/core/transformations/custom/Nordics/15E/invoice.py`

```python
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

#### Step 5: Enable the Pipeline

1. **Verify Configuration:**
   ```bash
   # Check source config exists
   resources/domains/finance/billing/core/config/sources/{region}/{entity_code}/{document_type}.yaml
   
   # Check contract exists
   resources/domains/finance/billing/core/contracts/{region}/{entity_code}/{document_type}/contract.yaml
   
   # Check master pipeline config
   resources/domains/finance/billing/core/config/pipelines/billing_master.yaml
   ```

2. **Deploy Configuration:**
   ```bash
   databricks bundle deploy -t dev
   ```

3. **Trigger Ingestion:**
   - **SFTP**: Files will be processed automatically when placed in the SFTP volume
   - **API**: Run `api_loader.py` notebook
   - **Database**: Run `db_loader.py` notebook

### Verification Checklist

After setting up a new entity, verify:

- [ ] Source configuration file created and valid YAML
- [ ] Contract file created with all required mappings
- [ ] Entity added to `billing_master.yaml` with `enabled: true`
- [ ] Custom transformation file created (if needed)
- [ ] Connection configuration exists (SFTP/API/DB)
- [ ] Data appears in RAW layer after ingestion
- [ ] Bronze table created: `bronze.billing_{region}_{entity_code}_{document_type}`
- [ ] Standardization produces Parquet files
- [ ] Silver table contains consolidated data

### Testing the Integration

1. **Test RAW Ingestion:**
   ```python
   # Check RAW files
   %fs ls /Volumes/teamblue_finance/default/sagacity-data-teamblue-finance-dev/raw/billing/{region}/{entity_code}/{document_type}/
   ```

2. **Test Bronze:**
   ```sql
   SELECT COUNT(*) FROM bronze.billing_{region}_{entity_code}_{document_type};
   SELECT * FROM bronze.billing_{region}_{entity_code}_{document_type} LIMIT 10;
   ```

3. **Test Standardization:**
   ```python
   # Run standardization notebook
   %run ./src/domains/finance/billing/transformation/billing_nb_bronze_to_standard
   # Widget: document_type = "{document_type}"
   ```

4. **Test Silver:**
   ```sql
   SELECT COUNT(*) FROM silver.billing_{document_type}
   WHERE _entity_code = '{entity_code}' AND _region = '{region}';
   ```

### Common Issues

#### Issue: Contract not found
**Solution**: Verify contract path in source config matches actual file location

#### Issue: Entity not processed
**Solution**: Check `billing_master.yaml` - ensure `enabled: true` and `document_types` includes your type

#### Issue: Custom transformation not applied
**Solution**: Verify `custom_module` path in source config matches actual file location

#### Issue: Schema mismatch
**Solution**: Check contract `source_type` matches actual data types in source files

### Next Steps

- Review `MAPPING_GUIDE.md` for contract validation rules
- Review `ARCHITECTURE.md` for complete pipeline flow documentation
- Set up monitoring and alerts for your new entity
