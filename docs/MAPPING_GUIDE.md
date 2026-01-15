# Mapping Guide

## 🗺️ Mapping (Source → Silver → NetSuite)

### Overview
This guide describes the simplified mapping model used by the Billing pipelines.

- Contracts define ONLY Source → NetSuite fields:
  - `source_column`, `source_type`
  - `target_column` (NetSuite camelCase), `target_type`
- The Silver layer automatically:
  - Converts `target_column` to snake_case for the DWH column name
  - Applies data types based on `target_type` (falls back to `source_type` if missing)
  - Supports schema evolution by unioning across sources with `unionByName(allowMissingColumns=True)`

We do NOT use `dwh_column` or `dwh_type` in contracts anymore.

---

## Contract Structure

Minimal fields required in each `contract.yaml`:

```yaml
document_type: "invoice"            # customer | invoice | item | payment
region: "nordics"                   # e.g., nordics, seu
entity_code: "15E"                  # company/entity code
version: "1.1"
description: "Invoice mapping for Nordics 15E"

mappings:
  - source_column: "invoice_id"
    source_type: "string"
    target_column: "Invoice.externalId"
    target_type: "string"

  - source_column: "currency_code"
    source_type: "string"
    target_column: "Invoice.currency"
    target_type: "RecordRef"
```

Notes:
- `target_column` follows NetSuite path conventions (camelCase and nested via dot if needed).
- For line items, use the NetSuite template path (e.g., `Invoice.itemList.item.quantity`).

---

## Silver Layer Naming and Types

### Snake_case conversion
The Silver layer converts every `target_column` into a DWH-friendly snake_case path. Examples:

- `Customer.externalId` → `customer.external_id`
- `Invoice.tranDate` → `invoice.tran_date`
- `Invoice.itemList.item.quantity` → `invoice.item_list.item.quantity`
- `billingAddress.country.value` → `billing_address.country.value`

Rules:
- CamelCase segments become snake_case (e.g., `tranDate` → `tran_date`).
- Dot notation is preserved to produce dotted DWH paths (flattened aliases).
- Custom and enum fields are preserved as given in the template (e.g., `@custentity_nb2_*`).

### Data type resolution
Silver chooses the Spark type using this priority:
1. `target_type` (string | int | decimal | date | boolean | RecordRef | timestamp)
2. Fallback: `source_type`

Implementation notes:
- Decimals may be stored as string for compatibility; cast downstream as needed.
- `RecordRef` is stored as string identifier (internalId or externalId) as provided by the mapping.

---

## Nested fields and templates

NetSuite templates (CSV) define the full expected schema. The loader recursively resolves nested objects:

- If a field type is an object (e.g., `billingAddress: Address`), the loader expands fields from `Address.csv`.
- If an expanded field is itself a typed object (e.g., `country: Country`), the loader expands `Country.csv`.
- Resulting flattened DWH path uses dot notation and snake_case, e.g., `billing_address.country.value`.

These dotted columns exist in Silver so the NetSuite integration can build JSON payloads using the same paths.

---

## Examples

### Customer
```yaml
document_type: "customer"
region: "nordics"
entity_code: "15E"

mappings:
  - source_column: "customer_id"
    source_type: "string"
    target_column: "Customer.externalId"
    target_type: "string"

  - source_column: "first_name"
    source_type: "string"
    target_column: "Customer.firstName"
    target_type: "string"

  - source_column: "15E"
    source_type: "string"
    target_column: "Customer.subsidiary"
    target_type: "RecordRef"
```

Silver columns produced (examples):
- `customer.external_id`, `customer.first_name`, `customer.subsidiary`

### Invoice (header and lines)
```yaml
document_type: "invoice"
region: "nordics"
entity_code: "15E"

mappings:
  # Header
  - source_column: "invoice_id"
    source_type: "string"
    target_column: "Invoice.externalId"
    target_type: "string"

  - source_column: "tran_date"
    source_type: "date"
    target_column: "Invoice.tranDate"
    target_type: "date"

  - source_column: "currency"
    source_type: "string"
    target_column: "Invoice.currency"
    target_type: "RecordRef"

  # Lines
  - source_column: "quantity"
    source_type: "string"
    target_column: "Invoice.itemList.item.quantity"
    target_type: "decimal"

  - source_column: "item_external_id"
    source_type: "string"
    target_column: "Invoice.itemList.item.item"
    target_type: "RecordRef"
```

Silver columns produced (examples):
- `invoice.external_id`, `invoice.tran_date`, `invoice.currency`
- `invoice.item_list.item.quantity`, `invoice.item_list.item.item`

---

## Testing the mapping (interactive)

Use a notebook to validate mappings against a Bronze table before/after contract changes:

```python
bronze = spark.read.table("teamblue_finance.bronze.billing_nordics_15e_invoice")
print("bronze.count() =", bronze.count())

region, entity_code = "nordics", "15E"
mappings = contract_loader.get_silver_mappings(region, entity_code, "invoice")

df_mapped = apply_bronze_to_silver_mapping(bronze, mappings, region, entity_code)
print("mapped.count() =", df_mapped.count())
print("mapped.columns (first 20) =", df_mapped.columns[:20])
df_mapped.display()
```

If the count is 0 or columns look wrong:
- Verify the contract path and the `document_type/region/entity_code`
- Confirm `target_column` names from the NetSuite templates
- Check the loader logs for contract discovery issues

---

## Troubleshooting

- Bronze table exists but Silver is empty
  - Ensure fully-qualified reads in Silver: `dlt.read("teamblue_finance.bronze.<table>")`
  - Remove row-dropping expectations while contracts are incomplete

- Contracts not found in Workspace
  - The contract discovery uses `os.walk` for `/Workspace/...` paths and `dbutils.fs.ls` for DBFS.
  - Confirm the base path printed by the loader and that `contracts/` exists under it.

- Fields missing across companies
  - The union logic uses `allowMissingColumns=True`; columns appear when they exist in any source.

---

## References

- NetSuite templates: `resources/domains/finance/billing/core/config/netsuite_templates/`
- Contracts: `resources/domains/finance/billing/core/contracts/`
- DLT multi-schema publishing: Databricks Blog “Publish to Multiple Catalogs and Schemas from a Single DLT Pipeline”

### Mapping Fields Explained

#### Source Fields
- **`source_column`**: Column name in source data
- **`source_type`**: Data type in source (string, int, decimal, date, boolean)

#### Target Fields
- `target_column`: NetSuite field path (camelCase; nested allowed)
- `target_type`: NetSuite data type (string, int, decimal, date, boolean, RecordRef)

### Field Mapping Examples

#### Customer Mapping
```yaml
mappings:
  # Basic fields
  - source_column: "customer_id"
    source_type: "string"
    target_column: "Customer.externalId"
    target_type: "string"

  # Name fields (15E has separate first/last name)
  - source_column: "first_name"
    source_type: "string"
    target_column: "Customer.firstName"
    target_type: "string"

  # Name fields (32H has combined name)
  - source_column: "full_name"
    source_type: "string"
    target_column: "Customer.firstName"
    target_type: "string"

  # Reference fields
  - source_column: "15E"
    source_type: "string"
    target_column: "Customer.subsidiary"
    target_type: "RecordRef"
```

#### Invoice Mapping
```yaml
mappings:
  # Header fields
  - source_column: "invoice_id"
    source_type: "string"
    target_column: "Invoice.externalId"
    target_type: "string"

  # Line item fields
  - source_column: "item_id"
    source_type: "string"
    target_column: "InvoiceItem.item"
    target_type: "RecordRef"

  - source_column: "quantity"
    source_type: "string"
    target_column: "InvoiceItem.quantity"
    target_type: "decimal"
```

### NetSuite Configuration

#### Object Types
- **Customer**: `object_type: "Customer"`
- **Invoice**: `object_type: "Invoice"`
- **Item**: `object_type: "Item"`
- **Payment**: `object_type: "Payment"`

#### Required Fields
```yaml
netsuite_config:
  required_fields:
    - "Customer.externalId"
    - "Customer.entityId"
    - "Customer.subsidiary"
```

#### Line Items (for Invoice)
```yaml
netsuite_config:
  line_items:
    object_type: "InvoiceItem"
    list_field: "Invoice.itemList"
    replace_all: true
```

### Lookup Tables

#### Subsidiary Mapping
```yaml
lookups:
  subsidiary_map:
    "15E": 36    # Nordics subsidiary ID
    "32H": 37    # SEU subsidiary ID
```

#### Currency Mapping
```yaml
lookups:
  currency_map:
    "SEK": 1     # Swedish Krona
    "EUR": 2     # Euro
    "USD": 3     # US Dollar
    "GBP": 4     # British Pound
```

#### Item Type Mapping
```yaml
lookups:
  item_type_map:
    "service": 1        # Service Item
    "inventory": 2      # Inventory Item
    "non_inventory": 3  # Non-Inventory Item
```

### Creating New Contracts

#### Step 1: Create Directory Structure
```
resources/domains/finance/billing/core/contracts/
└── {region}/
    └── {entity_code}/
        └── {document_type}/
            └── contract.yaml
```

#### Step 2: Define Mapping
1. Identify source columns from Bronze data
2. For each field, set `target_column` (NetSuite path) and `target_type` (optional)
3. Keep names in camelCase; Silver will generate snake_case automatically

#### Step 3: Configure NetSuite
1. Set `object_type` and `operation`
2. Define `required_fields` if applicable
3. Set `batch_size` where needed

#### Step 4: Test Mapping
```python
# Validate one document type interactively
bronze = spark.read.table("teamblue_finance.bronze.billing_nordics_15e_invoice")
mappings = contract_loader.get_silver_mappings("nordics", "15E", "invoice")
df_mapped = apply_bronze_to_silver_mapping(bronze, mappings, "nordics", "15E")
df_mapped.display()
```

### Common Mapping Patterns

#### Date Fields
```yaml
- source_column: "invoice_date"
  source_type: "string"
  target_column: "Invoice.tranDate"
  target_type: "date"
```

#### Decimal Fields
```yaml
- source_column: "amount"
  source_type: "string"
  target_column: "InvoiceItem.amount"
  target_type: "decimal"
```

#### Boolean Fields
```yaml
- source_column: "is_company"
  source_type: "string"
  target_column: "Customer.isPerson"
  target_type: "boolean"
```

#### Reference Fields
```yaml
- source_column: "customer_id"
  source_type: "string"
  target_column: "Invoice.entity"
  target_type: "RecordRef"
```

### Validation Rules

#### Required Fields
- Contract must have: `document_type`, `region`, `entity_code`
- Each mapping must have: `source_column`, `source_type`, `target_column` (optional `target_type`)

#### Data Types
- Supported: `string`, `int`, `decimal`, `date`, `boolean`, `RecordRef`, `timestamp`

### Troubleshooting

#### Issue: Mapping not applied
**Check**: Contract file exists and is valid YAML
**Solution**: Validate YAML syntax and required fields

#### Issue: NetSuite field not found
**Check**: Field path is correct (e.g., "Customer.externalId")
**Solution**: Verify NetSuite object structure

#### Issue: Data type mismatch
**Check**: Source and DWH data types match
**Solution**: Update mapping with correct types

#### Issue: Lookup not found
**Check**: Lookup table exists in contract
**Solution**: Add missing lookup mappings

---

## 📚 Additional Resources
- **NetSuite API Documentation**: [NetSuite Web Services](https://docs.oracle.com/en/cloud/saas/netsuite/)
- **Contract Examples**: See existing contracts in `resources/domains/finance/billing/core/contracts/`
- **Transformation Examples**: See `resources/domains/finance/billing/core/transformations/`
