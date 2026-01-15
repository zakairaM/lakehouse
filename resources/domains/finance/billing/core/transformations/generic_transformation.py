"""
Generic transformation engine for Bronze layer billing data processing.

This module provides contract-driven data processing for Bronze layer:
- Parse YAML contracts (source_column + source_type only)
- Build PySpark schemas from contracts
- Apply basic column mapping and type casting

Note: Target columns and DWH transformations are handled in Silver layer.
"""

import yaml
from typing import Dict, List, Any
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, 
    TimestampType, BooleanType, DateType
)


class ContractTransformationEngine:
    """Transformation engine for Bronze layer - processes raw data based on contract definitions."""
    
    # Backwards-compatibility: some callers still use parse_contract_csv
    # Delegate to YAML parser to avoid breaking existing imports
    def parse_contract_csv(self, contract_path: str) -> List[Dict[str, Any]]:
        return self.parse_contract_yaml(contract_path)
    
    def parse_contract_yaml(self, contract_path: str) -> List[Dict[str, Any]]:
        """
        Parse YAML contract file into a list of column mapping dictionaries.
        
        Bronze layer uses only source_column and source_type.
        target_column and target_type are ignored (used in Silver layer).
        
        Args:
            contract_path: Path to the contract YAML file
            
        Returns:
            List of dictionaries containing column mapping information
        """
        try:
            with open(contract_path, 'r') as f:
                contract_data = yaml.safe_load(f)
            
            # Extract mappings from YAML structure
            mappings = contract_data.get('mappings', [])
            contract_mappings = []
            
            for mapping in mappings:
                source_col = mapping.get('source_column', '')
                source_type = mapping.get('source_type', 'string')
                
                # Skip if source_column is empty
                if not source_col or not str(source_col).strip():
                    continue
                
                # Bronze keeps source column names (no DWH transformation)
                contract_mappings.append({
                    'source_column': source_col,
                    'target_column': source_col,  # Bronze keeps source name
                    'source_type': source_type,
                    'target_type': source_type,  # Use source_type for Bronze
                    'required': mapping.get('required', False)
                })
            
            return contract_mappings
            
        except Exception as e:
            raise Exception(f"Error parsing contract YAML {contract_path}: {str(e)}")
    
    def build_schema_from_contract(self, contract_path: str) -> StructType:
        """
        Build PySpark schema from contract definition.
        
        Uses source_column names and source_type for Bronze layer.
        
        Args:
            contract_path: Path to the contract YAML file
            
        Returns:
            PySpark StructType schema
        """
        contract_mappings = self.parse_contract_yaml(contract_path)
        
        fields = []
        for mapping in contract_mappings:
            source_column = mapping.get('source_column', '')
            source_type = mapping.get('source_type', 'string')
            
            # Skip if column name is empty
            if not source_column or not str(source_column).strip():
                continue
            
            # Map data types
            spark_type = self._map_data_type(source_type)
            nullable = not mapping.get('required', False)
            
            fields.append(StructField(source_column, spark_type, nullable))
        
        return StructType(fields)


    def build_schema_from_config(self, schema: list) -> StructType:
        """
        Build PySpark StructType schema from a YAML schema list.

        Args:
            schema: List of dictionaries containing column definitions, e.g.:
                [
                    {"name": "column1", "type": "string", "required": True},
                    {"name": "column2", "type": "double", "required": False},
                    ...
                ]

        Returns:
            StructType: PySpark schema
        """
        if not schema:
            raise ValueError("No schema provided for headerless source.")

        fields = []
        for col in schema:
            col_name = col.get("name")
            col_type = col.get("type", "string")
            required = col.get("required", False)

            if not col_name:
                continue

            spark_type = self._map_data_type(col_type)
            fields.append(StructField(col_name, spark_type, nullable=not required))

        return StructType(fields)

    
    def _map_data_type(self, data_type: str) -> Any:
        """Map contract data types to PySpark data types."""
        type_lower = data_type.lower() if data_type else 'string'
        
        # Handle complex types like decimal(10,4)
        if 'decimal' in type_lower:
            return DoubleType()
        elif type_lower == 'date':
            return DateType()
        elif 'timestamp' in type_lower or 'datetime' in type_lower:
            return TimestampType()
        elif type_lower in ['int', 'integer', 'long']:
            return IntegerType()
        elif type_lower in ['boolean', 'bool']:
            return BooleanType()
        elif 'double' in type_lower:
            return DoubleType()
        else:
            return StringType()
    
    def apply_contract_mapping(self, df: DataFrame, contract_mappings: List[Dict[str, Any]]) -> DataFrame:
        """
        Apply column mappings and type casting based on contract.
        
        Bronze layer: reads source columns and casts to source_type.
        No column renaming or DWH transformation - that happens in Silver.
        
        Args:
            df: Input DataFrame from raw data
            contract_mappings: List of contract mapping dictionaries
            
        Returns:
            DataFrame with columns mapped and typed according to contract
        """
        select_exprs = []
        
        for mapping in contract_mappings:
            source_column = mapping.get('source_column', '')
            source_type = mapping.get('source_type', 'string')
            
            # Skip if source_column is empty
            if not source_column or not str(source_column).strip():
                continue
            
            # Get column from source or create NULL
            if source_column in df.columns:
                col_expr = F.col(source_column)
            else:
                col_expr = F.lit(None)
            
            # Cast to source_type
            spark_type = self._map_data_type(source_type)
            col_expr = col_expr.cast(spark_type)
            
            # Keep source column name in Bronze (no renaming)
            select_exprs.append(col_expr.alias(source_column))
        
        # Return DataFrame with mapped columns
        if select_exprs:
            return df.select(*select_exprs)
        else:
            return df


# Convenience functions for direct use
def parse_contract_csv(contract_path: str) -> List[Dict[str, Any]]:
    """Parse contract YAML file into mapping dictionaries (legacy name, now parses YAML)."""
    engine = ContractTransformationEngine()
    return engine.parse_contract_yaml(contract_path)


def apply_contract_mapping(df: DataFrame, contract_mapping: List[Dict[str, Any]]) -> DataFrame:
    """Apply contract mapping to DataFrame."""
    engine = ContractTransformationEngine()
    return engine.apply_contract_mapping(df, contract_mapping)


def build_schema_from_contract(contract_path: str) -> StructType:
    """Build PySpark schema from contract."""
    engine = ContractTransformationEngine()
    return engine.build_schema_from_contract(contract_path)

def build_schema_from_contract(source_config_path: str) -> StructType:
    """Build PySpark schema from source config file."""
    engine = ContractTransformationEngine()
    return engine.build_schema_from_contract(source_config_path)
