from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

def apply_transformation(df, mapping_config):
    """
    Applies business transformations to the input DataFrame based on the given mapping configuration.
    
    Args:
        df (pyspark.sql.DataFrame): Input DataFrame.
        mapping_config (dict): Dictionary containing mapping configuration (e.g., entity code).
    
    Returns:
        pyspark.sql.DataFrame: Transformed DataFrame.
    """
    
    entity = mapping_config.get("entity", "")
    
    df = (
        df
        .withColumn("ExternalId", F.concat_ws("", F.lit(entity), F.lit("E"), F.col("Code")))/
        .withColumn("Name",
                        F.when(F.col("Name").isNotNull() & (F.col("Name") != ""), F.col("Name"))
                        .otherwise(F.concat_ws("", F.lit(entity), F.lit("E"), F.col("Code")))
                    )/
        .withColumn("TaxRegNumber",
                    F.when(F.col("TaxRegNumber").isNotNull() & (F.col("TaxRegNumber") != ""),
                        F.expr("substring(TaxRegNumber, 1, 20)"))
                    .otherwise(F.lit(None))
            return df_final

'''

Lookups:

    # PaymentTerms = lookup global.terms by 'description' = PaymentTerms, get 'internal_id'
    # Subsidiary = lookup global.terms by 'description' = PaymentTerms, get 'internal_id'
    # Assuming global_terms_df is a dataframe with columns: description, internal_id


'''