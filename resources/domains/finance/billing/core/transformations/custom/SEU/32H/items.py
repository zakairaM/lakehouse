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
        # itemCode = "32H-E" + "ITM" + itemCode
        .withColumn("itemCode", F.concat_ws("-", F.lit(entity), F.lit("E"), F.lit("ITM"), F.col("itemCode")))\
        
        # Entity = "32H-E"
        .withColumn("Entity",  F.concat_ws("-", F.lit(entity), F.lit("E"))\
        
        # Subsidiary = lookup('global.subsidiaries', 'name':'32H')['internal_id'] = 30
        .withColumn("Subsidiary", F.lit(30))
        )
        return df_final

'''

Lookups:

after apply mapping!
#withColumn Subsidiary lookup('global.subsidiaries'['internal_id']

'''