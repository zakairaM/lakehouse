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
    
    # Apply transformations
    df = (
        df
        # CustomerCode = entity + 'CUS' + CustomerCode
        .withColumn("CustomerCode", F.concat_ws("", F.lit(entity), F.lit("CUS"), F.col("CustomerCode")))\
        
        # ExternalId = entity + 'INV' + ADCode
        .withColumn("ExternalId", F.concat_ws("-", F.lit(entity), F.lit("E"), F.lit("INV"), F.col("ADCode")))\
        
        # ServiceCode = entity + 'ITM' + servicecode
        .withColumn("ServiceCode", F.concat_ws("-", F.lit(entity), F.lit("E"), F.lit("ITM"), F.col("servicecode")))\
        
        # Rate = round(CurrencyNetValue / Quantity, 2)
        .withColumn("Rate", F.round(F.col("CurrencyNetValue") / F.col("Quantity"), 2))\
        
        # ExchangeRate = 1
        .withColumn("ExchangeRate", F.lit(1))\
        
        # ContractEndDate = add_months(ADRegistrationDate, BillingCycle)
        .withColumn(
            "ContractEndDate",
            F.add_months(F.col("ADRegistrationDate"), F.col("BillingCycle").cast("int"))\
        )
        
        # RevRecStartDate = trunc(ADRegistrationDate, "month")
        .withColumn("RevRecStartDate", F.trunc(F.col("ADRegistrationDate"), "month"))\
        
        # RevRecEndDate logic:
        # if BillingCycle > 0:
        #     date_sub(trunc(add_months(ADRegistrationDate, BillingCycle), "month"), 1)
        # else:
        #     trunc(ADRegistrationDate, "month")
        .withColumn(
            "RevRecEndDate",
            F.when(
                F.col("BillingCycle") > 0,
                F.date_sub(
                    F.trunc(
                        F.add_months(F.col("ADRegistrationDate"), F.col("BillingCycle").cast("int")),
                        "month"
                    ),
                    1
                )
            ).otherwise(F.trunc(F.col("ADRegistrationDate"), "month"))
        )
    )
    
    # Add custom column (as mentioned: custcol_nb2_invbillsysitemnum)
    df_final = df.withColumn("custcol_nb2_invbillsysitemnum", F.col("ServiceCode"))
    
    return df_final

'''
TRANSFORMATIONS



Lookups:

after apply mapping!
withcolumn CustProfileCode/itemlist.taxcode"  #?#"Tax Code ID"#lookup 'global.tax_codes_mapping'
withColumn Currency lookup('global.currency','iso_code':'EUR')['internal_id']
#withCOlumn PaymentTerms lookup('global.#withCOlumn PaymentTerms lookup('global.terms', 'description':PaymentTerms ['internal_id']', 'description':PaymentTerms ['internal_id']
#withColumn Subsidiary lookup('global.subsidiaries'['internal_id']

'''