from pyspark import pipelines as dp
from pyspark.sql.functions import col
from utilities import utils


# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.


@dp.table
def sample_trips_bronze_standard_loader_api():
    return (
        spark.read.table("samples.nyctaxi.trips")
        .withColumn("trip_distance_km", utils.distance_km(col("trip_distance")))
    )
