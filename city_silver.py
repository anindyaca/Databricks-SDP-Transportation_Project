from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark import pipelines as dp

@dp.materialized_view(
    name = "transportation.silver.city_silver"
)
def city_silver():
    df_bronze = spark.read.table("transportation.bronze.city_bronze")
    df_silver = df_bronze.select(
        F.col("city_id").alias("city_id"),
        F.col("city_name").alias("city_name"),
        F.col("ingestion_time").alias("bronze_ingestion_time")
    )
    df_silver = df_silver.withColumn(
        "silver_ingestion_time", F.current_timestamp()
    )
    return df_silver