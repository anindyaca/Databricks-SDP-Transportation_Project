from pyspark.sql.functions import *
from pyspark import pipelines as dp

#Config
SOURCE_PATH = "s3://ranjita-s3/data-store/city"

@dp.materialized_view(
    name = "city_bronze"
)

def city_bronze():
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "corrupt_record")
        .load(SOURCE_PATH)
    )

    df = df.withColumn("file_name",col("_metadata.file_path")).withColumn("ingestion_time",current_timestamp())

    return df