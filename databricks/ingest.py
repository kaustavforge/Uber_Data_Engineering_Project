from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp


# Ansh read Azure Event Hubs here. We read the NDJSON files that Kafka -> MinIO -> Volume delivered.
# Output keeps his shape: a column `rides` holding the raw JSON string.
@dp.table(name="uber.bronze.rides_raw")
def rides_raw():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")            # one JSON event per line
        .load("/Volumes/uber/raw/landing/rides_stream/")
    )

    df = df.select(
        col("value").alias("rides"),
        col("_metadata.file_path").alias("source_file"),
        current_timestamp().alias("ingested_at"),
    )
    return df
