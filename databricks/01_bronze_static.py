# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Setup + static bronze tables
# MAGIC Layers: `uber.raw` (landing files), `uber.bronze`, `uber.silver`, `uber.gold`. Every pipeline table is written with its full 3-part name.
# MAGIC Run the first 2 code cells, upload `data/*.json` to the Volume's `static/` folder, then run the rest.

# COMMAND ----------

try:
    spark.sql("CREATE CATALOG IF NOT EXISTS uber")
except Exception as e:
    print("Could not create catalog 'uber' (maybe blocked on Free Edition):", str(e)[:200])

spark.sql("CREATE SCHEMA IF NOT EXISTS uber.raw")
spark.sql("CREATE SCHEMA IF NOT EXISTS uber.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS uber.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS uber.gold")
spark.sql("CREATE VOLUME IF NOT EXISTS uber.raw.landing")

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/uber/raw/landing/static")
dbutils.fs.mkdirs("/Volumes/uber/raw/landing/rides_stream")

# COMMAND ----------

# MAGIC %md ## (Optional) Download the static files from GitHub instead of uploading by hand
# MAGIC This is the "GitHub" box of the architecture. Push this project to a **public** GitHub repo first,
# MAGIC set `GITHUB_RAW_BASE` below, and run the cell. If you already uploaded `data/*.json` yourself, skip it.

# COMMAND ----------

import urllib.request

# change <your-username> and <your-repo>; branch is "main"; the folder is the repo's data/ folder
GITHUB_RAW_BASE = "https://raw.githubusercontent.com/<your-username>/<your-repo>/main/data"

static_files = [
    "bulk_rides",
    "map_cities",
    "map_cancellation_reasons",
    "map_payment_methods",
    "map_ride_statuses",
    "map_vehicle_makes",
    "map_vehicle_types",
]

if "<your-username>" in GITHUB_RAW_BASE:
    print("Set GITHUB_RAW_BASE first (or skip this cell and upload data/*.json by hand).")
else:
    for name in static_files:
        data = urllib.request.urlopen(f"{GITHUB_RAW_BASE}/{name}.json", timeout=60).read()
        with open(f"/Volumes/uber/raw/landing/static/{name}.json", "wb") as f:
            f.write(data)
        print(f"downloaded {name}.json ({len(data):,} bytes)")

# COMMAND ----------

# MAGIC %md ## Map tables  (replaces his pandas + Blob SAS-URL loop)

# COMMAND ----------

from pyspark.sql import functions as F

files = [
    {"file": "map_cities"},
    {"file": "map_cancellation_reasons"},
    {"file": "map_payment_methods"},
    {"file": "map_ride_statuses"},
    {"file": "map_vehicle_makes"},
    {"file": "map_vehicle_types"},
]

for f in files:
    df = spark.read.option("multiLine", "true").json(f"/Volumes/uber/raw/landing/static/{f['file']}.json")
    if f["file"] == "map_cities":
        df = df.withColumn("updated_at", F.col("updated_at").cast("timestamp"))  # used by SCD2 dim_location
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"uber.bronze.{f['file']}"))

# COMMAND ----------

# MAGIC %md ## bulk_rides (historical load, runs only once)

# COMMAND ----------

RIDES_DDL = "ride_id STRING, confirmation_number STRING, passenger_id STRING, driver_id STRING, vehicle_id STRING, pickup_location_id STRING, dropoff_location_id STRING, vehicle_type_id BIGINT, vehicle_make_id BIGINT, payment_method_id BIGINT, ride_status_id BIGINT, pickup_city_id BIGINT, dropoff_city_id BIGINT, cancellation_reason_id BIGINT, passenger_name STRING, passenger_email STRING, passenger_phone STRING, driver_name STRING, driver_rating DOUBLE, driver_phone STRING, driver_license STRING, vehicle_model STRING, vehicle_color STRING, license_plate STRING, pickup_address STRING, pickup_latitude DOUBLE, pickup_longitude DOUBLE, dropoff_address STRING, dropoff_latitude DOUBLE, dropoff_longitude DOUBLE, distance_miles DOUBLE, duration_minutes BIGINT, booking_timestamp STRING, pickup_timestamp STRING, dropoff_timestamp STRING, base_fare DOUBLE, distance_fare DOUBLE, time_fare DOUBLE, surge_multiplier DOUBLE, subtotal DOUBLE, tip_amount DOUBLE, total_fare DOUBLE, rating DOUBLE"

if not spark.catalog.tableExists("uber.bronze.bulk_rides"):
    df = (spark.read.schema(RIDES_DDL)
          .option("multiLine", "true")
          .json("/Volumes/uber/raw/landing/static/bulk_rides.json"))
    df.write.format("delta").mode("overwrite").saveAsTable("uber.bronze.bulk_rides")
    print("This will not run more than 1 time")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM uber.bronze.map_cities

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM uber.bronze.rides_raw
