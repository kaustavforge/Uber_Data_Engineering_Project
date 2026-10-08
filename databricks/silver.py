from pyspark import pipelines as dp
from pyspark.sql.functions import col, from_json

RIDES_DDL = "ride_id STRING, confirmation_number STRING, passenger_id STRING, driver_id STRING, vehicle_id STRING, pickup_location_id STRING, dropoff_location_id STRING, vehicle_type_id BIGINT, vehicle_make_id BIGINT, payment_method_id BIGINT, ride_status_id BIGINT, pickup_city_id BIGINT, dropoff_city_id BIGINT, cancellation_reason_id BIGINT, passenger_name STRING, passenger_email STRING, passenger_phone STRING, driver_name STRING, driver_rating DOUBLE, driver_phone STRING, driver_license STRING, vehicle_model STRING, vehicle_color STRING, license_plate STRING, pickup_address STRING, pickup_latitude DOUBLE, pickup_longitude DOUBLE, dropoff_address STRING, dropoff_latitude DOUBLE, dropoff_longitude DOUBLE, distance_miles DOUBLE, duration_minutes BIGINT, booking_timestamp STRING, pickup_timestamp STRING, dropoff_timestamp STRING, base_fare DOUBLE, distance_fare DOUBLE, time_fare DOUBLE, surge_multiplier DOUBLE, subtotal DOUBLE, tip_amount DOUBLE, total_fare DOUBLE, rating DOUBLE"

# Empty Streaming Table
dp.create_streaming_table("uber.silver.stg_rides")


# Bulk/Initial Load
@dp.append_flow(target="uber.silver.stg_rides")
def rides_bulk():
    df = spark.readStream.table("uber.bronze.bulk_rides")
    df = df.withColumn("booking_timestamp", col("booking_timestamp").cast("timestamp"))
    return df


# Streaming Load
@dp.append_flow(target="uber.silver.stg_rides")
def rides_stream():
    df = spark.readStream.table("uber.bronze.rides_raw")
    df_parsed = (
        df.select(from_json(col("rides"), RIDES_DDL).alias("parsed_rides"))
          .select("parsed_rides.*")
          .withColumn("booking_timestamp", col("booking_timestamp").cast("timestamp"))
    )
    return df_parsed
