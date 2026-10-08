CREATE OR REFRESH STREAMING TABLE uber.silver.silver_obt
AS
SELECT
    stg_rides.*,
    map_vehicle_makes.vehicle_make,
    map_vehicle_types.vehicle_type,
    map_vehicle_types.description,
    map_vehicle_types.base_rate,
    map_vehicle_types.per_mile,
    map_vehicle_types.per_minute,
    map_ride_statuses.ride_status,
    map_payment_methods.payment_method,
    map_payment_methods.is_card,
    map_payment_methods.requires_auth,
    map_cities.city AS pickup_city,
    map_cities.state,
    map_cities.region,
    map_cities.updated_at AS city_updated_at,
    map_cancellation_reasons.cancellation_reason
FROM
    STREAM(uber.silver.stg_rides) WATERMARK booking_timestamp DELAY OF INTERVAL 3 MINUTES stg_rides
LEFT JOIN uber.bronze.map_vehicle_makes map_vehicle_makes
       ON stg_rides.vehicle_make_id = map_vehicle_makes.vehicle_make_id
LEFT JOIN uber.bronze.map_vehicle_types map_vehicle_types
       ON stg_rides.vehicle_type_id = map_vehicle_types.vehicle_type_id
LEFT JOIN uber.bronze.map_ride_statuses map_ride_statuses
       ON stg_rides.ride_status_id = map_ride_statuses.ride_status_id
LEFT JOIN uber.bronze.map_payment_methods map_payment_methods
       ON stg_rides.payment_method_id = map_payment_methods.payment_method_id
LEFT JOIN uber.bronze.map_cities map_cities
       ON stg_rides.pickup_city_id = map_cities.city_id
LEFT JOIN uber.bronze.map_cancellation_reasons map_cancellation_reasons
       ON stg_rides.cancellation_reason_id = map_cancellation_reasons.cancellation_reason_id
;