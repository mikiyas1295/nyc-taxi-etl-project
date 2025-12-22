SELECT
    VendorID,
    PULocationID,
    DOLocationID,
    trip_distance,
    fare_amount,
    trip_duration_min,
    temperature,
    weather_category,
    pickup_date
FROM {{ source('taxi_source', 'taxi_weather') }}
