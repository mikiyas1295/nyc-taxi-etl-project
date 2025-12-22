SELECT
    PULocationID,
    COUNT(*) AS total_trips,
    AVG(trip_distance) AS avg_distance,
    AVG(fare_amount) AS avg_fare,
    AVG(trip_duration_min) AS avg_duration
FROM {{ ref('stg_taxi_weather') }}
GROUP BY PULocationID
