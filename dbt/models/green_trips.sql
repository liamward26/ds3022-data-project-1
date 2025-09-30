{{ config(materialized='table') }}

-- Transform green taxi trips with CO2 calculations, speed metrics, and time extractions
SELECT 
    g.VendorID, 
    g.lpep_pickup_datetime, 
    g.lpep_dropoff_datetime, 
    g.passenger_count, 
    g.trip_distance, 
    l.vehicle_type, 
    l.co2_grams_per_mile,
    -- Calculate total CO2 output per trip by multiplying trip_distance * co2_grams_per_mile / 1000 (to get Kg)
    (g.trip_distance * l.co2_grams_per_mile) / 1000 AS trip_co2_kgs,
    -- Calculate average miles per hour based on distance divided by trip duration
    CASE 
        WHEN EXTRACT(EPOCH FROM (g.lpep_dropoff_datetime - g.lpep_pickup_datetime)) / 3600 > 0 
        THEN g.trip_distance / (EXTRACT(EPOCH FROM (g.lpep_dropoff_datetime - g.lpep_pickup_datetime)) / 3600)
        ELSE 0 
    END AS avg_mph,
    -- Extract the HOUR of the day from pickup_time (0-23)
    EXTRACT(HOUR FROM g.lpep_pickup_datetime) AS hour_of_day,
    -- Extract the DAY OF WEEK from pickup_time (1=Monday, 7=Sunday)
    EXTRACT(DOW FROM g.lpep_pickup_datetime) AS day_of_week,
    -- Extract the WEEK NUMBER from pickup_time (1-53)
    EXTRACT(WEEK FROM g.lpep_pickup_datetime) AS week_of_year,
    -- Extract the MONTH from pickup_time (1-12)
    EXTRACT(MONTH FROM g.lpep_pickup_datetime) AS month_of_year
    -- EXTRACT the YEAR from pickup_time (e.g., 2015, 2016, ..., 2024) this isnt used but if we were able to load the 10 years of data it would be useful
    -- EXTRACT(YEAR FROM g.lpep_pickup_datetime) AS year_of_data
FROM {{ source('raw_data', 'green_trips') }} g
-- Join with vehicle emissions lookup table to get CO2 data for green vehicles
JOIN {{ source('raw_data', 'taxi_lookup') }} l ON l.vehicle_type = 'green_taxi'
-- Filter out trips with zero or negative duration to ensure valid speed calculations
WHERE EXTRACT(EPOCH FROM (g.lpep_dropoff_datetime - g.lpep_pickup_datetime)) / 3600 > 0