CREATE DATABASE IF NOT EXISTS weather_db;
USE weather_db;
CREATE TABLE IF NOT EXISTS weather_readings (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,  -- Primary key, auto-generated
    station_id BIGINT NOT NULL,            -- ID of the weather station
    sequence_number BIGINT NOT NULL,       -- Auto-incremental per message
    battery_status VARCHAR(10) NOT NULL,   -- Battery level (low/medium/high)
    timestamp BIGINT NOT NULL,             -- Unix timestamp of reading
    humidity INT NOT NULL,                 -- Humidity percentage
    temperature INT NOT NULL,              -- Temperature in Fahrenheit
    wind_speed INT NOT NULL                -- Wind speed in km/h
);



