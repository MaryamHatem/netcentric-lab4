-- Battery status distribution per station
-- Confirms the 30% / 40% / 30% distribution

SELECT
    station_id,
    battery_status,
    COUNT(*) AS count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY station_id), 2) AS percentage
FROM weather_readings
GROUP BY station_id, battery_status
ORDER BY station_id, battery_status;

-- Dropped messages per station
-- Compares expected vs received message counts
-- Since s_no increments every second even when messages are dropped,
-- the gap between max-min+1 and actual count is the number of dropped messages

SELECT
    station_id,
    MIN(sequence_number) AS first_seq,
    MAX(sequence_number) AS last_seq,
    (MAX(sequence_number) - MIN(sequence_number) + 1) AS expected_count,
    COUNT(*) AS received_count,
    (MAX(sequence_number) - MIN(sequence_number) + 1) - COUNT(*) AS dropped_count,
    ROUND(100.0 * ((MAX(sequence_number) - MIN(sequence_number) + 1) - COUNT(*))
          / (MAX(sequence_number) - MIN(sequence_number) + 1), 2) AS drop_percentage
FROM weather_readings
GROUP BY station_id
ORDER BY station_id;

-- Latest weather status per station
-- Queries the latest reading directly from the database

SELECT DISTINCT ON (station_id)
    station_id,
    battery_status,
    timestamp,
    humidity,
    temperature,
    wind_speed
FROM weather_readings
ORDER BY station_id, timestamp DESC;

-- Total readings per station
-- Helps verify all 10 stations are sending data

SELECT
    station_id,
    COUNT(*) AS total_readings,
    MIN(timestamp) AS first_reading,
    MAX(timestamp) AS last_reading
FROM weather_readings
GROUP BY station_id
ORDER BY station_id;

-- Raining alerts per station (bonus)
-- Counts how many times each station triggered a raining alert

SELECT
    station_id,
    COUNT(*) AS raining_alerts_count
FROM raining_alerts
GROUP BY station_id
ORDER BY station_id;

