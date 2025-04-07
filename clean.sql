START TRANSACTION;


-- =====================
-- Insert flight records
-- =====================
WITH inserted_flights AS (
    SELECT
        id AS raw_id,
        state ->> 0 AS icao24,
        NULLIF(TRIM(state ->> 1), '') AS callsign,
        state ->> 2 AS country,
        (state ->> 5)::DOUBLE PRECISION AS longitude,
        (state ->> 6)::DOUBLE PRECISION AS latitude,
        NULLIF((state ->> 7), 'null')::DOUBLE PRECISION AS altitude_meters,
        NULLIF((state ->> 9), 'null')::DOUBLE PRECISION AS velocity_knots,
        NULLIF((state ->> 10), 'null')::DOUBLE PRECISION AS heading_degrees,
        NULLIF((state ->> 11), 'null')::DOUBLE PRECISION AS vertical_rate,
        to_timestamp((raw_json ->> 'time')::BIGINT) AT TIME ZONE 'UTC' AS timestamp
    FROM flight_json_data,
         jsonb_array_elements(raw_json -> 'states') AS state
)
INSERT INTO flights (
    icao24,
    callsign,
    country,
    longitude,
    latitude,
    altitude_meters,
    velocity_knots,
    heading_degrees,
    vertical_rate,
    timestamp
)
SELECT
    icao24, callsign, country, longitude, latitude,
    altitude_meters, velocity_knots,
    heading_degrees, vertical_rate, timestamp
FROM inserted_flights;

DELETE FROM flight_json_data
WHERE id IN (
    SELECT id
    FROM flight_json_data,
         jsonb_array_elements(raw_json -> 'states') AS state
);

-- =====================
-- Insert weather records
-- =====================

INSERT INTO weather_observations (
    latitude,
    longitude,
    timestamp,
    precipitation_mm,
    weathercode
)
SELECT
    (raw_json ->> 'latitude')::DOUBLE PRECISION AS latitude,
    (raw_json ->> 'longitude')::DOUBLE PRECISION AS longitude,
    (raw_json -> 'current' ->> 'time')::TIMESTAMPTZ AS timestamp,
    NULL AS precipitation_mm,  -- not present in this JSON structure
    (raw_json -> 'current' ->> 'weathercode')::SMALLINT AS weathercode
FROM weather_json_data
WHERE raw_json ? 'current';

DELETE FROM weather_json_data
WHERE raw_json ? 'current';



COMMIT;
