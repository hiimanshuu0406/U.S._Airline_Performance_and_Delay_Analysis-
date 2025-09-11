CREATE TABLE airlines (
   iata_code VARCHAR(10) PRIMARY KEY,
   airline_name VARCHAR(100)
);

CREATE TABLE airports (
   iata_code VARCHAR(10) PRIMARY KEY,
   airport_name VARCHAR(100),
   city VARCHAR(100),
   state VARCHAR(100),
   country VARCHAR(100),
   latitude DECIMAL(10,6),
   longitude DECIMAL(10,6)
);

CREATE TABLE flights (
    year INT,
    month INT,
    day INT,
    day_of_week INT,
    airline VARCHAR(10),
    flight_number INT,
    tail_number VARCHAR(20),
    origin_airport VARCHAR(10),
    destination_airport VARCHAR(10),
    scheduled_departure INT,
    departure_time INT,
    departure_delay INT,
    taxi_out INT,
    wheels_off INT,
    scheduled_time INT,
    elapsed_time INT,
    air_time INT,
    distance INT,
    wheels_on INT,
    taxi_in INT,
    scheduled_arrival INT,
    arrival_time INT,
    arrival_delay INT,
    diverted INT,
    cancelled INT,
    cancellation_reason CHAR(1),
    air_system_delay INT,
    security_delay INT,
    airline_delay INT,
    late_aircraft_delay INT,
    weather_delay INT
);
ALTER TABLE flights ADD COLUMN flight_date DATE;

UPDATE flights
SET flight_date = TO_DATE(CONCAT(year, '-', month, '-', day), 'YYYY-MM-DD');

SELECT year, month, day, flight_date 
FROM flights 
LIMIT 10;

ALTER TABLE flights ADD COLUMN scheduled_departure_time TIME;

UPDATE flights
SET scheduled_departure_time = 
    MAKE_TIME(
        FLOOR(scheduled_departure / 100)::INT,
        MOD(scheduled_departure, 100)::INT,
        0
    )
WHERE scheduled_departure IS NOT NULL;

SELECT scheduled_departure, scheduled_departure_time 
FROM flights 
WHERE scheduled_departure IS NOT NULL
LIMIT 10;

ALTER TABLE flights ADD COLUMN departure_time_formatted TIME;

UPDATE flights
SET departure_time_formatted = 
    MAKE_TIME(
        FLOOR(departure_time / 100)::INT,
        MOD(departure_time, 100)::INT,
        0
    )
WHERE departure_time IS NOT NULL;

SELECT departure_time, departure_time_formatted
FROM flights
WHERE departure_time IS NOT NULL
LIMIT 10;

ALTER TABLE flights ADD COLUMN scheduled_arrival_time TIME;

UPDATE flights
SET scheduled_arrival_time = 
    MAKE_TIME(
        FLOOR(scheduled_arrival / 100)::INT,
        MOD(scheduled_arrival, 100)::INT,
        0
    )
WHERE scheduled_arrival IS NOT NULL;

SELECT scheduled_arrival, scheduled_arrival_time
FROM flights
WHERE scheduled_arrival IS NOT NULL
LIMIT 10;

ALTER TABLE flights ADD COLUMN arrival_time_formatted TIME;

UPDATE flights
SET arrival_time_formatted = 
    MAKE_TIME(
        FLOOR(arrival_time / 100)::INT,
        MOD(arrival_time, 100)::INT,
        0
    )
WHERE arrival_time IS NOT NULL;

SELECT arrival_time, arrival_time_formatted
FROM flights
WHERE arrival_time IS NOT NULL
LIMIT 10;

ALTER TABLE flights ADD COLUMN wheels_off_time TIME;

UPDATE flights
SET wheels_off_time = 
    MAKE_TIME(
        FLOOR(wheels_off / 100)::INT,
        MOD(wheels_off, 100)::INT,
        0
    )
WHERE wheels_off IS NOT NULL;

ALTER TABLE flights ADD COLUMN wheels_on_time TIME;

UPDATE flights
SET wheels_on_time = 
    MAKE_TIME(
        FLOOR(wheels_on / 100)::INT,
        MOD(wheels_on, 100)::INT,
        0
    )
WHERE wheels_on IS NOT NULL;

SELECT wheels_off, wheels_off_time, wheels_on, wheels_on_time
FROM flights
WHERE wheels_off IS NOT NULL AND wheels_on IS NOT NULL
LIMIT 10;

ALTER TABLE flights 
ADD COLUMN cancellation_reason_desc TEXT;
UPDATE flights
SET cancellation_reason_desc = 
    CASE cancellation_reason
        WHEN 'A' THEN 'Carrier Delay'
        WHEN 'B' THEN 'Weather Delay'
        WHEN 'C' THEN 'National Air System Delay'
        WHEN 'D' THEN 'Security Delay'
        ELSE NULL
    END;

SELECT cancellation_reason, cancellation_reason_desc
FROM flights
WHERE cancelled = 1
LIMIT 10;

UPDATE flights
SET flight_date = 
    MAKE_DATE(year, month, day);

SELECT year, month, day, flight_date
FROM flights
LIMIT 10;

SELECT 
    COUNT(*) FILTER (WHERE departure_delay IS NULL) AS dep_delay_nulls,
    COUNT(*) FILTER (WHERE arrival_delay IS NULL) AS arr_delay_nulls,
    COUNT(*) FILTER (WHERE air_time IS NULL) AS air_time_nulls,
    COUNT(*) FILTER (WHERE taxi_out IS NULL) AS taxi_out_nulls,
    COUNT(*) FILTER (WHERE taxi_in IS NULL) AS taxi_in_nulls,
    COUNT(*) FILTER (WHERE cancellation_reason IS NULL AND cancelled = 1) AS cancel_reason_nulls
FROM flights;

SELECT 
    departure_delay, arrival_delay, air_time, taxi_out, taxi_in, cancelled
FROM flights
WHERE departure_delay IS NULL 
   OR arrival_delay IS NULL 
   OR air_time IS NULL 
   OR taxi_out IS NULL 
   OR taxi_in IS NULL
LIMIT 10;

UPDATE flights
SET departure_delay = 0
WHERE departure_delay IS NULL AND cancelled = 0;

UPDATE flights
SET arrival_delay = 0
WHERE arrival_delay IS NULL AND cancelled = 0;

UPDATE flights
SET air_time = 0
WHERE air_time IS NULL AND cancelled = 0;

UPDATE flights
SET taxi_out = 0
WHERE taxi_out IS NULL AND cancelled = 0;

UPDATE flights
SET taxi_in = 0
WHERE taxi_in IS NULL AND cancelled = 0;

SELECT cancellation_reason, cancellation_reason_desc, cancelled
FROM flights
WHERE cancelled = 1 AND cancellation_reason IS NULL
LIMIT 10;

SELECT 
    flight_date, airline, departure_delay, arrival_delay, air_time, taxi_out, taxi_in,
    cancelled, cancellation_reason, cancellation_reason_desc
FROM flights
LIMIT 10;

SELECT DISTINCT airline
FROM flights
WHERE airline NOT IN (SELECT iata_code FROM airlines);

SELECT DISTINCT origin_airport
FROM flights
WHERE origin_airport NOT IN (SELECT iata_code FROM airports);

SELECT DISTINCT origin_airport
FROM flights
WHERE origin_airport ~ '^[0-9]+'
LIMIT 20;

SELECT * FROM airports LIMIT 5;

SELECT 
    f.flight_date,
    f.airline,
    a.airline_name,
    f.origin_airport,
    o.airport_name AS origin_airport_name,
    o.city AS origin_city,
    f.destination_airport,
    d.airport_name AS destination_airport_name,
    d.city AS destination_city,
    f.departure_delay,
    f.arrival_delay,
    f.cancelled,
    f.cancellation_reason,
    f.cancellation_reason_desc
FROM flights f
LEFT JOIN airlines a ON f.airline = a.iata_code
LEFT JOIN airports o ON f.origin_airport = o.iata_code
LEFT JOIN airports d ON f.destination_airport = d.iata_code
LIMIT 10;

CREATE OR REPLACE VIEW enriched_flights_data AS
SELECT 
    f.*,
    a.airline_name,
    ao.airport_name AS origin_airport_name,
    ao.city AS origin_city,
    ao.state AS origin_state,
    ao.country AS origin_country,
    ao.latitude AS origin_latitude,
    ao.longitude AS origin_longitude,
    ad.airport_name AS destination_airport_name,
    ad.city AS destination_city,
    ad.state AS destination_state,
    ad.country AS destination_country,
    ad.latitude AS destination_latitude,
    ad.longitude AS destination_longitude
FROM flights f
LEFT JOIN airlines a ON f.airline = a.iata_code
LEFT JOIN airports ao ON f.origin_airport = ao.iata_code
LEFT JOIN airports ad ON f.destination_airport = ad.iata_code;

SELECT 
    COUNT(*) AS total_flights,
    SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS cancelled_flights,
    ROUND(
        100.0 * SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS cancellation_rate_percent
FROM enriched_flights_data;

SELECT 
    cancellation_reason_desc,
    COUNT(*) AS total_cancelled
FROM enriched_flights_data
WHERE cancelled = 1
GROUP BY cancellation_reason_desc
ORDER BY total_cancelled DESC;

SELECT 
    COUNT(*) AS diverted_flights
FROM enriched_flights_data
WHERE diverted = 1;

SELECT 
    COUNT(*) FILTER (WHERE arrival_delay <= 15) * 100.0 / COUNT(*) AS otp_rate_percentage
FROM enriched_flights_data
WHERE cancelled = 0 AND diverted = 0;

SELECT 
    ROUND(AVG(arrival_delay), 2) AS avg_arrival_delay,
    ROUND(AVG(departure_delay), 2) AS avg_departure_delay,
    MAX(arrival_delay) AS max_arrival_delay,
    MAX(departure_delay) AS max_departure_delay,
    MIN(arrival_delay) AS min_arrival_delay,
    MIN(departure_delay) AS min_departure_delay
FROM enriched_flights_data
WHERE cancelled = 0 AND diverted = 0;

SELECT
    ROUND(AVG(departure_delay), 2) AS avg_departure_delay,
    ROUND(AVG(arrival_delay), 2) AS avg_arrival_delay,
    MIN(departure_delay) AS min_departure_delay,
    MAX(departure_delay) AS max_departure_delay,
    MIN(arrival_delay) AS min_arrival_delay,
    MAX(arrival_delay) AS max_arrival_delay
FROM enriched_flights_data
WHERE cancelled = 0 AND diverted = 0;

SELECT 
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY departure_delay) AS median_departure_delay,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY arrival_delay) AS median_arrival_delay
FROM enriched_flights_data
WHERE cancelled = 0 AND diverted = 0;

WITH delay_totals AS (
    SELECT
        SUM(air_system_delay) AS air_system,
        SUM(security_delay) AS security,
        SUM(airline_delay) AS airline,
        SUM(late_aircraft_delay) AS late_aircraft,
        SUM(weather_delay) AS weather
    FROM enriched_flights_data
),
unpivoted AS (
    SELECT 'Air System Delay' AS delay_type, air_system AS total_delay FROM delay_totals
    UNION ALL
    SELECT 'Security Delay', security FROM delay_totals
    UNION ALL
    SELECT 'Airline Delay', airline FROM delay_totals
    UNION ALL
    SELECT 'Late Aircraft Delay', late_aircraft FROM delay_totals
    UNION ALL
    SELECT 'Weather Delay', weather FROM delay_totals
),
final_calc AS (
    SELECT *,
           SUM(total_delay) OVER () AS grand_total
    FROM unpivoted
)
SELECT
    delay_type,
    total_delay,
    ROUND((total_delay * 100.0) / grand_total, 2) AS percentage_contribution
FROM final_calc
ORDER BY total_delay DESC;

SELECT 
    COUNT(*) AS total_flights,
    ROUND(AVG(arrival_delay), 2) AS avg_arrival_delay,
    ROUND(AVG(departure_delay), 2) AS avg_departure_delay,
    ROUND(
        (COUNT(*) FILTER (WHERE arrival_delay <= 15 OR arrival_delay IS NULL) * 100.0) / COUNT(*),
        2
    ) AS otp_rate_percent,
    ROUND(
        (COUNT(*) FILTER (WHERE cancelled = 1) * 100.0) / COUNT(*),
        2
    ) AS cancellation_rate_percent
FROM enriched_flights_data;

SELECT DISTINCT efd.airline, a.airline_name
FROM enriched_flights_data efd
JOIN airlines a ON efd.airline = a.iata_code;

SELECT
    airline,
    airline_name,
    COUNT(*) AS total_flights,
    ROUND(AVG(departure_delay), 2) AS avg_departure_delay,
    ROUND(AVG(arrival_delay), 2) AS avg_arrival_delay,
    ROUND(
        100.0 * SUM(CASE WHEN arrival_delay <= 15 OR arrival_delay IS NULL THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS otp_rate_percent,
    ROUND(
        100.0 * SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS cancellation_rate_percent
FROM
    enriched_flights_data
GROUP BY
    airline, airline_name
ORDER BY
    otp_rate_percent DESC;

SELECT DISTINCT origin_airport
FROM flights
WHERE origin_airport ~ '^[0-9]+$'
LIMIT 10;
SELECT DISTINCT destination_airport
FROM flights
WHERE destination_airport ~ '^[0-9]+$'
LIMIT 10;

WITH cleaned_flights AS (
  SELECT 
    *,
    CASE 
      WHEN origin_airport ~ '^[0-9]+$' THEN 'Unknown'
      ELSE origin_airport
    END AS cleaned_origin_airport,

    CASE 
      WHEN destination_airport ~ '^[0-9]+$' THEN 'Unknown'
      ELSE destination_airport
    END AS cleaned_destination_airport
  FROM flights
)
SELECT
  COUNT(*) FILTER (WHERE cleaned_origin_airport = 'Unknown') AS unknown_origin_count,
  COUNT(*) FILTER (WHERE cleaned_destination_airport = 'Unknown') AS unknown_destination_count
FROM cleaned_flights;

CREATE OR REPLACE VIEW enriched_flights_data AS
SELECT
    f.*,
    COALESCE(ao.airport_name, 'Unknown') AS origin_airport_name,
    COALESCE(ao.city, 'Unknown') AS origin_city,
    COALESCE(ao.state, 'Unknown') AS origin_state,
    COALESCE(ao.country, 'Unknown') AS origin_country,
    COALESCE(ao.latitude, 0.0) AS origin_latitude,
    COALESCE(ao.longitude, 0.0) AS origin_longitude,
    COALESCE(ad.airport_name, 'Unknown') AS destination_airport_name,
    COALESCE(ad.city, 'Unknown') AS destination_city,
    COALESCE(ad.state, 'Unknown') AS destination_state,
    COALESCE(ad.country, 'Unknown') AS destination_country,
    COALESCE(ad.latitude, 0.0) AS destination_latitude,
    COALESCE(ad.longitude, 0.0) AS destination_longitude
FROM flights f
LEFT JOIN airports ao ON f.origin_airport = ao.iata_code
LEFT JOIN airports ad ON f.destination_airport = ad.iata_code;

DROP VIEW IF EXISTS enriched_flights_data;

CREATE VIEW enriched_flights_data AS
SELECT
    f.*,
    COALESCE(ao.airport_name, 'Unknown') AS origin_airport_name,
    COALESCE(ao.city, 'Unknown') AS origin_city,
    COALESCE(ao.state, 'Unknown') AS origin_state,
    COALESCE(ao.country, 'Unknown') AS origin_country,
    COALESCE(ao.latitude, 0.0) AS origin_latitude,
    COALESCE(ao.longitude, 0.0) AS origin_longitude,
    COALESCE(ad.airport_name, 'Unknown') AS destination_airport_name,
    COALESCE(ad.city, 'Unknown') AS destination_city,
    COALESCE(ad.state, 'Unknown') AS destination_state,
    COALESCE(ad.country, 'Unknown') AS destination_country,
    COALESCE(ad.latitude, 0.0) AS destination_latitude,
    COALESCE(ad.longitude, 0.0) AS destination_longitude
FROM flights f
LEFT JOIN airports ao ON f.origin_airport = ao.iata_code
LEFT JOIN airports ad ON f.destination_airport = ad.iata_code;

SELECT * 
FROM enriched_flights_data
limit all;

DROP VIEW IF EXISTS enriched_flights_data;

-- Now recreate the view (use the same enriched join query you used earlier)
CREATE VIEW enriched_flights_data AS
SELECT
    f.*,
    a.airline_name,
    ao.airport_name AS origin_airport_name,
    ao.city AS origin_city,
    ao.state AS origin_state,
    ao.country AS origin_country,
    ao.latitude AS origin_latitude,
    ao.longitude AS origin_longitude,
    ad.airport_name AS destination_airport_name,
    ad.city AS destination_city,
    ad.state AS destination_state,
    ad.country AS destination_country,
    ad.latitude AS destination_latitude,
    ad.longitude AS destination_longitude
FROM flights f
LEFT JOIN airlines a ON f.airline = a.iata_code
LEFT JOIN airports ao ON f.origin_airport = ao.iata_code
LEFT JOIN airports ad ON f.destination_airport = ad.iata_code;

-- Replace numeric FAA codes with 'Unknown' in origin_airport
UPDATE flights
SET origin_airport = 'Unknown'
WHERE origin_airport ~ '^[0-9]+$';

-- Replace numeric FAA codes with 'Unknown' in destination_airport
UPDATE flights
SET destination_airport = 'Unknown'
WHERE destination_airport ~ '^[0-9]+$';

DROP VIEW IF EXISTS enriched_flights_data;

CREATE VIEW enriched_flights_data AS
SELECT
    f.*,
    a.airline_name,
    ao.airport_name AS origin_airport_name,
    ao.city AS origin_city,
    ao.state AS origin_state,
    ao.country AS origin_country,
    ao.latitude AS origin_latitude,
    ao.longitude AS origin_longitude,
    ad.airport_name AS destination_airport_name,
    ad.city AS destination_city,
    ad.state AS destination_state,
    ad.country AS destination_country,
    ad.latitude AS destination_latitude,
    ad.longitude AS destination_longitude
FROM flights f
LEFT JOIN airlines a ON f.airline = a.iata_code
LEFT JOIN airports ao ON f.origin_airport = ao.iata_code
LEFT JOIN airports ad ON f.destination_airport = ad.iata_code;

SELECT * 
FROM enriched_flights_data 
WHERE origin_airport = 'Unknown' 
   OR destination_airport = 'Unknown';

SELECT 
    origin_airport, 
    origin_airport_name, 
    destination_airport, 
    destination_airport_name
FROM enriched_flights_data
WHERE origin_airport = 'Unknown'
   OR destination_airport = 'Unknown'
LIMIT 100;

SELECT
    year,
    month,
    COUNT(*) AS total_flights
FROM enriched_flights_data
GROUP BY year, month
ORDER BY year, month;

SELECT
    airline,
    airline_name,
    COUNT(*) AS total_flights,
    ROUND(AVG(arrival_delay), 2) AS avg_arrival_delay,
    ROUND(AVG(departure_delay), 2) AS avg_departure_delay,
    ROUND(100.0 * SUM(CASE WHEN arrival_delay <= 0 THEN 1 ELSE 0 END) / COUNT(*), 2) AS otp_rate_percent,
    ROUND(100.0 * SUM(cancelled) / COUNT(*), 2) AS cancellation_rate_percent
FROM enriched_flights_data
GROUP BY airline, airline_name
ORDER BY total_flights DESC;

SELECT
    origin_airport,
    origin_airport_name,
    origin_city,
    origin_state,
    COUNT(*) AS total_departures,
    ROUND(AVG(departure_delay), 2) AS avg_departure_delay,
    ROUND(100.0 * SUM(CASE WHEN departure_delay <= 0 THEN 1 ELSE 0 END) / COUNT(*), 2) AS otp_departure_rate,
    ROUND(100.0 * SUM(cancelled) / COUNT(*), 2) AS cancellation_rate
FROM enriched_flights_data
GROUP BY origin_airport, origin_airport_name, origin_city, origin_state
ORDER BY total_departures DESC;

SELECT
    destination_airport,
    destination_airport_name,
    destination_city,
    destination_state,
    COUNT(*) AS total_arrivals,
    ROUND(AVG(arrival_delay), 2) AS avg_arrival_delay,
    ROUND(100.0 * SUM(CASE WHEN arrival_delay <= 0 THEN 1 ELSE 0 END) / COUNT(*), 2) AS otp_arrival_rate,
    ROUND(100.0 * SUM(cancelled) / COUNT(*), 2) AS cancellation_rate
FROM enriched_flights_data
GROUP BY destination_airport, destination_airport_name, destination_city, destination_state
ORDER BY total_arrivals DESC;

SELECT
    SUM(air_system_delay) AS total_air_system_delay,
    SUM(security_delay) AS total_security_delay,
    SUM(airline_delay) AS total_airline_delay,
    SUM(late_aircraft_delay) AS total_late_aircraft_delay,
    SUM(weather_delay) AS total_weather_delay
FROM flights;

SELECT
    ROUND(AVG(COALESCE(air_system_delay, 0)), 2) AS avg_air_system_delay,
    ROUND(AVG(COALESCE(security_delay, 0)), 2) AS avg_security_delay,
    ROUND(AVG(COALESCE(airline_delay, 0)), 2) AS avg_airline_delay,
    ROUND(AVG(COALESCE(late_aircraft_delay, 0)), 2) AS avg_late_aircraft_delay,
    ROUND(AVG(COALESCE(weather_delay, 0)), 2) AS avg_weather_delay
FROM flights;

SELECT
    f.airline,
    a.airline_name,
    ROUND(AVG(COALESCE(f.air_system_delay, 0)), 2) AS avg_air_system_delay,
    ROUND(AVG(COALESCE(f.security_delay, 0)), 2) AS avg_security_delay,
    ROUND(AVG(COALESCE(f.airline_delay, 0)), 2) AS avg_airline_delay,
    ROUND(AVG(COALESCE(f.late_aircraft_delay, 0)), 2) AS avg_late_aircraft_delay,
    ROUND(AVG(COALESCE(f.weather_delay, 0)), 2) AS avg_weather_delay
FROM flights f
JOIN airlines a ON f.airline = a.iata_code
GROUP BY f.airline, a.airline_name
ORDER BY avg_airline_delay DESC;

SELECT 
    month,
    ROUND(AVG(COALESCE(arrival_delay, 0)), 2) AS avg_arrival_delay,
    ROUND(AVG(COALESCE(departure_delay, 0)), 2) AS avg_departure_delay,
    ROUND(AVG(COALESCE(air_system_delay, 0)), 2) AS avg_air_system_delay,
    ROUND(AVG(COALESCE(security_delay, 0)), 2) AS avg_security_delay,
    ROUND(AVG(COALESCE(airline_delay, 0)), 2) AS avg_airline_delay,
    ROUND(AVG(COALESCE(late_aircraft_delay, 0)), 2) AS avg_late_aircraft_delay,
    ROUND(AVG(COALESCE(weather_delay, 0)), 2) AS avg_weather_delay
FROM flights
GROUP BY month
ORDER BY month;

SELECT 
    f.airline,
    a.airline_name,
    ROUND(AVG(COALESCE(arrival_delay, 0)), 2) AS avg_arrival_delay,
    ROUND(AVG(COALESCE(departure_delay, 0)), 2) AS avg_departure_delay,
    ROUND(AVG(COALESCE(air_system_delay, 0)), 2) AS avg_air_system_delay,
    ROUND(AVG(COALESCE(security_delay, 0)), 2) AS avg_security_delay,
    ROUND(AVG(COALESCE(airline_delay, 0)), 2) AS avg_airline_delay,
    ROUND(AVG(COALESCE(late_aircraft_delay, 0)), 2) AS avg_late_aircraft_delay,
    ROUND(AVG(COALESCE(weather_delay, 0)), 2) AS avg_weather_delay
FROM flights f
LEFT JOIN airlines a ON f.airline = a.iata_code
GROUP BY f.airline, a.airline_name
ORDER BY avg_arrival_delay DESC;

SELECT 
    f.origin_airport,
    ao.airport_name AS origin_airport_name,
    ROUND(AVG(COALESCE(arrival_delay, 0)), 2) AS avg_arrival_delay,
    ROUND(AVG(COALESCE(departure_delay, 0)), 2) AS avg_departure_delay,
    ROUND(AVG(COALESCE(air_system_delay, 0)), 2) AS avg_air_system_delay,
    ROUND(AVG(COALESCE(security_delay, 0)), 2) AS avg_security_delay,
    ROUND(AVG(COALESCE(airline_delay, 0)), 2) AS avg_airline_delay,
    ROUND(AVG(COALESCE(late_aircraft_delay, 0)), 2) AS avg_late_aircraft_delay,
    ROUND(AVG(COALESCE(weather_delay, 0)), 2) AS avg_weather_delay
FROM flights f
LEFT JOIN airports ao ON f.origin_airport = ao.iata_code
GROUP BY f.origin_airport, ao.airport_name
ORDER BY avg_departure_delay DESC;

SELECT 
    f.origin_airport,
    ao.airport_name AS origin_airport_name,
    f.destination_airport,
    ad.airport_name AS destination_airport_name,
    COUNT(*) AS total_flights,
    ROUND(AVG(COALESCE(arrival_delay, 0)), 2) AS avg_arrival_delay,
    ROUND(AVG(COALESCE(departure_delay, 0)), 2) AS avg_departure_delay,
    ROUND(AVG(COALESCE(airline_delay, 0)), 2) AS avg_airline_delay
FROM flights f
LEFT JOIN airports ao ON f.origin_airport = ao.iata_code
LEFT JOIN airports ad ON f.destination_airport = ad.iata_code
GROUP BY f.origin_airport, f.destination_airport, ao.airport_name, ad.airport_name
ORDER BY avg_departure_delay DESC
LIMIT 10;

SELECT 
    airline,
    a.airline_name,
    cancellation_reason,
    CASE 
        WHEN cancellation_reason = 'A' THEN 'Carrier'
        WHEN cancellation_reason = 'B' THEN 'Weather'
        WHEN cancellation_reason = 'C' THEN 'NAS'
        WHEN cancellation_reason = 'D' THEN 'Security'
        ELSE 'Unknown'
    END AS reason_description,
    COUNT(*) AS total_cancellations
FROM flights f
LEFT JOIN airlines a ON f.airline = a.iata_code
WHERE cancelled = 1
GROUP BY airline, a.airline_name, cancellation_reason
ORDER BY total_cancellations DESC;

SELECT *
FROM enriched_flights_data;



