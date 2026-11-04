-- ============================================================
-- PROJECT: Azerbaijan Aviation & Sky Network
-- DESCRIPTION: Analytical Queries for Aviation Operations & Fleet Performance
-- ============================================================

-- ------------------------------------------------------------
-- QUERY 1: Peak Departure Hours (UTC)
-- Purpose: Identifies operational flight density peaks throughout the day.
-- ------------------------------------------------------------
SELECT 
    EXTRACT(HOUR FROM f.sched_dep_utc) AS dep_hour_utc,
    COUNT(f.flight_number) AS flight_count,
    ROUND(COUNT(f.flight_number) * 100.0 / SUM(COUNT(f.flight_number)) OVER(), 2) AS share_pct
FROM `flight-data-pipeline-512007.flight_dw.fact_flights` f
GROUP BY dep_hour_utc
ORDER BY flight_count DESC;

-- ------------------------------------------------------------
-- QUERY 2: Airlines Network Breadth
-- Purpose: Ranks carriers by the diversity of unique cities and countries served.
-- ------------------------------------------------------------
SELECT 
    al.airline_name,
    COUNT(DISTINCT ap.country_code) AS unique_countries,
    COUNT(DISTINCT ap.city_name) AS unique_cities,
    COUNT(f.flight_number) AS total_flights
FROM `flight-data-pipeline-512007.flight_dw.fact_flights` f
JOIN `flight-data-pipeline-512007.flight_dw.dim_airlines` al 
    ON f.airline_id = al.airline_id
JOIN `flight-data-pipeline-512007.flight_dw.dim_airports` ap 
    ON f.destination_iata = ap.iata_code
GROUP BY al.airline_name
ORDER BY unique_cities DESC;

-- ------------------------------------------------------------
-- QUERY 3: Airline Competition on Top Routes
-- Purpose: Evaluates market share across top destination cities (Istanbul, Moscow, SAW, Tbilisi, Tel Aviv).
-- ------------------------------------------------------------
WITH TopCities AS (
    SELECT ap.city_name
    FROM `flight-data-pipeline-512007.flight_dw.fact_flights` f
    JOIN `flight-data-pipeline-512007.flight_dw.dim_airports` ap 
        ON f.destination_iata = ap.iata_code
    GROUP BY ap.city_name
    ORDER BY COUNT(*) DESC
    LIMIT 5
)
SELECT 
    ap.city_name,
    al.airline_name,
    COUNT(f.flight_number) AS flights_on_route
FROM `flight-data-pipeline-512007.flight_dw.fact_flights` f
JOIN `flight-data-pipeline-512007.flight_dw.dim_airlines` al 
    ON f.airline_id = al.airline_id
JOIN `flight-data-pipeline-512007.flight_dw.dim_airports` ap 
    ON f.destination_iata = ap.iata_code
WHERE ap.city_name IN (SELECT city_name FROM TopCities)
GROUP BY ap.city_name, al.airline_name
ORDER BY ap.city_name, flights_on_route DESC;

-- ------------------------------------------------------------
-- QUERY 4: Terminal Traffic Distribution
-- Purpose: Analyzes flight allocation across airport terminals (Terminal 1, 2, 2N, 2S).
-- ------------------------------------------------------------
SELECT 
    f.dep_terminal,
    al.airline_name,
    COUNT(f.flight_number) AS total_flights
FROM `flight-data-pipeline-512007.flight_dw.fact_flights` f
JOIN `flight-data-pipeline-512007.flight_dw.dim_airlines` al 
    ON f.airline_id = al.airline_id
WHERE f.dep_terminal IS NOT NULL AND f.dep_terminal != ''
GROUP BY f.dep_terminal, al.airline_name
ORDER BY f.dep_terminal, total_flights DESC;

-- ------------------------------------------------------------
-- QUERY 5: Route & Fleet Assignment
-- Purpose: Maps specific aircraft models deployed per destination route (Min 5 flights).
-- ------------------------------------------------------------
SELECT 
    ap.city_name,
    am.aircraft_model,
    COUNT(f.flight_number) AS flight_count
FROM `flight-data-pipeline-512007.flight_dw.fact_flights` f
JOIN `flight-data-pipeline-512007.flight_dw.dim_airports` ap 
    ON f.destination_iata = ap.iata_code
JOIN `flight-data-pipeline-512007.flight_dw.dim_aircraft_models` am 
    ON f.aircraft_model_id = am.aircraft_model_id
GROUP BY ap.city_name, am.aircraft_model
HAVING flight_count >= 5
ORDER BY flight_count DESC;