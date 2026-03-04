/* ==================================================
PROJECT: WIND TURBINE DATA ANALYSIS (SCADA)
DATABASE: PostgreSQL
DESCRIPTION: Full analysis from Data Import to Advanced Window Functions.
================================================== */

-- 1. TABLE CREATION
CREATE TABLE wind_scada_data (
    date_time_utc TIMESTAMP,
    active_power_kw FLOAT,
    wind_speed_ms FLOAT,
    theoretical_power_curve_kw FLOAT,
    wind_direction FLOAT
);

-- 2. DATE SETTINGS & DATA IMPORT
SET datestyle = 'ISO, DMY';

-- Update the path to your local CSV file
COPY wind_scada_data FROM '/Users/Shared/T1.csv' WITH (FORMAT CSV, HEADER);

-- 3. EXPLORATORY DATA ANALYSIS (EDA)
SELECT COUNT(*) AS total_rows FROM wind_scada_data;

-- 4. MONTHLY STATISTICS (MWh & Average Wind Speed)
SELECT 
    DATE_TRUNC('month', date_time_utc) AS production_month,
    ROUND(CAST(AVG(wind_speed_ms) AS NUMERIC), 2) AS avg_wind_speed,
    ROUND(CAST(SUM(active_power_kw) / 6000.0 AS NUMERIC), 2) AS total_mwh
FROM wind_scada_data
GROUP BY 1
ORDER BY 1;

-- 5. BASIC ANALYSIS (Filtering & Aggregations)
-- Detecting extreme wind speeds (> 20 m/s)
SELECT date_time_utc, wind_speed_ms, active_power_kw
FROM wind_scada_data
WHERE wind_speed_ms > 20
ORDER BY wind_speed_ms DESC;

-- Identifying "Low Production" days (Avg wind < 3 m/s)
SELECT 
    DATE_TRUNC('day', date_time_utc) AS production_day, 
    AVG(wind_speed_ms) AS daily_avg_wind
FROM wind_scada_data
GROUP BY 1
HAVING AVG(wind_speed_ms) < 3
ORDER BY 2 ASC;

-- 6. PERFORMANCE ANALYSIS (Actual vs Theoretical)
CREATE OR REPLACE VIEW v_performance_summary AS
SELECT 
    date_time_utc,
    active_power_kw,
    theoretical_power_curve_kw,
    (active_power_kw - theoretical_power_curve_kw) AS power_diff,
    ((active_power_kw - theoretical_power_curve_kw) / NULLIF(theoretical_power_curve_kw, 0)) * 100 AS percentage_diff
FROM wind_scada_data;

-- 7. DATABASE NORMALIZATION (Metadata)
CREATE TABLE turbine_info (
    turbine_id VARCHAR(50) PRIMARY KEY,
    model_name VARCHAR(100) NOT NULL,
    nominal_power_kw NUMERIC NOT NULL,
    location VARCHAR(255) NOT NULL,
    latitude FLOAT,
    longitude FLOAT
);

INSERT INTO turbine_info (turbine_id, model_name, nominal_power_kw, location, latitude, longitude)
VALUES ('T1', 'GE-2.5-120', 2500, 'Karystos-Greece', 38.01, 24.42);

ALTER TABLE wind_scada_data ADD COLUMN turbine_id VARCHAR(50) DEFAULT 'T1';

-- 8. WINDOW FUNCTIONS (Averages & Lags)
-- Daily average comparison
SELECT 
    date_time_utc, 
    active_power_kw, 
    AVG(active_power_kw) OVER(PARTITION BY DATE_TRUNC('day', date_time_utc)) AS daily_avg
FROM wind_scada_data
LIMIT 10;

-- Wind speed change detection (Wind Gusts)
SELECT 
    date_time_utc, 
    wind_speed_ms, 
    LAG(wind_speed_ms) OVER (ORDER BY date_time_utc) AS previous_wind_speed,
    wind_speed_ms - LAG(wind_speed_ms) OVER (ORDER BY date_time_utc) AS wind_speed_change
FROM wind_scada_data
LIMIT 10;

-- 9. ADVANCED ANALYTICS (Ranking & Running Totals using CTEs)
-- Top 3 production peaks per day
WITH ranked_data AS (
    SELECT date_time_utc, active_power_kw, 
           RANK() OVER (PARTITION BY DATE_TRUNC('day', date_time_utc) ORDER BY active_power_kw DESC) AS rank
    FROM wind_scada_data
)
SELECT * FROM ranked_data WHERE rank <= 3;

-- Cumulative daily energy production
WITH daily_summary AS (
    SELECT date_time_utc, active_power_kw,
           SUM(active_power_kw) OVER (PARTITION BY DATE_TRUNC('day', date_time_utc) ORDER BY date_time_utc) AS cumulative_sum
    FROM wind_scada_data
)
SELECT * FROM daily_summary;

-- 10. MASTER QUERY FOR BI TOOLS
WITH combined_data AS (
    SELECT 
        s.date_time_utc, 
        s.active_power_kw, 
        s.wind_speed_ms,
        t.model_name,
        t.location,
        t.latitude,
        t.longitude,
        AVG(s.active_power_kw) OVER (PARTITION BY DATE_TRUNC('day', s.date_time_utc)) AS daily_avg
    FROM wind_scada_data AS s
    LEFT JOIN turbine_info AS t ON s.turbine_id = t.turbine_id
)
SELECT * FROM combined_data;