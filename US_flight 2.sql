-- =====================================================
-- 1. CREATE DATABASE, SCHEMA, WAREHOUSE
-- =====================================================
USE DATABASE flight_analysis;

CREATE SCHEMA IF NOT EXISTS dimensions;
USE SCHEMA dimensions;

USE WAREHOUSE COMPUTE_WH;

-- =====================================================
-- 2. AIRLINE DIMENSION TABLE (SCD TYPE 2)
-- =====================================================
CREATE OR REPLACE TABLE airline_dimension (
    airline_code VARCHAR,
    airline_name VARCHAR,
    airport_name VARCHAR,
    carrier_plane VARCHAR,
    is_carrier_flight BOOLEAN,
    effective_start_date DATE,
    effective_end_date DATE,
    is_current BOOLEAN
);

-- =====================================================
-- 3. STAGING TABLE (FROM df_silver / source)
-- =====================================================
CREATE OR REPLACE TEMP TABLE airline_stage AS
SELECT DISTINCT 
    airline_code,

    CASE airline_code
        WHEN 'F9' THEN 'Frontier Airlines'
        WHEN 'YV' THEN 'Mesa Airlines'
        WHEN 'AA' THEN 'American Airlines'
        WHEN 'NK' THEN 'Spirit Airlines'
        WHEN 'OH' THEN 'PSA Airlines'
        WHEN 'YX' THEN 'Republic Airways'
        WHEN 'AS' THEN 'Alaska Airlines'
        WHEN 'MQ' THEN 'Envoy Air'
        WHEN 'DL' THEN 'Delta Air Lines'
        WHEN 'UA' THEN 'United Airlines'
        WHEN '9E' THEN 'Endeavor Air'
        WHEN 'HA' THEN 'Hawaiian Airlines'
        WHEN 'QX' THEN 'Horizon Air'
        WHEN 'OO' THEN 'SkyWest Airlines'
        WHEN 'WN' THEN 'Southwest Airlines'
        WHEN 'B6' THEN 'JetBlue Airways'
        WHEN 'G4' THEN 'Allegiant Air'
        ELSE 'Unknown Airline'
    END AS airline_name,

    NULL AS airport_name,
    NULL AS carrier_plane,

    -- Carrier flight flag
    CASE 
        WHEN airline_code IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS is_carrier_flight,

    CURRENT_DATE() AS effective_start_date

FROM flight_analysis.staging.MONTHLY_AIRLINE_API;

-- =====================================================
-- 4. INITIAL LOAD (FIRST TIME ONLY)
-- =====================================================
INSERT INTO airline_dimension
SELECT 
    airline_code,
    airline_name,
    airport_name,
    carrier_plane,
    is_carrier_flight,
    effective_start_date,
    NULL,
    TRUE
FROM airline_stage;

-- =====================================================
-- 5. SCD TYPE 2 IMPLEMENTATION
-- =====================================================

-- Step 1: Expire old records
UPDATE airline_dimension tgt
SET 
    effective_end_date = CURRENT_DATE(),
    is_current = FALSE
FROM airline_stage src
WHERE tgt.airline_code = src.airline_code
AND tgt.is_current = TRUE
AND tgt.airline_name != src.airline_name;

-- Step 2: Insert new records
INSERT INTO airline_dimension
SELECT 
    src.airline_code,
    src.airline_name,
    src.airport_name,
    src.carrier_plane,
    src.is_carrier_flight,
    CURRENT_DATE(),
    NULL,
    TRUE
FROM airline_stage src
LEFT JOIN airline_dimension tgt
ON src.airline_code = tgt.airline_code
AND tgt.is_current = TRUE
WHERE tgt.airline_code IS NULL
   OR tgt.airline_name != src.airline_name;

-- =====================================================
-- 6. VERIFY DATA
-- =====================================================
SELECT * FROM airline_dimension;

-- =====================================================
-- 7. CTE: GENERATE NUMBERS 1 TO 100
-- =====================================================
WITH numbers AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS num
    FROM TABLE(GENERATOR(ROWCOUNT => 100))
)
SELECT * FROM numbers;

-- =====================================================
-- 8. CREATE DATES: MAY 1 TO MAY 7
-- =====================================================
WITH may_dates AS (
    SELECT DATEADD(DAY, SEQ4(), '2023-05-01') AS full_date
    FROM TABLE(GENERATOR(ROWCOUNT => 7))
)
SELECT * FROM may_dates;

-- =====================================================
-- 9. DATE DIMENSION TABLE (750 DAYS: 2021–2022)
-- =====================================================
CREATE OR REPLACE TABLE date_dimension AS
SELECT
    generated_date AS full_date,

    YEAR(generated_date) AS year_number,
    MONTH(generated_date) AS month_number,
    MONTHNAME(generated_date) AS month_name,

    DAYOFWEEKISO(generated_date) AS day_of_week_number,
    DAYNAME(generated_date) AS day_of_week_name,

    QUARTER(generated_date) AS quarter_number,

    -- Weekend flag
    CASE 
        WHEN DAYOFWEEKISO(generated_date) IN (6,7) THEN TRUE
        ELSE FALSE
    END AS weekend_flag,

    -- Season logic
    CASE 
        WHEN MONTH(generated_date) IN (12,1,2) THEN 'Winter'
        WHEN MONTH(generated_date) IN (3,4) THEN 'Spring'
        WHEN MONTH(generated_date) IN (5,6,7) THEN 'Summer'
        ELSE 'Monsoon'
    END AS season_weather,

    -- Year-Month
    TO_CHAR(generated_date, 'YYYY-MM') AS year_month

FROM (
    SELECT DATEADD(DAY, SEQ4(), '2021-01-01') AS generated_date
    FROM TABLE(GENERATOR(ROWCOUNT => 750))
)
WHERE generated_date < '2023-01-01';

-- =====================================================
-- 10. VERIFY DATE DIMENSION
-- =====================================================
SELECT * FROM date_dimension;