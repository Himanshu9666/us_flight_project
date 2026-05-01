-- =========================================================
-- DATABASE / SCHEMA / WAREHOUSE
-- =========================================================
CREATE OR REPLACE DATABASE FLIGHT_ANALYSIS;
USE DATABASE FLIGHT_ANALYSIS;

CREATE OR REPLACE SCHEMA STAGING;
USE SCHEMA STAGING;

USE WAREHOUSE COMPUTE_WH;

-- =========================================================
-- STORAGE INTEGRATION (already created ho to skip kar sakta hai)
-- =========================================================
CREATE OR REPLACE STORAGE INTEGRATION s3_flight_gold_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::686131354870:role/snowflake_role'
STORAGE_ALLOWED_LOCATIONS = ('s3://flight-analytics-de-divyansh-2026/gold_delta_folder/');

-- =========================================================
-- FILE FORMAT
-- =========================================================
CREATE OR REPLACE FILE FORMAT parquet_ff
TYPE = PARQUET;

-- =========================================================
-- STAGE
-- =========================================================
CREATE OR REPLACE STAGE gold_stage_s3
STORAGE_INTEGRATION = s3_flight_gold_int
URL = 's3://flight-analytics-de-divyansh-2026/gold_delta_folder/'
FILE_FORMAT = parquet_ff;

-- Check files
LIST @gold_stage_s3;

-- =========================================================
-- TABLE 1 : MONTHLY AIRLINE
-- =========================================================
CREATE OR REPLACE TABLE MONTHLY_AIRLINE_API (
    flight_year NUMBER,
    flight_month NUMBER,
    airline_code STRING,
    reporting_airline STRING,
    avg_distance_travelled FLOAT,
    total_distance_travelled FLOAT
);

-- =========================================================
-- TABLE 2 : ROUTE KPI
-- =========================================================
CREATE OR REPLACE TABLE ANNUAL_ROUTE_PERFORMANCE (
    flight_year NUMBER,
    route STRING,
    origin_code STRING,
    destination_code STRING,
    number_of_flights NUMBER,
    avg_arrival_delay FLOAT,
    avg_distance_travelled FLOAT,
    total_delayed_flights NUMBER,
    total_on_time_flights NUMBER,
    number_of_airlines_on_route NUMBER,
    on_time_percentage_airline_percentage FLOAT
);

-- =========================================================
-- TABLE 3 : AIRPORT KPI
-- =========================================================
CREATE OR REPLACE TABLE AIRPORT_DEPARTURE_KPI (
    flight_year NUMBER,
    flight_month NUMBER,
    origin_code STRING,
    name STRING,
    city STRING,
    state STRING,
    lon FLOAT,
    lat FLOAT,
    total_departure NUMBER,
    total_cancelled_departure NUMBER,
    avg_delayed_departure FLOAT,
    total_on_time_departure NUMBER,
    avg_route_distance FLOAT,
    number_of_flights_operating NUMBER,
    avg_airtime FLOAT,
    departure_on_time_percentage FLOAT,
    year_month STRING
);

-- =========================================================
-- TABLE 4 : DELAY CAUSE
-- =========================================================
CREATE OR REPLACE TABLE DELAY_CAUSE_TABLE (
    flight_year NUMBER,
    flight_month NUMBER,
    airline_code STRING,
    total_minutes_delayed FLOAT,
    total_weather_delayed_minutes FLOAT,
    total_carrier_delayed_minutes FLOAT,
    total_security_delayed_minutes FLOAT,
    total_late_aircraft_delayed_minutes FLOAT,
    weather_delay_percentage FLOAT,
    carrier_delay_percentage FLOAT,
    security_delay_percentage FLOAT,
    late_aircraft_delay_percentage FLOAT
);

-- =========================================================
--  MANUAL LOAD TEST (RUN ONCE)
-- =========================================================
COPY INTO MONTHLY_AIRLINE_API
FROM @gold_stage_s3/gold_airline/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

COPY INTO ANNUAL_ROUTE_PERFORMANCE
FROM @gold_stage_s3/gold_route_kpi/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

COPY INTO AIRPORT_DEPARTURE_KPI
FROM @gold_stage_s3/gold_airport_departure_kpi/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

COPY INTO DELAY_CAUSE_TABLE
FROM @gold_stage_s3/gold_delay_cause/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- =========================================================
--  SNOWPIPES (AUTO INGEST)
-- =========================================================
CREATE OR REPLACE PIPE PIPE_MONTHLY_AIRLINE_API
AUTO_INGEST = TRUE
AS
COPY INTO MONTHLY_AIRLINE_API
FROM @gold_stage_s3/gold_airline/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

desc pipe PIPE_MONTHLY_AIRLINE_API;

CREATE OR REPLACE PIPE PIPE_ROUTE_KPI
AUTO_INGEST = TRUE
AS
COPY INTO ANNUAL_ROUTE_PERFORMANCE
FROM @gold_stage_s3/gold_route_kpi/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

CREATE OR REPLACE PIPE PIPE_AIRPORT_KPI
AUTO_INGEST = TRUE
AS
COPY INTO AIRPORT_DEPARTURE_KPI
FROM @gold_stage_s3/gold_airport_departure_kpi/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

CREATE OR REPLACE PIPE PIPE_DELAY_CAUSE
AUTO_INGEST = TRUE
AS
COPY INTO DELAY_CAUSE_TABLE
FROM @gold_stage_s3/gold_delay_cause/
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- =========================================================
--  REFRESH (OLD FILE LOAD KARNE KE LIYE)
-- =========================================================
ALTER PIPE PIPE_MONTHLY_AIRLINE_API REFRESH;
ALTER PIPE PIPE_ROUTE_KPI REFRESH;
ALTER PIPE PIPE_AIRPORT_KPI REFRESH;
ALTER PIPE PIPE_DELAY_CAUSE REFRESH;

-- =========================================================
--  PIPE STATUS CHECK
-- =========================================================
SELECT SYSTEM$PIPE_STATUS('PIPE_MONTHLY_AIRLINE_API');
SELECT SYSTEM$PIPE_STATUS('PIPE_ROUTE_KPI');
SELECT SYSTEM$PIPE_STATUS('PIPE_AIRPORT_KPI');
SELECT SYSTEM$PIPE_STATUS('PIPE_DELAY_CAUSE');

-- =========================================================
--  DATA VALIDATION
-- =========================================================
SELECT COUNT(*) FROM MONTHLY_AIRLINE_API;
SELECT COUNT(*) FROM ANNUAL_ROUTE_PERFORMANCE;
SELECT COUNT(*) FROM AIRPORT_DEPARTURE_KPI;
SELECT COUNT(*) FROM DELAY_CAUSE_TABLE;

-- SAMPLE DATA
SELECT * FROM MONTHLY_AIRLINE_API ;
SELECT * FROM ANNUAL_ROUTE_PERFORMANCE;
SELECT * FROM AIRPORT_DEPARTURE_KPI;
SELECT * FROM DELAY_CAUSE_TABLE;

--01/05/2026

CREATE OR REPLACE SCHEMA flight_analytics;