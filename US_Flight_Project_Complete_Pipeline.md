# US Flight Project — Complete Data Engineering Pipeline

**Platform:** Databricks (PySpark) + Snowflake | **Storage:** AWS S3 (Delta Lake) | **Source:** Bureau of Transportation Statistics (BTS)

---

## Table of Contents

1. [Project Architecture](#architecture)
2. [Bronze Layer — Raw Ingestion](#bronze-layer)
3. [Silver Layer — Transformation & Enrichment](#silver-layer)
4. [Gold Layer — Aggregations & Analytics](#gold-layer)
5. [Snowflake — Data Warehouse](#snowflake-layer)

---

## Project Architecture

```
BTS Flight CSV Data (S3)
         │
         ▼
  ┌─────────────┐
  │ BRONZE LAYER│  Raw CSV → Delta (schema applied, basic validation)
  └──────┬──────┘
         │
         ▼
  ┌─────────────┐
  │ SILVER LAYER│  Enrichment, feature engineering, airport join, Haversine distance
  └──────┬──────┘
         │
         ▼
  ┌─────────────┐
  │  GOLD LAYER │  4 analytical aggregation tables → Delta Parquet on S3
  └──────┬──────┘
         │
         ▼
  ┌─────────────┐
  │  SNOWFLAKE  │  External Stage + Snowpipes → 4 warehouse tables
  └─────────────┘
```

| Layer | Notebook / File | Records | Columns |
|-------|----------------|---------|---------|
| Bronze | `US_Flight_Project.ipynb` | 2,736,840 | ~110 |
| Silver | `US-flight-2.ipynb` | 2,736,840 | 72 |
| Gold | `US_flight_3.ipynb` | Aggregated | 4 tables |
| Warehouse | `flight_project.sql` | Loaded via Snowpipe | 4 tables |

---

## Bronze Layer

> **Notebook:** `US_Flight_Project.ipynb`

### S3 Data Source

```python
dbutils.fs.ls("s3://us-flight-project/bts_flight_data/")
```

```
[FileInfo(path='.../airport/', ...), FileInfo(path='.../bronze/', ...),
 FileInfo(path='.../flight/', ...), FileInfo(path='.../gold_airline/', ...),
 FileInfo(path='.../silver/', ...)]
```

### Load Raw CSV

```python
df = spark.read.csv("s3://us-flight-project/bts_flight_data/flight/", header=True)
df.count()
```

**Output:** `2,736,840` total flight records

### Dataset Columns (110 fields)

```
year, Quarter, month, DayofMonth, DayOfWeek, FlightDate,
Reporting_Airline, DOT_ID_Reporting_Airline, IATA_CODE_Reporting_Airline,
Tail_Number, Flight_Number_Reporting_Airline,
OriginAirportID, OriginAirportSeqID, OriginCityMarketID,
Origin, OriginCityName, OriginState, OriginStateFips, OriginStateName, OriginWac,
DestAirportID, DestAirportSeqID, DestCityMarketID,
Dest, DestCityName, DestState, DestStateFips, DestStateName, DestWac,
CRSDepTime, DepTime, DepDelay, DepDelayMinutes, DepDel15,
DepartureDelayGroups, DepTimeBlk, TaxiOut, WheelsOff, WheelsOn, TaxiIn,
CRSArrTime, ArrTime, ArrDelay, ArrDelayMinutes, ArrDel15,
ArrivalDelayGroups, ArrTimeBlk, Cancelled, CancellationCode, Diverted,
CRSElapsedTime, ActualElapsedTime, AirTime, Flights, Distance, DistanceGroup,
CarrierDelay, WeatherDelay, NASDelay, SecurityDelay, LateAircraftDelay,
FirstDepTime, TotalAddGTime, LongestAddGTime, DivAirportLandings,
DivReachedDest, DivActualElapsedTime, DivArrDelay, DivDistance,
Div1–Div5 fields (Airport, AirportID, AirportSeqID, WheelsOn, TotalGTime,
LongestGTime, WheelsOff, TailNum), _c109
```

### Schema (printSchema output)

```
root
 |-- year: integer (nullable = true)
 |-- Quarter: string (nullable = true)
 |-- month: integer (nullable = true)
 |-- DayofMonth: string (nullable = true)
 |-- DayOfWeek: string (nullable = true)
 |-- FlightDate: string (nullable = true)
 |-- Reporting_Airline: string (nullable = true)
 |-- IATA_CODE_Reporting_Airline: string (nullable = true)
 |-- Flight_Number_Reporting_Airline: string (nullable = true)
 |-- Origin / Dest (and all related fields): string (nullable = true)
 |-- DepDelay / ArrDelay / all delay fields: string (nullable = true)
 |-- Cancelled, Diverted: string (nullable = true)
 ... (110 columns total)
```

### Corrupt Record Handling (Explored, Commented Out)

```python
# from pyspark.sql.functions import when, col, concat_ws
# fligt_df = df.withColumn(
#     "_corrupt_record",
#     when(col("Flight_Number_Reporting_Airline").isNull(),
#          concat_ws(",", *df.columns)).otherwise(None)
# )
```

> A `_corrupt_record` column approach was explored to flag rows where `Flight_Number_Reporting_Airline` is null.

### Year Filter — 2021

```python
df_year_2021.count()  # → 1,138,372
```

### FAILFAST Mode Test

```python
df_failfast = spark.read.format("csv") \
    .option("header", "true") \
    .option("nullValue", "") \
    .option("mode", "FAILFAST") \
    .schema(schema) \
    .load("s3://us-flight-project/bts_flight_data/flight/year=2021/")
```

**Result:** `SparkException` — confirmed malformed records exist in the 2021 data.

### Bronze Delta Count

```python
df_bronze.count()  # Final Bronze layer row count
```

---

## Silver Layer

> **Notebook:** `US-flight-2.ipynb`

### Airport Reference Data

```python
df_airport = spark.read.option("multiLine", "true") \
    .schema(airport_schema) \
    .json("s3://us-flight-project/bts_flight_data/airport/")

df_airport.count()  # → 29,305
```

**Airport Schema:**
```
city, code, country, elevation (long), iata, icao,
lat (double), lon (double), name, state, tz
```

Airport data written to Delta and filtered to US-only airports:

```python
df_airport.write.format("delta").mode("overwrite") \
    .save("s3://us-flight-project/bts_flight_data/airport/delta/")

df_airport = df_airport.filter(col("country") == "US")
```

### Load Bronze Delta Table

```python
df = spark.read.format("delta") \
    .load("s3://us-flight-project/bts_flight_data/bronze/")
```

**17 Airlines in dataset:** `YX, AS, B6, UA, YV, OH, HA, QX, G4, WN, F9, 9E, NK, DL, AA, OO, MQ`

### Data Quality — Null Flags

```python
df = df.withColumn("flightnull", when(col("DepDelay").isNull(), 1).otherwise(0)) \
       .withColumn("originnull", when(col("Origin").isNull(), 1).otherwise(0)) \
       .withColumn("total_nulls", col("flightnull") + col("originnull"))

# Aggregate all 7 quality flag columns
df = df.withColumn(
    "total_null_count",
    col("FlightDate_null") + col("Reporting_Airline_null") +
    col("Origin_null") + col("Dest_null") +
    col("dep_delay_flag") + col("same_origin_destination") +
    col("is_domestic_flag")
)

# Label rows VALID / INVALID
df_final = df.withColumn(
    "is_data_clean",
    when(col("total_null_count") <= 2, "VALID").otherwise("INVALID")
)
```

**Result:**
```
+-------------+-------+
|is_data_clean|  count|
+-------------+-------+
|        VALID|2736840|
+-------------+-------+
```

> All **2,736,840** rows are VALID — 100% pass rate.

### Deduplication via Window Function

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy(
    "FlightDate", "Reporting_Airline",
    "Flight_Number_Reporting_Airline", "Origin", "Dest"
).orderBy("FlightDate")

df_window = df.withColumn("row_num", row_number().over(window_spec))
```

**Result:** All rows have `row_num = 1` — no duplicates.

### Feature Engineering

#### Standardized Time Format (HH:MM)

```python
hours = floor(col("CRSDepTime") / 100).cast("long")
minutes = col("CRSDepTime") % 100

df = df.withColumn("crs_dep_time_std",
    concat(
        when(hours < 10, lit("0")).otherwise(lit("")), hours.cast("string"),
        lit(":"),
        when(minutes < 10, lit("0")).otherwise(lit("")), minutes.cast("string")
    )
)
# Same logic → crs_arr_time_std
```

#### HHMM to Total Minutes (Null-Safe Version)

```python
def hours_to_minutes_safe(column):
    return when(column.isNull(), None) \
           .when(column == 2400, 0) \
           .otherwise(floor(column / 100) * 60 + column % 100)

df = df \
    .withColumn("crs_dep_minutes_safe", hours_to_minutes_safe(col("CRSDepTime"))) \
    .withColumn("crs_arr_minutes_safe", hours_to_minutes_safe(col("CRSArrTime")))
```

#### Boolean Flag Columns

```python
df = df \
    .withColumn("is_cancelled",         when(col("Cancelled") == 1, True).otherwise(False)) \
    .withColumn("is_diverted",          when(col("Diverted") == 1, True).otherwise(False)) \
    .withColumn("is_departure_delayed", when(col("DepDel15") == 1, True).otherwise(False)) \
    .withColumn("is_arrival_delayed",   when(col("ArrivalDelayGroups") > 0, True).otherwise(False))
```

#### Arrival Delay Category

| Condition | Label |
|-----------|-------|
| Cancelled == 1 | `Cancelled` |
| ArrDelayMinutes < 0 | `Early` |
| 0 – 14 min | `On Time` |
| 15 – 44 min | `Minor Delay` |
| 45 – 120 min | `Major Delay` |
| > 120 min | `Severe Delay` |

```python
df = df.withColumn(
    "arrival_delay_category_bucket",
    when(col("Cancelled") == 1, "Cancelled")
    .when(col("ArrDelayMinutes") < 0, "Early")
    .when((col("ArrDelayMinutes") >= 0)  & (col("ArrDelayMinutes") <= 14),  "On Time")
    .when((col("ArrDelayMinutes") >= 15) & (col("ArrDelayMinutes") <= 44),  "Minor Delay")
    .when((col("ArrDelayMinutes") >= 45) & (col("ArrDelayMinutes") <= 120), "Major Delay")
    .when(col("ArrDelayMinutes") > 120, "Severe Delay")
    .otherwise(None)
)
```

#### Date Part Extraction

```python
df = df \
    .withColumn("flight_year",    year(col("FlightDate"))) \
    .withColumn("flight_month",   month(col("FlightDate"))) \
    .withColumn("flight_quarter", quarter(col("FlightDate"))) \
    .withColumn("day_of_week",    dayofweek(col("FlightDate")))
```

#### Standardized Code Columns

```python
df = df \
    .withColumn("airline_code",     upper(col("IATA_CODE_Reporting_Airline"))) \
    .withColumn("origin_code",      upper(col("Origin"))) \
    .withColumn("destination_code", upper(col("Dest")))
```

#### Cancellation Reason Lookup

| Code | Reason |
|------|--------|
| A | Carrier |
| B | Weather |
| C | National_Security |
| D | Security_Concern |

### Coordinate Enrichment (Broadcast Joins)

```python
from pyspark.sql.functions import broadcast

# Origin coordinates
df_origin_coords = df_airport.select(
    col("iata").alias("origin_iata"),
    col("lat").alias("origin_lat"),
    col("lon").alias("origin_lon")
)
df_silver = df_silver.join(broadcast(df_origin_coords),
    df_silver["Origin"] == df_origin_coords["origin_iata"], "left"
).drop("origin_iata")

# Destination coordinates (same pattern → dest_lat, dest_lon)
```

> Broadcast join used because `df_airport` is small — avoids expensive shuffle across worker nodes.

### Haversine Distance Calculation

```python
from pyspark.sql.functions import acos, sin, cos, lit, round

PI = 3.141592653589793

def add_haversine_distance(df, lat1, lon1, lat2, lon2):
    return df.withColumn(
        "haversine_distance_km",
        round(
            acos(
                sin(col(lat1) * lit(PI/180)) * sin(col(lat2) * lit(PI/180)) +
                cos(col(lat1) * lit(PI/180)) * cos(col(lat2) * lit(PI/180)) *
                cos(col(lon2) * lit(PI/180) - col(lon1) * lit(PI/180))
            ) * lit(6371.0), 5
        )
    )
```

### Write Silver Delta Table

```python
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("delta.dataSkippingNumIndexedCols", 5) \
    .partitionBy("year", "month") \
    .save("s3://us-flight-project/bts_flight_data/silver/")
```

**Output:** `Total Columns Saved: 72 | Silver data saved successfully!`

**Final Silver Columns (72 total):**
```
FlightDate, Reporting_Airline, IATA_CODE_Reporting_Airline, Tail_Number,
Flight_Number_Reporting_Airline, Origin, OriginCityName, OriginState,
Dest, DestCityName, DestState, CRSDepTime, DepTime, DepDelay,
DepDelayMinutes, DepDel15, DepartureDelayGroups, CRSArrTime, ArrTime,
ArrDelayMinutes, ArrivalDelayGroups, Cancelled, CancellationCode,
Diverted, CRSElapsedTime, ActualElapsedTime, AirTime, Flights, Distance,
DistanceGroup, CarrierDelay, WeatherDelay, NASDelay, SecurityDelay,
LateAircraftDelay, FlightDate_null, Reporting_Airline_null, Origin_null,
Dest_null, dep_delay_flag, same_origin_destination, is_domestic_flag,
year, month, flightnull, originnull, total_nulls, total_null_count,
crs_dep_time_std, crs_arr_time_std, crs_dep_minutes, crs_arr_minutes,
crs_dep_minutes_safe, crs_arr_minutes_safe, is_cancelled, is_diverted,
is_departure_delayed, is_arrival_delayed, arrival_delay_category_bucket,
flight_year, flight_month, flight_quarter, day_of_week, airline_code,
origin_code, destination_code, cancellation_reason,
origin_lat, origin_lon, dest_lat, dest_lon, haversine_distance_km
```

---

## Gold Layer

> **Notebook:** `US_flight_3.ipynb`

### Load Silver Delta Table

```python
df_silver = spark.read.format("delta") \
    .load("s3://us-flight-project/bts_flight_data/silver/")

print("Total Rows   :", df_silver.count())   # → 2,736,840
print("Total Columns:", len(df_silver.columns))  # → 72
```

**Performance benchmark (Databricks Serverless / Photon):**
```
Without Cache : 1.21 seconds
With Cache    : 1.30 seconds
```

> On Serverless, Photon Engine already optimizes Delta reads — explicit caching showed negligible benefit. Temp View used as equivalent.

---

### Gold Table 1 — Monthly Airline KPI

**S3 Path:** `s3://us-flight-project/bts_flight_data/gold/gold_airline/`

**Grouping:** `flight_year`, `flight_month`, `airline_code`, `Reporting_Airline`

```python
from pyspark.sql.functions import count, sum, avg, round, when, col

df_gold = df_silver.groupBy(
    "flight_year", "flight_month", "airline_code", "Reporting_Airline"
).agg(
    round(avg(col("Distance")), 2).alias("avg_distance_travelled"),
    round(sum(col("Distance")), 2).alias("total_distance_travelled")
)
```

Additional metrics computed during exploration:

| Metric | Column | Logic |
|--------|--------|-------|
| Total flights | `total_flights` | `count(*)` |
| Delayed flights | `delayed_flights` | `is_arrival_delayed == True` |
| Cancelled flights | `total_flights_cancelled` | `is_cancelled == True` |
| Diverted flights | `total_diverted` | `is_diverted == True` |
| Avg arrival delay | `avg_arr_delay_minutes` | `avg(ArrDelayMinutes)` where not cancelled |
| Median arrival delay | `is_medium_arrival_delay` | `percentile_approx(ArrDelayMinutes, 0.5)` |
| On-time % | `on_time_flight_percentage` | `(on_time / total) * 100` |
| Cancelled % | `cancelled_flight_percentage` | `(cancelled / total) * 100` |

**Monthly rank added via Window:**
```python
window_spec = Window.partitionBy("flight_year", "flight_month") \
                    .orderBy(F.desc("airline_code"))

df_gold = df_gold.withColumn("monthly_rank", F.rank().over(window_spec))
```

**Clean flights (not cancelled + not delayed):** `2,210,584`

---

### Gold Table 2 — Annual Route Performance

**S3 Path:** `s3://us-flight-project/bts_flight_data/gold/gold_route_kpi/`

**Grouping:** `flight_year`, `route`, `origin_code`, `destination_code`

```python
from pyspark.sql.functions import concat_ws, countDistinct

# Route column
df_route_base = df_silver.withColumn(
    "route", concat_ws("-", col("origin_code"), col("destination_code"))
)

# On-time flag
df_route_base = df_route_base.withColumn(
    "on_time_flight",
    when((col("is_cancelled") == False) & (col("is_arrival_delayed") == False), 1).otherwise(0)
)

# KPI aggregation
df_gold_route_kpi = df_route_base.groupBy(
    "flight_year", "route", "origin_code", "destination_code"
).agg(
    count("*").alias("number_of_flights"),
    round(avg(when(col("is_cancelled") == False, col("ArrDelayMinutes"))), 2).alias("avg_arrival_delay"),
    round(avg("Distance"), 2).alias("avg_distance_travelled"),
    sum(when(col("is_arrival_delayed") == True, 1).otherwise(0)).alias("total_delayed_flights"),
    sum("on_time_flight").alias("total_on_time_flights"),
    countDistinct("airline_code").alias("number_of_airlines_on_route")
).withColumn(
    "on_time_percentage_airline_percentage",
    round((col("total_on_time_flights") / col("number_of_flights")) * 100, 2)
)
```

**Sample output:**
```
route    | flights | avg_delay | avg_dist  | delayed | on_time | airlines | on_time%
ORD-DLH  |      24 |      7.05 |     397.0 |       4 |      16 |        2 |    66.67
EWR-SFO  |    1076 |     12.70 |    2565.0 |     187 |     838 |        3 |    77.88
MSP-LAX  |     579 |      9.83 |    1535.0 |      94 |     482 |        2 |    83.25
MCO-BOS  |    1068 |     24.49 |    1121.0 |     313 |     708 |        5 |    66.29
```

---

### Gold Table 3 — Airport Departure KPI

**S3 Path:** `s3://us-flight-project/bts_flight_data/gold/gold_airport_departure_kpi`

**Grouping:** `flight_year`, `flight_month`, `origin_code`, `name`, `city`, `state`, `lon`, `lat`

```python
# Join Silver with Airport reference
df_cl_join = df_silver.join(df_airport,
    df_silver["origin_code"] == df_airport["iata"], "inner")

# Departure on-time flag
df_departure_base = df_cl_join.withColumn(
    "departure_on_time_flight",
    when((col("is_cancelled") == False) & (col("is_departure_delayed") == False), 1).otherwise(0)
)

# Aggregation
df_gold_airport_departure_kpi = df_departure_base.groupBy(
    "flight_year", "flight_month", "origin_code", "name", "city", "state", "lon", "lat"
).agg(
    count("*").alias("total_departure"),
    sum(when(col("is_cancelled") == True, 1).otherwise(0)).alias("total_cancelled_departure"),
    round(avg(when(col("is_cancelled") == False, col("DepDelayMinutes"))), 2).alias("avg_delayed_departure"),
    sum("departure_on_time_flight").alias("total_on_time_departure"),
    round(avg("Distance"), 2).alias("avg_route_distance"),
    count("*").alias("number_of_flights_operating"),
    round(avg(when(col("is_cancelled") == False, col("AirTime"))), 2).alias("avg_airtime")
).withColumn(
    "departure_on_time_percentage",
    round((col("total_on_time_departure") / col("total_departure")) * 100, 2)
).withColumn(
    "year_month", concat_ws("-", col("flight_year"), col("flight_month"))
)
```

**Sample output:**
```
airport | city        | state     | total_dep | cancelled | avg_delay | on_time%
ORF     | Norfolk     | Virginia  |     1,633 |       170 |     14.89 |    73.97
RHI     | Rhinelander | Wisconsin |        62 |         2 |     26.03 |    79.03
MGM     | Montgomery  | Alabama   |       210 |        11 |     19.27 |    73.81
```

---

### Gold Table 4 — Delay Cause Analysis

**S3 Path:** `s3://us-flight-project/bts_flight_data/gold/gold_delay_cause/`

**Grouping:** `flight_year`, `flight_month`, `airline_code`

```python
# Filter: non-cancelled + arrival delayed flights only
df_delay_base = df_silver.filter(
    (col("is_cancelled") == False) & (col("ArrDelayMinutes") > 0)
)

# Aggregate delay minutes by cause
df_delay_cause_table = df_delay_base.groupBy(
    "flight_year", "flight_month", "airline_code"
).agg(
    round(sum("ArrDelayMinutes"), 2).alias("total_minutes_delayed"),
    round(sum("WeatherDelay"), 2).alias("total_weather_delayed_minutes"),
    round(sum("CarrierDelay"), 2).alias("total_carrier_delayed_minutes"),
    round(sum("SecurityDelay"), 2).alias("total_security_delayed_minutes"),
    round(sum("LateAircraftDelay"), 2).alias("total_late_aircraft_delayed_minutes")
).withColumn("weather_delay_percentage",
    round((col("total_weather_delayed_minutes") / col("total_minutes_delayed")) * 100, 2)
).withColumn("carrier_delay_percentage",
    round((col("total_carrier_delayed_minutes") / col("total_minutes_delayed")) * 100, 2)
).withColumn("security_delay_percentage",
    round((col("total_security_delayed_minutes") / col("total_minutes_delayed")) * 100, 2)
).withColumn("late_aircraft_delay_percentage",
    round((col("total_late_aircraft_delayed_minutes") / col("total_minutes_delayed")) * 100, 2)
)
```

---

## Snowflake Layer

> **File:** `flight_project.sql`

### Database & Schema Setup

```sql
CREATE OR REPLACE DATABASE FLIGHT_ANALYSIS;
USE DATABASE FLIGHT_ANALYSIS;

CREATE OR REPLACE SCHEMA STAGING;
USE SCHEMA STAGING;

USE WAREHOUSE COMPUTE_WH;
```

### Storage Integration — S3 Access

```sql
CREATE OR REPLACE STORAGE INTEGRATION s3_flight_gold_int
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::686131354870:role/snowflake_role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://flight-analytics-de-divyansh-2026/gold_delta_folder/');
```

### File Format & External Stage

```sql
CREATE OR REPLACE FILE FORMAT parquet_ff TYPE = PARQUET;

CREATE OR REPLACE STAGE gold_stage_s3
    STORAGE_INTEGRATION = s3_flight_gold_int
    URL = 's3://flight-analytics-de-divyansh-2026/gold_delta_folder/'
    FILE_FORMAT = parquet_ff;

LIST @gold_stage_s3;
```

### Target Tables

#### Table 1 — Monthly Airline

```sql
CREATE OR REPLACE TABLE MONTHLY_AIRLINE_API (
    flight_year NUMBER, flight_month NUMBER,
    airline_code STRING, reporting_airline STRING,
    avg_distance_travelled FLOAT, total_distance_travelled FLOAT
);
```

#### Table 2 — Annual Route Performance

```sql
CREATE OR REPLACE TABLE ANNUAL_ROUTE_PERFORMANCE (
    flight_year NUMBER, route STRING,
    origin_code STRING, destination_code STRING,
    number_of_flights NUMBER, avg_arrival_delay FLOAT,
    avg_distance_travelled FLOAT, total_delayed_flights NUMBER,
    total_on_time_flights NUMBER, number_of_airlines_on_route NUMBER,
    on_time_percentage_airline_percentage FLOAT
);
```

#### Table 3 — Airport Departure KPI

```sql
CREATE OR REPLACE TABLE AIRPORT_DEPARTURE_KPI (
    flight_year NUMBER, flight_month NUMBER,
    origin_code STRING, name STRING, city STRING, state STRING,
    lon FLOAT, lat FLOAT, total_departure NUMBER,
    total_cancelled_departure NUMBER, avg_delayed_departure FLOAT,
    total_on_time_departure NUMBER, avg_route_distance FLOAT,
    number_of_flights_operating NUMBER, avg_airtime FLOAT,
    departure_on_time_percentage FLOAT, year_month STRING
);
```

#### Table 4 — Delay Cause

```sql
CREATE OR REPLACE TABLE DELAY_CAUSE_TABLE (
    flight_year NUMBER, flight_month NUMBER, airline_code STRING,
    total_minutes_delayed FLOAT,
    total_weather_delayed_minutes FLOAT,
    total_carrier_delayed_minutes FLOAT,
    total_security_delayed_minutes FLOAT,
    total_late_aircraft_delayed_minutes FLOAT,
    weather_delay_percentage FLOAT, carrier_delay_percentage FLOAT,
    security_delay_percentage FLOAT, late_aircraft_delay_percentage FLOAT
);
```

### Manual Load (One-Time Bulk COPY)

```sql
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
```

### Snowpipes — Auto Ingest

Snowpipes automatically load new Parquet files as they arrive in S3 (triggered via S3 event notifications):

```sql
CREATE OR REPLACE PIPE PIPE_MONTHLY_AIRLINE_API AUTO_INGEST = TRUE AS
    COPY INTO MONTHLY_AIRLINE_API FROM @gold_stage_s3/gold_airline/
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

CREATE OR REPLACE PIPE PIPE_ROUTE_KPI AUTO_INGEST = TRUE AS
    COPY INTO ANNUAL_ROUTE_PERFORMANCE FROM @gold_stage_s3/gold_route_kpi/
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

CREATE OR REPLACE PIPE PIPE_AIRPORT_KPI AUTO_INGEST = TRUE AS
    COPY INTO AIRPORT_DEPARTURE_KPI FROM @gold_stage_s3/gold_airport_departure_kpi/
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

CREATE OR REPLACE PIPE PIPE_DELAY_CAUSE AUTO_INGEST = TRUE AS
    COPY INTO DELAY_CAUSE_TABLE FROM @gold_stage_s3/gold_delay_cause/
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```

### Pipe Refresh (Historical Files)

```sql
ALTER PIPE PIPE_MONTHLY_AIRLINE_API REFRESH;
ALTER PIPE PIPE_ROUTE_KPI REFRESH;
ALTER PIPE PIPE_AIRPORT_KPI REFRESH;
ALTER PIPE PIPE_DELAY_CAUSE REFRESH;
```

### Pipe Status Check

```sql
SELECT SYSTEM$PIPE_STATUS('PIPE_MONTHLY_AIRLINE_API');
SELECT SYSTEM$PIPE_STATUS('PIPE_ROUTE_KPI');
SELECT SYSTEM$PIPE_STATUS('PIPE_AIRPORT_KPI');
SELECT SYSTEM$PIPE_STATUS('PIPE_DELAY_CAUSE');
```

### Data Validation

```sql
SELECT COUNT(*) FROM MONTHLY_AIRLINE_API;
SELECT COUNT(*) FROM ANNUAL_ROUTE_PERFORMANCE;
SELECT COUNT(*) FROM AIRPORT_DEPARTURE_KPI;
SELECT COUNT(*) FROM DELAY_CAUSE_TABLE;

SELECT * FROM MONTHLY_AIRLINE_API;
SELECT * FROM ANNUAL_ROUTE_PERFORMANCE;
SELECT * FROM AIRPORT_DEPARTURE_KPI;
SELECT * FROM DELAY_CAUSE_TABLE;
```

---

## Complete Pipeline Summary

| Layer | Tool | Input | Output | Records |
|-------|------|-------|--------|---------|
| **Bronze** | PySpark | BTS CSV (S3) | Delta Table | 2,736,840 |
| **Silver** | PySpark | Bronze Delta | Delta Table (partitioned year/month) | 2,736,840 × 72 cols |
| **Gold** | PySpark | Silver Delta | 4× Delta Tables | Aggregated |
| **Warehouse** | Snowflake SQL | Gold Parquet (S3) | 4× Snowflake Tables | Auto-ingested via Snowpipe |

| Key Statistic | Value |
|---------------|-------|
| Total Raw Flights | 2,736,840 |
| 2021 Flights | 1,138,372 |
| Airlines | 17 |
| Airport Records | 29,305 |
| Data Quality Pass Rate | 100% VALID |
| Clean Flights (not cancelled/delayed) | 2,210,584 |
| Silver Layer Columns | 72 |
| Gold Tables | 4 |
| Snowflake Pipes | 4 |
