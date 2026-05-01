# ✈️ US Flight Analytics — Data Engineering Pipeline

> An end-to-end data engineering project processing **2.7 million+** US domestic flight records from the Bureau of Transportation Statistics (BTS), built on a **Medallion Architecture** (Bronze → Silver → Gold) using PySpark on Databricks, AWS S3, Delta Lake, and Snowflake.

---

##  Table of Contents

- [Overview](#overview)
- [Tech Stack](#tech-stack)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Pipeline Layers](#pipeline-layers)
  - [Bronze Layer](#-bronze-layer)
  - [Silver Layer](#-silver-layer)
  - [Gold Layer](#-gold-layer)
  - [Snowflake Warehouse](#-snowflake-warehouse)
- [Gold Tables Overview](#gold-tables-overview)
- [Key Stats](#key-stats)
- [Setup & Usage](#setup--usage)

---

## Overview

This project ingests raw US domestic flight data from S3, transforms it through a multi-layer Delta Lake pipeline, and loads analytical Gold tables into Snowflake for BI/reporting. The pipeline handles data quality validation, feature engineering, geo-coordinate enrichment, and automated ingestion using Snowpipes.

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| **Apache Spark (PySpark)** | Distributed data processing |
| **Databricks** | Spark compute (Serverless + Photon Engine) |
| **AWS S3** | Raw & transformed data storage |
| **Delta Lake** | ACID transactions, schema enforcement, time travel |
| **Snowflake** | Cloud data warehouse & BI layer |
| **Snowpipe** | Auto-ingest from S3 to Snowflake |

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│               AWS S3 (Data Lake)                    │
│                                                     │
│  BTS CSV Data                                       │
│       │                                             │
│       ▼                                             │
│  ┌──────────────┐                                   │
│  │ BRONZE LAYER │  Raw CSV → Delta                  │
│  │  ~110 cols   │  Schema + basic validation        │
│  └──────┬───────┘                                   │
│         │                                           │
│         ▼                                           │
│  ┌──────────────┐                                   │
│  │ SILVER LAYER │  Cleaned + Enriched Delta         │
│  │   72 cols    │  Feature engineering, joins,      │
│  │              │  Haversine distance, DQ flags     │
│  └──────┬───────┘                                   │
│         │                                           │
│         ▼                                           │
│  ┌──────────────┐                                   │
│  │  GOLD LAYER  │  4 Analytical Delta Tables        │
│  │  (Parquet)   │  Aggregated KPIs                  │
│  └──────┬───────┘                                   │
└─────────┼───────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────┐
│       SNOWFLAKE         │
│  External Stage (S3)    │
│  Snowpipes (AUTO INGEST)│
│  4 Warehouse Tables     │
└─────────────────────────┘
```

---

## Project Structure

```
us-flight-project/
│
├── US_Flight_Project.ipynb       # Bronze Layer — Raw ingestion & validation
├── US-flight-2.ipynb             # Silver Layer — Transformation & enrichment
├── US_flight_3.ipynb             # Gold Layer — Aggregations & KPIs
├── flight_project.sql            # Snowflake — DDL, Snowpipes, validation
│
└── README.md                     # This file
```

**S3 Bucket Layout:**
```
s3://us-flight-project/bts_flight_data/
├── flight/          ← Raw BTS CSV files
├── airport/         ← Airport reference JSON
├── bronze/          ← Bronze Delta table
├── silver/          ← Silver Delta table (partitioned by year/month)
└── gold/
    ├── gold_airline/
    ├── gold_route_kpi/
    ├── gold_airport_departure_kpi/
    └── gold_delay_cause/
```

---

## Pipeline Layers

### 🥉 Bronze Layer
**Notebook:** `US_Flight_Project.ipynb`

Reads raw CSVs from S3 and creates the foundational Delta table.

- Loads **2,736,840** flight records from S3
- Inspects schema (110 columns), checks for corrupt records
- Filters to year **2021** subset (1,138,372 records)
- Validates data using `FAILFAST` mode — confirmed malformed records
- Writes clean Delta table to `s3://.../bronze/`

---

### 🥈 Silver Layer
**Notebook:** `US-flight-2.ipynb`

Enriches and cleans the Bronze data — the heaviest transformation stage.

**Data Quality:**
- Adds null-flag columns for 7 critical fields
- Labels all rows as `VALID` / `INVALID` → **100% VALID** (2,736,840 rows)
- Window-based deduplication → **0 duplicates** found

**Feature Engineering (28 new columns):**
- Time standardization — `CRSDepTime` / `CRSArrTime` → `HH:MM` format
- HHMM → total minutes conversion (null-safe)
- Boolean flags: `is_cancelled`, `is_diverted`, `is_departure_delayed`, `is_arrival_delayed`
- Arrival delay buckets: `Early` / `On Time` / `Minor Delay` / `Major Delay` / `Severe Delay`
- Date parts: `flight_year`, `flight_month`, `flight_quarter`, `day_of_week`
- Cancellation reason lookup (A/B/C/D codes → labels)

**Geo Enrichment:**
- Broadcast joins with 29,305 airport records → origin & destination `lat/lon`
- **Haversine great-circle distance** calculation in km

**Output:** 72-column Delta table, partitioned by `year` / `month`

---

###  Gold Layer
**Notebook:** `US_flight_3.ipynb`

Produces 4 analytical aggregation tables from the Silver data.

| Table | Granularity | Key Metrics |
|-------|-------------|-------------|
| `gold_airline` | Year, Month, Airline | Avg/total distance, on-time %, cancellation %, monthly rank |
| `gold_route_kpi` | Year, Route | Delay, on-time %, airlines per route, flight count |
| `gold_airport_departure_kpi` | Year, Month, Airport | Departures, cancellations, avg delay, geo coordinates |
| `gold_delay_cause` | Year, Month, Airline | Weather / Carrier / Security / Late Aircraft delay % |

---

###  Snowflake Warehouse
**File:** `flight_project.sql`

Loads Gold Parquet data from S3 into Snowflake for analytics and reporting.

- **Storage Integration** — IAM role-based S3 access
- **External Stage** — points to Gold Delta Parquet folder
- **4 target tables** created with explicit schemas
- **Manual COPY INTO** — one-time historical bulk load
- **4 Snowpipes** with `AUTO_INGEST = TRUE` — event-driven ongoing loads
- **Pipe REFRESH** — backfills files present before pipe creation
- **Validation queries** — row counts + sample data checks

---

## Gold Tables Overview

### 1. MONTHLY_AIRLINE_API
```
flight_year | flight_month | airline_code | reporting_airline |
avg_distance_travelled | total_distance_travelled
```

### 2. ANNUAL_ROUTE_PERFORMANCE
```
flight_year | route | origin_code | destination_code | number_of_flights |
avg_arrival_delay | avg_distance_travelled | total_delayed_flights |
total_on_time_flights | number_of_airlines_on_route |
on_time_percentage_airline_percentage
```

### 3. AIRPORT_DEPARTURE_KPI
```
flight_year | flight_month | origin_code | name | city | state | lon | lat |
total_departure | total_cancelled_departure | avg_delayed_departure |
total_on_time_departure | avg_route_distance | number_of_flights_operating |
avg_airtime | departure_on_time_percentage | year_month
```

### 4. DELAY_CAUSE_TABLE
```
flight_year | flight_month | airline_code | total_minutes_delayed |
total_weather_delayed_minutes | total_carrier_delayed_minutes |
total_security_delayed_minutes | total_late_aircraft_delayed_minutes |
weather_delay_percentage | carrier_delay_percentage |
security_delay_percentage | late_aircraft_delay_percentage
```

---

## Key Stats

| Metric | Value |
|--------|-------|
| Total Raw Flight Records | **2,736,840** |
| 2021 Flight Records | **1,138,372** |
| Distinct Airlines | **17** |
| Airport Reference Records | **29,305** |
| Data Quality Pass Rate | **100% VALID** |
| Clean Flights (not cancelled / not delayed) | **2,210,584** |
| Silver Layer Columns | **72** |
| Gold Analytical Tables | **4** |
| Snowflake Pipes | **4** |

---

## Setup & Usage

### Prerequisites

- Databricks workspace (Serverless or Classic cluster)
- AWS S3 bucket with BTS flight CSV data
- Snowflake account with `ACCOUNTADMIN` or `SYSADMIN` role
- IAM Role configured for Snowflake ↔ S3 access

### Steps

1. **Bronze** — Run `US_Flight_Project.ipynb` on Databricks to ingest raw CSVs into a Delta table.

2. **Silver** — Run `US-flight-2.ipynb` to apply transformations, enrichment, and write the Silver Delta table partitioned by `year/month`.

3. **Gold** — Run `US_flight_3.ipynb` to generate the 4 Gold aggregation tables and write them as Parquet to S3.

4. **Snowflake** — Execute `flight_project.sql` in Snowflake:
   - Create storage integration and stage
   - Create target tables
   - Run `COPY INTO` for initial load
   - Create Snowpipes for ongoing auto-ingestion

---

>  For full code, transformations, and outputs — see [`US_Flight_Project_Complete_Pipeline.md`](./US_Flight_Project_Complete_Pipeline.md)
