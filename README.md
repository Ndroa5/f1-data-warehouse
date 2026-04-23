# F1 Data Warehouse 🏎️

A complete data warehouse system for Formula 1 data covering the 2012–2023 seasons, built using modern Data Engineering practices and technologies.

## Overview

The raw dataset contains **518,417 rows and 76 columns** in a flat CSV format. The goal of this project was to transform that raw data into a structured, analytics-ready data warehouse using a Medallion Architecture, with a fully automated ETL pipeline orchestrated by Apache Airflow.

## Architecture

This project implements the **Medallion Architecture** with three layers, each stored as a separate schema within a single PostgreSQL database (`f1_warehouse`):

```
Bronze → Silver → Gold
```

| Layer | Schema | Description |
|-------|--------|-------------|
| **Bronze** | `bronze` | Raw data ingested 1:1 from CSV. All columns are TEXT — no transformations, no data loss. |
| **Silver** | `silver` | Cleaned data with proper types and NULL handling. `\N` artifacts replaced with real NULLs, columns cast to appropriate types (INTEGER, FLOAT, DATE, TIME). |
| **Gold** | `gold` | Star schema optimized for analytics and business intelligence. Deduplicated, with dimension and fact tables. |

## Star Schema (Gold Layer)

### Dimension Tables
- `dim_driver` — 65 unique drivers
- `dim_constructor` — 20 teams
- `dim_circuit` — 34 circuits
- `dim_race` — 232 races
- `dim_status` — 65 status types
- `dim_date` — generated from race dates

### Fact Tables
- `fact_results` — 4,502 race results
- `fact_lap_times` — 256,836 lap time records
- `fact_pit_stops` — 8,975 pit stop records
- `fact_driver_standings` — 4,502 driver standing records
- `fact_constructor_standings` — 2,398 constructor standing records

## Tech Stack

| Tool | Purpose |
|------|---------|
| **Python** | Primary programming language |
| **Apache Airflow** | Pipeline orchestration and scheduling |
| **Docker & Docker Compose** | Airflow deployment |
| **PostgreSQL 18** | Data warehouse storage |
| **SQLAlchemy** | ORM and database communication |
| **pandas** | CSV ingestion |
| **pgAdmin** | Database management UI |
| **Git & GitHub** | Version control and collaboration |

## Pipeline (Airflow DAG)

The ETL pipeline is fully automated via an Airflow DAG (`f1_etl_pipeline`) with **12 tasks** running in the following order:

```
load_bronze >> load_silver >> [dim_driver, dim_constructor, dim_circuit, dim_status, dim_date]
                                                    ↓
                                               dim_race
                                                    ↓
             [fact_results, fact_lap_times, fact_pit_stops, fact_driver_standings, fact_constructor_standings]
```

DIM tables (except `dim_race`) run **in parallel**. FACT tables also run **in parallel** after `dim_race` completes.

### Key Design Decisions

- **SCD Type 1 (UPSERT)** — Gold layer uses `INSERT ... ON CONFLICT DO UPDATE` instead of TRUNCATE + reload, ensuring only changed records are updated without touching unchanged data
- **Idempotency** — Pipeline can be run any number of times and always produces the same result
- **Logging** — Python `logging` module used throughout instead of `print()`, with timestamps and log levels visible in Airflow UI
- **Error handling** — All load functions wrapped in `try/except` blocks that log errors and re-raise for Airflow to handle

## Project Structure

```
f1-data-warehouse/
├── bronze/
│   ├── model.py        ← SQLAlchemy ORM class (BronzeRawData)
│   ├── load.py         ← Reads CSV, loads into bronze.raw_data
│   └── main.py         ← Orchestrates table creation and load
├── silver/
│   ├── model.py        ← SQLAlchemy ORM class (SilverRawData)
│   ├── load.py         ← DROP + CREATE TABLE AS SELECT from bronze
│   └── main.py         ← Orchestrates silver pipeline
├── gold/
│   ├── model.py        ← SQLAlchemy ORM classes for DIM and FACT tables
│   ├── load.py         ← Individual UPSERT functions per table
│   └── main.py         ← Orchestrates gold pipeline
├── sql/
│   └── ddl.sql         ← Original DDL definitions
├── docker-compose.yaml ← Airflow Docker Compose configuration
├── f1_etl_dag.py       ← Airflow DAG definition
└── .gitignore
```

## How to Run

### Prerequisites
- Docker & Docker Compose
- PostgreSQL instance with `f1_warehouse` database
- `bronze`, `silver`, and `gold` schemas created

### Setup

1. Clone the repository:
```bash
git clone https://github.com/Ndroa5/f1-data-warehouse.git
cd f1-data-warehouse
```

2. Start Airflow:
```bash
cd airflow-docker
docker compose up -d
```

3. Open Airflow UI at `http://localhost:8080` (credentials: `airflow/airflow`)

4. Trigger the `f1_etl_pipeline` DAG manually

## Key Insights (from BI validation queries)

- **Lewis Hamilton** — 86 wins, most successful driver of the 2012–2023 period
- **Mercedes** — 116 team wins, dominant constructor of the era
- **Max Verstappen** — World Champion in 2021, 2022, and 2023
- **Nico Rosberg** — broke Hamilton's title streak in 2016
