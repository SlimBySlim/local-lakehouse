# Local Lakehouse Project

Modern data engineering pet project for building a local lakehouse pipeline using open-source technologies.

## Pipeline

API → MinIO → Spark → Iceberg → Trino/dbt → Superset

Airflow is used as the orchestration layer for pipeline execution.

---

## Goals

* Ingest market data from public APIs (Bybit)
* Store raw data in MinIO (local S3-compatible storage)
* Process batch data with Apache Spark
* Manage analytical tables with Apache Iceberg
* Query data through Trino
* Build transformations and marts with dbt
* Visualize results in Superset

---

## Stack

* Docker Compose
* Apache Airflow
* Apache Spark
* MinIO
* Apache Iceberg
* Trino
* dbt
* Apache Superset

---

## Architecture

* MinIO as S3-compatible object storage
* Apache Spark for distributed batch processing
* Apache Iceberg as table format and metadata layer
* Trino as distributed SQL query engine
* Apache Airflow for orchestration
* dbt for transformation and marts layer
* Superset for BI and visualization

---

## Data Flow

```text
Bybit API
    ↓
 Airflow
    ↓
MinIO (raw parquet)
    ↓
Apache Spark
    ↓
Iceberg tables
    ↓
Trino + dbt
    ↓
Superset
```

---

## Storage Layout

MinIO is used as local S3-compatible object storage.

### Buckets

* `raw` — raw data ingested from external APIs
* `iceberg` — Iceberg warehouse and table metadata

---

## Current Pipeline Status

* [x] Raw ingest to MinIO
* [x] Validation layer
* [x] Spark batch ingest
* [x] Iceberg integration
* [x] Trino access
* [x] dbt marts
* [ ] Incremental loads
* [ ] Airflow Spark orchestration

---

## Features Implemented

* Batch ingestion from Bybit API
* Raw parquet storage in MinIO
* Spark-based distributed transformations
* Iceberg table creation and management
* SQL querying through Trino
* dbt transformation layer
* Local lakehouse architecture with Docker Compose
