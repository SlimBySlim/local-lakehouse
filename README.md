# Local lakehouse project

Data engineering project for building lakehouse pipeline at home:

API → Airflow → MinIO → Iceberg → Trino → dbt → Superset

## Goals

- Ingest data from public API (Bybit)
- Store raw data in MinIO (local alternative to S3)
- Manage tables with Apache Iceberg
- Query data through Trino
- Transform data with dbt
- Visualize results in Superset

## Stack

- Docker Compose
- Apache Airflow
- MinIO
- Apache Iceberg
- Trino
- dbt
- Apache Superset