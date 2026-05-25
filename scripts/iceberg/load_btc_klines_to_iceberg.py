import os
from datetime import datetime
from pathlib import Path

import boto3
import pandas as pd
import trino


BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "raw")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "airflow")

SYMBOL = "btcusdt"
INTERVAL = "1m"

STAGING_TABLE = "iceberg.crypto.btcusdt_klines_staging"
FINAL_TABLE = "iceberg.crypto.btcusdt_klines"

def get_target_date() -> datetime:
    data_date = os.getenv("DATA_DATE")

    if not data_date:
        raise ValueError("DATA_DATE environment variable is required")

    return datetime.strptime(data_date, "%Y-%m-%d")
def get_target_s3_key() -> str:
    target_date = get_target_date()

    return (
        f"crypto/{SYMBOL}/klines/"
        f"interval={INTERVAL}/"
        f"year={target_date:%Y}/"
        f"month={target_date:%m}/"
        f"day={target_date:%d}/"
        f"{SYMBOL}_{INTERVAL}_{target_date:%Y%m%d}.parquet"
    )


def download_parquet_from_minio(s3_key: str) -> Path:
    local_path = Path(f"/tmp/{SYMBOL}_{INTERVAL}.parquet")

    s3_client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

    print(f"Downloading s3://{BUCKET_NAME}/{s3_key} to {local_path}")
    s3_client.download_file(BUCKET_NAME, s3_key, str(local_path))

    return local_path


def connect_to_trino():
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog="iceberg",
        schema="crypto",
    )


def create_schema_and_table(cursor) -> None:
    cursor.execute("CREATE SCHEMA IF NOT EXISTS iceberg.crypto")

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS iceberg.crypto.btcusdt_klines (
            timestamp TIMESTAMP(6),
            symbol VARCHAR,
            category VARCHAR,
            interval VARCHAR,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            turnover DOUBLE
        )
        WITH (
            format = 'PARQUET',
            partitioning = ARRAY['day(timestamp)']
        )
        """
    )


def insert_dataframe(cursor, df: pd.DataFrame, target_table: str) -> None:
    insert_sql = f"""
        INSERT INTO {target_table} (
            timestamp,
            symbol,
            category,
            interval,
            open,
            high,
            low,
            close,
            volume,
            turnover
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    rows = []

    for _, row in df.iterrows():
        rows.append(
            (
                row["timestamp"].to_pydatetime().replace(tzinfo=None),
                str(row["symbol"]),
                str(row["category"]),
                str(row["interval"]),
                float(row["open"]),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
                float(row["volume"]),
                float(row["turnover"]),
            )
        )

    print(f"Inserting {len(rows)} rows into Iceberg table")
    cursor.executemany(insert_sql, rows)

def recreate_staging_table(cursor) -> None:
    cursor.execute(f"DROP TABLE IF EXISTS {STAGING_TABLE}")

    cursor.execute(f"""
        CREATE TABLE {STAGING_TABLE} (
            timestamp TIMESTAMP(6),
            symbol VARCHAR,
            category VARCHAR,
            interval VARCHAR,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            turnover DOUBLE
        )
        WITH (
            format = 'PARQUET'
        )
    """)

def insert_from_staging_to_final(cursor) -> None:
    cursor.execute(f"""
        INSERT INTO {FINAL_TABLE}
        SELECT
            timestamp,
            symbol,
            category,
            interval,
            open,
            high,
            low,
            close,
            volume,
            turnover
        FROM {STAGING_TABLE}
    """)

def main() -> None:
    s3_key = get_target_s3_key()
    local_parquet_path = download_parquet_from_minio(s3_key)

    df = pd.read_parquet(local_parquet_path)

    print("Loaded dataframe:")
    print(df.head())
    print(df.dtypes)

    conn = connect_to_trino()
    cursor = conn.cursor()

    create_schema_and_table(cursor)
    recreate_staging_table(cursor)
    insert_dataframe(cursor, df, target_table=STAGING_TABLE)
    insert_from_staging_to_final(cursor)

    cursor.execute(f"DROP TABLE IF EXISTS {STAGING_TABLE}")

    print("Data successfully loaded to Iceberg")


if __name__ == "__main__":
    main()