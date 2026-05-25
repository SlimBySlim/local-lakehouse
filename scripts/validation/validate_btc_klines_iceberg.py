import os
from datetime import datetime, timedelta

import trino


TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "airflow")


def get_target_date() -> str:
    data_date = os.getenv("DATA_DATE")

    if not data_date:
        raise ValueError("DATA_DATE environment variable is required")

    return data_date


def connect_to_trino():
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog="iceberg",
        schema="crypto",
    )


def main() -> None:
    target_date = get_target_date()

    conn = connect_to_trino()
    cursor = conn.cursor()

    query = f"""
        SELECT count(*)
        FROM iceberg.crypto.btcusdt_klines
        WHERE DATE(timestamp) = DATE '{target_date}'
    """

    print(f"Validating data for date: {target_date}")
    cursor.execute(query)

    row_count = cursor.fetchone()[0]

    print(f"Rows found: {row_count}")

    if row_count == 0:
        raise ValueError(f"No rows found for date {target_date}")

    print("Validation passed")


if __name__ == "__main__":
    main()