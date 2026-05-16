from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
import trino


BYBIT_KLINE_URL = "https://api.bybit.com/v5/market/kline"

SYMBOL = "BTCUSDT"
CATEGORY = "spot"
INTERVAL = "1"
LIMIT = 1000


def fetch_klines() -> pd.DataFrame:
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=24)

    params = {
        "category": CATEGORY,
        "symbol": SYMBOL,
        "interval": INTERVAL,
        "start": int(start.timestamp() * 1000),
        "end": int(now.timestamp() * 1000),
        "limit": LIMIT,
    }

    response = requests.get(BYBIT_KLINE_URL, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    if data.get("retCode") != 0:
        raise RuntimeError(f"Bybit API error: {data}")

    rows = data["result"]["list"]

    df = pd.DataFrame(
        rows,
        columns=[
            "timestamp_ms",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "turnover",
        ],
    )

    df["timestamp"] = pd.to_datetime(
        df["timestamp_ms"].astype("int64"),
        unit="ms",
        utc=True,
    )

    numeric_columns = ["open", "high", "low", "close", "volume", "turnover"]
    df[numeric_columns] = df[numeric_columns].astype("float64")

    df["symbol"] = SYMBOL
    df["category"] = CATEGORY
    df["interval"] = INTERVAL

    df = df[
        [
            "timestamp",
            "symbol",
            "category",
            "interval",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "turnover",
        ]
    ]

    df = df.sort_values("timestamp").reset_index(drop=True)

    return df


def get_trino_connection():
    return trino.dbapi.connect(
        host="localhost",
        port=8080,
        user="lakehouse",
        catalog="iceberg",
        schema="raw",
    )


def insert_to_iceberg(df: pd.DataFrame) -> None:
    conn = get_trino_connection()
    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO iceberg.raw.btc_klines (
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
                row["timestamp"].to_pydatetime(),
                row["symbol"],
                row["category"],
                row["interval"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row["volume"],
                row["turnover"],
            )
        )

    cursor.executemany(insert_sql, rows)

    cursor.close()
    conn.close()

    print(f"Inserted {len(rows)} rows into iceberg.raw.btc_klines")


def main():
    df = fetch_klines()

    print(df.head())
    print(df.dtypes)
    print(f"Rows: {len(df)}")
    print(f"Min timestamp: {df['timestamp'].min()}")
    print(f"Max timestamp: {df['timestamp'].max()}")

    insert_to_iceberg(df)


if __name__ == "__main__":
    main()