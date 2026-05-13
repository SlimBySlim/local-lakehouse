import os
from io import BytesIO
from datetime import datetime, timedelta, timezone

import boto3
import pandas as pd
import requests
from dotenv import load_dotenv


load_dotenv()

BYBIT_KLINE_URL = "https://api.bybit.com/v5/market/kline"

SYMBOL = "BTCUSDT"
CATEGORY = "spot"
INTERVAL = "1"  # 1 minute candles
LIMIT = 1000


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )


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

    df["timestamp"] = pd.to_datetime(df["timestamp_ms"].astype("int64"), unit="ms", utc=True)

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


def upload_parquet_to_s3(df: pd.DataFrame) -> None:
    bucket_name = os.getenv("S3_BUCKET_RAW")

    if not bucket_name:
        raise RuntimeError("S3_BUCKET_RAW is not set")

    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    dt = df["timestamp"].min()
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")

    file_name = f"{SYMBOL.lower()}_{INTERVAL}m_{year}{month}{day}.parquet"

    object_key = (
        f"crypto/{SYMBOL.lower()}/klines/"
        f"interval={INTERVAL}m/"
        f"year={year}/month={month}/day={day}/"
        f"{file_name}"
    )

    s3 = get_s3_client()

    s3.upload_fileobj(
        buffer,
        bucket_name,
        object_key,
    )

    print(f"Uploaded {len(df)} rows")
    print(f"s3://{bucket_name}/{object_key}")


def main():
    df = fetch_klines()

    print(df.head())
    print(df.dtypes)
    print(f"Rows: {len(df)}")
    print(f"Min timestamp: {df['timestamp'].min()}")
    print(f"Max timestamp: {df['timestamp'].max()}")

    upload_parquet_to_s3(df)


if __name__ == "__main__":
    main()