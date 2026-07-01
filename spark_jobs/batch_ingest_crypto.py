from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp
import argparse
from datetime import datetime
import os


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("batch-ingest-crypto")
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.2")
        .config("spark.sql.legacy.parquet.nanosAsLong", "true")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.hadoop:hadoop-aws:3.4.2",
                "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0",
            ])
        )
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", "s3a://raw/iceberg/")
        .getOrCreate()
    )

def valid_date(value):
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return value
    except ValueError:
        raise argparse.ArgumentTypeError(
            "Date must be in YYYY-MM-DD format."
        )

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        required=True,
        type=valid_date
    )
    return parser.parse_args()

def build_input_path(data_date: str):
    dt = datetime.strptime(data_date, "%Y-%m-%d")

    return (
        "s3a://raw/crypto/btcusdt/klines/"
        f"interval=1m/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
    )

def main():
    args = parse_args()
    input_path = build_input_path(args.date)

    print(f"Reading raw data from: {input_path}")
    spark = create_spark_session()

    df = spark.read.parquet(input_path)

    result_df = (
        df
        .withColumn("event_time", to_timestamp((col("timestamp") / 1_000_000_000)))
        .withColumn("date", to_date(col("event_time")))
        .filter(col("symbol").isNotNull())
        .filter(col("date").isNotNull())
        .filter(col("close").isNotNull())
    )

    # result_df.show()
    result_df.writeTo(
        "local.crypto.btc_klines"
    ).createOrReplace()
    result_df.printSchema()

    spark.stop()


if __name__ == "__main__":
    main()