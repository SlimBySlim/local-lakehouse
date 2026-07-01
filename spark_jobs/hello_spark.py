from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = (
    SparkSession.builder
    .appName("parquet-demo")
    .master("local[*]")
    .getOrCreate()
)

print("SPARK UI:", spark.sparkContext.uiWebUrl)

data = [
    ("BTCUSDT", "2026-05-01", 65000.0),
    ("BTCUSDT", "2026-05-02", 66000.0),
    ("ETHUSDT", "2026-05-01", 3200.0),
    ("ETHUSDT", "2026-05-02", 3300.0),
]

df = spark.createDataFrame(data, ["symbol", "date", "close"])

print("INITIAL:", df.rdd.getNumPartitions())

df.count()

df.explain(True)

print("AFTER REPARTITION:", df.rdd.getNumPartitions())

# =========================
# WRITE PARQUET
# =========================


df.write.mode("overwrite").parquet("data/crypto_prices")

print("PARQUET WRITTEN")

# =========================
# READ PARQUET
# =========================

parquet_df = spark.read.parquet("data/crypto_prices")

print("PARQUET READ")

parquet_df.show()

# =========================
# FILTER
# =========================

btc_df = parquet_df.filter(col("symbol") == "BTCUSDT")

btc_df.show()

input("PRESS ENTER TO STOP")

spark.stop()