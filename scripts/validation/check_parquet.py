import os
from io import BytesIO

import boto3
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT_URL"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

bucket_name = os.getenv("S3_BUCKET_RAW")

response = s3.list_objects_v2(Bucket=bucket_name)

if "Contents" not in response:
    print("Bucket is empty")
    exit()

latest_file = response["Contents"][-1]["Key"]

print(f"Reading file: {latest_file}")

obj = s3.get_object(Bucket=bucket_name, Key=latest_file)

df = pd.read_parquet(BytesIO(obj["Body"].read()))

print("\nColumns:")
print(df.columns.tolist())

print("\nDtypes:")
print(df.dtypes)

print(f"\nRows count: {len(df)}")

if "timestamp" in df.columns:
    print(f"\nMin timestamp: {df['timestamp'].min()}")
    print(f"Max timestamp: {df['timestamp'].max()}")

print("\nPreview:")
print(df.head())