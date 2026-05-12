from datetime import datetime
import json
import os

import boto3
import requests
from dotenv import load_dotenv

load_dotenv()

# ===== API =====

url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"

response = requests.get(url)
data = response.json()

# ===== FILE NAME =====

now = datetime.utcnow()

date_path = now.strftime("%Y/%m/%d")
timestamp = now.strftime("%Y%m%d_%H%M%S")

file_name = f"btc_price_{timestamp}.json"

# ===== SAVE TEMP FILE =====

local_path = f"/tmp/{file_name}"

with open(local_path, "w") as file:
    json.dump(data, file)

# ===== S3 / MINIO CLIENT =====

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
    aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
)

bucket_name = "raw"

# ===== CREATE BUCKET IF NOT EXISTS =====

existing_buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]

if bucket_name not in existing_buckets:
    s3.create_bucket(Bucket=bucket_name)

# ===== UPLOAD FILE =====

s3.upload_file(
    local_path,
    bucket_name,
    f"crypto/btcusdt/{date_path}/{file_name}"
)

print("File uploaded successfully")