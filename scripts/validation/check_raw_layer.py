import os
import boto3
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
    print(f"Bucket '{bucket_name}' is empty")
else:
    print(f"Files in bucket '{bucket_name}':")
    for obj in response["Contents"]:
        print(f"- {obj['Key']} | {obj['Size']} bytes")