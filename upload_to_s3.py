import boto3
import pandas as pd
from io import BytesIO

session = boto3.Session(
    aws_access_key_id="",
    aws_secret_access_key="",
    aws_session_token="",
    region_name="us-east-1"
)

# Initialize S3 client
s3 = session.client('s3')

# --- Upload local file to S3 ---
local_file_path = "rag_docs/glossary.csv"
bucket_name = "etl-health-data-lab"
s3_key = "raw/glossary.csv"   # Folder/key name in your bucket

try:
    s3.upload_file(local_file_path, bucket_name, s3_key)
    print(f"✅ File uploaded successfully to s3://{bucket_name}/{s3_key}")
except Exception as e:
    print(f"❌ Upload failed: {e}")
