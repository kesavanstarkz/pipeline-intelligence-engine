import os
import boto3
import json
import urllib.request
import uuid
from datetime import datetime

# We will load coordinates from environment variables (Best Practice)
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
AWS_REGION = 'us-east-1'

# Generate a unique bucket name to avoid collisions
BUCKET_NAME = f"simple-boto3-pipeline-{uuid.uuid4().hex[:8]}"

def extract_data():
    """Extract: Fetch mock data from a public REST API"""
    print("➜ [EXTRACT] Fetching data from public API...")
    url = "https://jsonplaceholder.typicode.com/posts/1"
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req) as response:
        data = json.loads(response.read().decode())
    return data

def transform_data(data):
    """Transform: Add ingestion metadata"""
    print("➜ [TRANSFORM] Appending metadata to payload...")
    data['_ingested_at'] = datetime.utcnow().isoformat()
    data['pipeline_source'] = 'boto3-simple-pipeline'
    return data

def load_data(data):
    """Load: Upload data into AWS S3 using Boto3"""
    print(f"➜ [LOAD] Connecting to S3...")
    
    # Initialize boto3 client using the provided credentials
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    
    try:
        # Create the bucket
        print(f"  Creating bucket: {BUCKET_NAME}")
        if AWS_REGION == 'us-east-1':
            s3_client.create_bucket(Bucket=BUCKET_NAME)
        else:
            s3_client.create_bucket(
                Bucket=BUCKET_NAME,
                CreateBucketConfiguration={'LocationConstraint': AWS_REGION}
            )
    except Exception as e:
        print(f"  ⚠️ Could not create bucket (it may exist or lack permissions): {e}")

    # Upload the JSON data
    object_key = f"raw_data/post-{data['id']}.json"
    print(f"  Uploading file: {object_key}")
    
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=object_key,
        Body=json.dumps(data, indent=2),
        ContentType='application/json'
    )
    print(f"✅ Pipeline complete! Data stored at s3://{BUCKET_NAME}/{object_key}")

def run_pipeline():
    print("=== Starting Boto3 ETL Pipeline ===")
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        print("❌ ERROR: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables are missing.")
        print("Please set them before running this script.")
        return

    data = extract_data()
    transformed_data = transform_data(data)
    load_data(transformed_data)

if __name__ == "__main__":
    run_pipeline()
