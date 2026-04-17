import boto3
import os
import datetime
import json

def get_dynamic_session():
    """
    Creates a Boto3 Session that supports a Dynamic Cloud environment.
    It automatically resolves the active Region and Credentials, avoiding hardcoded parameters.
    """
    session = boto3.Session(
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
    )
    return session

def cleanup_resources(session):
    """
    Deletes all previously created mock-pipeline resources.
    Emptying S3 buckets correctly before destroying them.
    """
    print("\n=== PHASE 1: Resource Cleanup ===")
    cf = session.client('cloudformation')
    s3 = session.client('s3')
    
    stack_name = 'mock-api-lambda-s3-pipeline'
    
    # 1. Delete CF Stack if it exists
    try:
        print(f"➜ Checking CloudFormation Stack: {stack_name} for deletion...")
        cf.describe_stacks(StackName=stack_name) # Will throw if not exists
        cf.delete_stack(StackName=stack_name)
        print("  Waiting for CloudFormation stack termination (this takes ~1 min)...")
        waiter = cf.get_waiter('stack_delete_complete')
        waiter.wait(StackName=stack_name)
        print("  ✅ Legacy stack securely terminated.")
    except Exception as e:
        print(f"  ℹ️ Stack '{stack_name}' not found or already deleted.")
        
    # 2. Cleanup orphaned pipeline S3 buckets created in earlier stages
    try:
        response = s3.list_buckets()
        for bucket in response.get('Buckets', []):
            b_name = bucket['Name']
            if b_name.startswith('simple-boto3-pipeline-') or b_name.startswith('dynamic-pipeline-'):
                print(f"➜ Emptying and deleting bucket: {b_name}...")
                
                # Delete all objects manually (Required before deleting bucket)
                paginator = s3.get_paginator('list_objects_v2')
                for page in paginator.paginate(Bucket=b_name):
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            s3.delete_object(Bucket=b_name, Key=obj['Key'])
                
                # Delete the physical bucket
                s3.delete_bucket(Bucket=b_name)
                print(f"  ✅ Deleted {b_name}")
    except Exception as e:
        print(f"  ⚠️ Non-critical failure searching orphaned buckets: {e}")

def create_dynamic_pipeline(session):
    """
    Provisions a simple pipeline based dynamically on the hosting AWS account and region.
    """
    print("\n=== PHASE 2: Dynamic Pipeline Creation ===")
    sts = session.client('sts')
    s3 = session.client('s3')
    
    # 1. Evaluate Dynamic Context (Account + Region)
    identity = sts.get_caller_identity()
    account_id = identity['Account']
    region = session.region_name
    
    print(f"➜ Dynamic Cloud Context Established:")
    print(f"  Account ID : {account_id}")
    print(f"  Region     : {region}")
    print(f"  IAM ARN    : {identity['Arn']}")
    
    # 2. Architect resource names specific to this dynamic environment footprint
    bucket_name = f"dynamic-pipeline-{account_id}-{region}"
    print(f"\n➜ Provisioning pipeline storage layer: {bucket_name}...")
    
    try:
        if region == 'us-east-1':
            s3.create_bucket(Bucket=bucket_name)
        else:
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': region}
            )
        print("  ✅ Storage successfully instantiated.")
    except Exception as e:
        if 'BucketAlreadyExists' in str(e) or 'BucketAlreadyOwnedByYou' in str(e):
             print("  ✅ Storage already exists in this dynamic context.")
        else:
             print(f"  ❌ Error provisioning storage: {e}")
             return

    # 3. Simulate pipeline telemetry event writing to the bucket
    manifest = {
        "pipeline_name": "dynamic-boto3-pipeline",
        "account_id": account_id,
        "region": region,
        "deployed_at": datetime.datetime.utcnow().isoformat(),
        "components": ["s3_storage_layer", "dynamic_ingestion"]
    }
    
    object_key = "pipeline_telemetry/active_manifest.json"
    s3.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=json.dumps(manifest, indent=2),
        ContentType="application/json"
    )
    print(f"  ✅ Dynamic pipeline telemtry pushed to s3://{bucket_name}/{object_key}")

if __name__ == "__main__":
    current_session = get_dynamic_session()
    cleanup_resources(current_session)
    create_dynamic_pipeline(current_session)
    print("\n🚀 All Pipeline execution loops completed successfully!")
