"""S3 file browser - lists S3 bucket objects."""

import asyncio
import logging
from typing import Any
from .registry import register

logger = logging.getLogger(__name__)

@register("s3")
async def list_files(connection: dict, max_files: int) -> list[str]:
    """List files from S3 bucket.
    
    connection fields:
      - bucket: S3 bucket name
      - path: Optional prefix path (default: "")
      - region: AWS region (default: "us-east-1")
      - authentication: "IAM Role", "Managed Identity", "Access Key", or "SAS Token"
      - access_key_id: For Access Key auth
      - secret_access_key: For Access Key auth
      - aws_sas_token: For temporary AWS sessions or SAS-style auth
    
    Returns: List of terminal file names (stripped of prefix, no directory markers)
    """
    try:
        import boto3
    except ImportError:
        raise ImportError("boto3 not installed. Run: pip install boto3")
    
    def _list_s3_files() -> list[str]:
        bucket = connection.get("bucket", "")
        path = connection.get("path", connection.get("base_path", connection.get("prefix", "")))
        region = connection.get("region") or "us-east-1"  # Handle empty string by using default
        auth = connection.get("authentication") or connection.get("auth_type") or "IAM Role"
        
        logger.info(f"S3 Browse - bucket={bucket}, path={path}, region={region}, auth={auth}")
        logger.info(f"S3 Browse - connection keys: {list(connection.keys())}")
        
        # Build S3 client with auth
        kwargs = {"region_name": region}
        if auth == "Access Key":
            access_key = connection.get("access_key_id")
            secret_key = connection.get("secret_access_key")
            logger.info(f"S3 Browse - Access Key: key_id={'***' if access_key else 'MISSING'}, secret={'***' if secret_key else 'MISSING'}")
            if not access_key or not secret_key:
                raise ValueError(f"Access Key auth requires access_key_id and secret_access_key (got key_id={bool(access_key)}, secret={bool(secret_key)})")
            kwargs["aws_access_key_id"] = access_key
            kwargs["aws_secret_access_key"] = secret_key
        elif auth == "SAS Token":
            token = connection.get("aws_sas_token")
            logger.info(f"S3 Browse - SAS Token: {'***' if token else 'MISSING'}")
            if token:
                kwargs["aws_session_token"] = token
        else:
            logger.info(f"S3 Browse - Using {auth} auth (relying on environment/instance role). Connection has: access_key_id={bool(connection.get('access_key_id'))}, secret_access_key={bool(connection.get('secret_access_key'))}")
        
        logger.info(f"S3 Browse - boto3 kwargs keys: {list(kwargs.keys())}")
        logger.info(f"S3 Browse - About to call boto3.client('s3') with region_name={kwargs.get('region_name')}, has_access_key={bool(kwargs.get('aws_access_key_id'))}, has_secret={'aws_secret_access_key' in kwargs}")

        s3_client = boto3.client("s3", **kwargs)
        
        try:
            response = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=path,
                MaxKeys=max_files
            )
        except Exception as e:
            error_msg = str(e)
            if "Unable to locate credentials" in error_msg and auth == "IAM Role":
                raise Exception(
                    f"S3 credentials not found. Tried: IAM Role/environment credentials. "
                    f"Solution: Select 'Access Key' auth type and provide AWS access key ID + secret access key."
                )
            else:
                raise Exception(f"S3 list_objects_v2 failed: {error_msg}")
        
        files = []
        if "Contents" in response:
            for obj in response["Contents"]:
                key = obj["Key"]
                # Skip directory markers
                if key.endswith("/"):
                    continue
                # Strip prefix and get terminal filename
                if path and key.startswith(path):
                    key = key[len(path):].lstrip("/")
                files.append(key)
        
        return files[:max_files]
    
    # Run sync code in thread
    return await asyncio.to_thread(_list_s3_files)
