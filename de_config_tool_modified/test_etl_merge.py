#!/usr/bin/env python3
"""
Test script for the merged ETL configuration system.
"""

import json
from services.etl_config_manager import ETLConfigManager

# Sample configs
source_config = {
    "source_type": "aws",
    "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
    "aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    "aws_region": "us-east-1",
    "source_bucket": "my-data-bucket",
    "source_prefix": "exports/2024/orders/",
    "file_format": "parquet"
}

ingestion_config = {
    "extraction_mode": "incremental",
    "batch_size": 50000,
    "parallelism": 4,
    "schedule": {
        "enabled": True,
        "cron_expression": "0 2 * * *",
        "timezone": "UTC"
    },
    "incremental": {
        "watermark_column": "updated_at",
        "watermark_type": "timestamp",
        "last_watermark_value": "2024-03-01T00:00:00Z",
        "lookback_window_minutes": 60
    },
    "filter": {
        "where_clause": "status = 'ACTIVE'",
        "include_columns": ["id", "name", "updated_at"]
    },
    "output": {
        "file_format": "parquet",
        "compression": "snappy",
        "partition_by": ["year", "month"]
    },
    "quality": {
        "null_check_columns": ["id"],
        "row_count_min": 1
    },
    "notifications": {
        "on_failure": True,
        "email": "etl-alerts@company.com"
    }
}

azure_config = {
    "adf_subscription_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    "adf_resource_group": "rg-etl-prod",
    "adf_factory_name": "adf-hivemind-prod",
    "adf_pipeline_name": "pl_ingest_landing",
    "adls_account_name": "hiveminddatalake",
    "adls_container": "raw",
    "managed_identity_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    "key_vault_name": "kv-hivemind-prod"
}

def test_etl_config():
    print("Testing ETL Config Manager...")

    try:
        manager = ETLConfigManager.from_request(source_config, ingestion_config, azure_config)
        payload = manager.build_runtime_payload()
        adf_params = manager.to_adf_parameters(payload)
        kv_secret = manager.get_kv_secret_payload(payload.job_id)

        print(f"✅ Job ID: {payload.job_id}")
        print(f"✅ Source Type: {payload.source_type.value}")
        print(f"✅ Extraction Mode: {payload.extraction_mode.value}")
        print(f"✅ ADF Parameters count: {len(adf_params)}")
        print(f"✅ KV Secret name: {kv_secret['secret_name']}")

        # Print summary
        summary = manager.summary()
        print("\n📋 Config Summary:")
        print(json.dumps(summary, indent=2, default=str))

        print("\n🎉 ETL Config merge successful!")
        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_etl_config()
    exit(0 if success else 1)