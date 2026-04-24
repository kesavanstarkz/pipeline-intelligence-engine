"""
health.py — System health & status checker
"""
from __future__ import annotations
import os
import sys
from typing import Any


def get_health_status() -> dict[str, Any]:
    status: dict[str, Any] = {
        "status": "ok",
        "python_version": sys.version.split()[0],
        "services": {},
        "api_key_configured": bool(os.environ.get("ANTHROPIC_API_KEY")),
    }

    # Check AWS boto3
    try:
        import boto3  # noqa
        status["services"]["aws_boto3"] = "available"
    except ImportError:
        status["services"]["aws_boto3"] = "not_installed"

    # Check Azure identity
    try:
        import azure.identity  # noqa
        status["services"]["azure_identity"] = "available"
    except ImportError:
        status["services"]["azure_identity"] = "not_installed"

    # Check Databricks SDK
    try:
        import databricks.sdk  # noqa
        status["services"]["databricks_sdk"] = "available"
    except ImportError:
        status["services"]["databricks_sdk"] = "not_installed (pip install databricks-sdk)"

    # Check Snowflake connector
    try:
        import snowflake.connector  # noqa
        status["services"]["snowflake_connector"] = "available"
    except ImportError:
        status["services"]["snowflake_connector"] = "not_installed"

    # Check httpx for LLM calls
    try:
        import httpx  # noqa
        status["services"]["httpx"] = "available"
    except ImportError:
        status["services"]["httpx"] = "not_installed"

    return status
