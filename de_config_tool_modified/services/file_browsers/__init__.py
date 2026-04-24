"""File browser plugins - self-registering modules for each source type."""

# Import all browser modules to trigger @register decorators
from . import (
    s3_browser,
    adls_browser,
    postgresql_browser,
    mysql_browser,
    azure_sql_browser,
    eventhub_browser,
    kafka_browser,
    mongodb_browser,
    snowflake_browser,
    salesforce_browser,
    rest_api_browser,
)

__all__ = [
    "s3_browser",
    "adls_browser", 
    "postgresql_browser",
    "mysql_browser",
    "azure_sql_browser",
    "eventhub_browser",
    "kafka_browser",
    "mongodb_browser",
    "snowflake_browser",
    "salesforce_browser",
    "rest_api_browser",
]
