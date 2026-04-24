"""
Source config generator — builds structured nested JSON from raw form fields.
"""
from typing import Any
import datetime

CONN_KEYS = {
    "server", "host", "port", "database", "schema", "collection",
    "namespace", "eventhub", "brokers", "topic", "consumer_group",
    "bucket", "path", "base_url", "endpoint", "account", "warehouse",
    "instance_url", "object", "storage_type", "authentication", "network",
}
EXTRACTION_KEYS = {
    "mode", "query", "offset_reset", "partition_pattern",
    "pagination", "rate_limit", "watermark_column", "state_store",
}
PROFILE_KEYS = {
    "volume_per_day", "format", "encoding", "peak_throughput",
    "partitions", "throughput_units", "retention_days",
}


def build_config(source_type: str, fields: dict[str, Any]) -> dict[str, Any]:
    name = fields.get("name") or f"{source_type}_source"
    source_key = f"{source_type}_source"

    connection: dict[str, Any] = {}
    extraction: dict[str, Any] = {}
    data_profile: dict[str, Any] = {}
    extra: dict[str, Any] = {}

    for k, v in fields.items():
        if k == "name" or not v:
            continue
        if k in CONN_KEYS:
            connection[k] = v
        elif k in EXTRACTION_KEYS:
            extraction[k] = v
        elif k in PROFILE_KEYS:
            data_profile[k] = v
        else:
            extra[k] = v

    source_cfg: dict[str, Any] = {
        "name": name,
        "type": source_type,
    }
    if connection:
        source_cfg["connection"] = connection
    if extraction:
        source_cfg["extraction"] = extraction
    if data_profile:
        source_cfg["data_profile"] = data_profile
    source_cfg.update(extra)
    source_cfg["_generated"] = datetime.date.today().isoformat()

    return {"source_configuration": {source_key: source_cfg}}
