"""
etl_config_manager.py  (v2 — three-layer config)
--------------------------------------------------
Merges three independent config layers into a single validated
ETL runtime payload:

    Layer 1 — source_configs.py   → WHERE to connect (user credentials)
    Layer 2 — ingestion_config.py → HOW to pull (user behaviour config)
    Layer 3 — azure_config.py     → WHERE to sink (your internal Azure config)

Credential isolation guarantee:
    SecretStr fields from source config are NEVER included in the
    runtime payload or ADF parameters. They go to Key Vault only,
    referenced by secret name.

Typical flow:
    manager = ETLConfigManager.from_request(source_raw, ingestion_raw, azure_raw)
    payload  = manager.build_runtime_payload()
    kv_blob  = manager.get_kv_secret_payload(payload.job_id)
    # → write kv_blob to Key Vault
    params   = manager.to_adf_parameters(payload)
    # → trigger ADF pipeline with params
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field, SecretStr

from app.models.schemas import (
    BaseSourceConfig, IngestionConfig, AzureSinkConfig,
    SourceType, ExtractionMode, ETLRuntimePayload, JobStatus
)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _kv_secret_name(source_type: SourceType, job_id: str) -> str:
    return f"etl-src-{source_type.value}-{job_id[:8]}"


def _source_metadata(cfg: BaseSourceConfig) -> dict[str, Any]:
    from app.models.schemas import (
        AWSSourceConfig, GCPSourceConfig, SnowflakeSourceConfig,
        PostgresSourceConfig, SalesforceSourceConfig,
    )
    if isinstance(cfg, AWSSourceConfig):
        label = f"s3://{cfg.source_bucket}/{cfg.source_prefix}"
    elif isinstance(cfg, GCPSourceConfig):
        label = (f"{cfg.gcp_project_id}.{cfg.bq_dataset}.{cfg.bq_table}"
                 if cfg.bq_dataset else f"gs://{cfg.gcs_bucket}/{cfg.gcs_prefix}")
    elif isinstance(cfg, SnowflakeSourceConfig):
        label = f"{cfg.sf_database}.{cfg.sf_schema}.{cfg.sf_table}"
    elif isinstance(cfg, PostgresSourceConfig):
        label = f"{cfg.pg_host}/{cfg.pg_database}.{cfg.pg_schema}.{cfg.pg_table}"
    elif isinstance(cfg, SalesforceSourceConfig):
        label = f"{cfg.sf_instance_url}/sobjects/{cfg.object_name}"
    else:
        label = cfg.source_type.value
    return {"source_label": label}


def parse_source_config(data: dict[str, Any]) -> BaseSourceConfig:
    """
    Factory: detect source_type from the dict and return the right model instance.
    """
    from app.models.schemas import (
        AWSSourceConfig, GCPSourceConfig, SnowflakeSourceConfig,
        PostgresSourceConfig, SalesforceSourceConfig, SourceType
    )

    SOURCE_CONFIG_MAP = {
        SourceType.AWS: AWSSourceConfig,
        SourceType.GCP: GCPSourceConfig,
        SourceType.SNOWFLAKE: SnowflakeSourceConfig,
        SourceType.POSTGRES: PostgresSourceConfig,
        SourceType.SALESFORCE: SalesforceSourceConfig,
    }

    raw_type = data.get("source_type")
    if not raw_type:
        raise ValueError("'source_type' is required in source config")

    try:
        src_type = SourceType(raw_type)
    except ValueError:
        valid = [e.value for e in SourceType]
        raise ValueError(f"Unknown source_type '{raw_type}'. Valid: {valid}")

    model_cls = SOURCE_CONFIG_MAP[src_type]
    return model_cls(**data)


# ─── Runtime payload ──────────────────────────────────────────────────────────

class ETLRuntimePayload(BaseModel):
    """
    Fully merged, sanitised payload.
    No credentials — only KV secret name references.
    This is what gets logged, stored in job history, and passed to ADF.
    """

    # Identity
    job_id:     str       = Field(default_factory=lambda: str(uuid.uuid4()))
    created_at: str       = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    status:     JobStatus = JobStatus.PENDING
    description: Optional[str] = None

    # Layer 1: source metadata (sanitised)
    source_type:  SourceType
    source_label: str

    # Layer 2: ingestion behaviour
    extraction_mode:    ExtractionMode
    batch_size:         int
    parallelism:        int
    schedule_enabled:   bool
    schedule_cron:      Optional[str]
    schedule_timezone:  str
    watermark_column:   Optional[str]
    watermark_type:     Optional[str]
    last_watermark:     Optional[str]
    lookback_minutes:   int
    cdc_mode:           Optional[str]
    updated_at_column:  Optional[str]
    deleted_at_column:  Optional[str]
    last_lsn_value:     Optional[str]
    where_clause:       Optional[str]
    include_columns:    list[str]
    exclude_columns:    list[str]
    row_limit:          Optional[int]
    file_format:        str
    compression:        str
    partition_by:       list[str]
    max_file_size_mb:   int
    add_metadata_cols:  bool
    null_check_columns: list[str]
    row_count_min:      Optional[int]
    schema_drift_action:str
    dedup_columns:      list[str]
    max_retries:        int
    retry_interval_seconds: int
    timeout_seconds:    int
    notify_on_failure:  bool
    notify_email:       Optional[str]

    # Layer 3: Azure sink (resolved)
    adf_pipeline_name:  str
    adls_account_name:  str
    adls_container:     str
    adls_sink_path:     str
    write_mode:         str
    synapse_pool:       Optional[str]
    synapse_schema:     Optional[str]
    synapse_table:      Optional[str]

    # KV reference
    key_vault_name:     str
    source_secret_name: str

    tags: dict[str, str] = Field(default_factory=dict)


# ─── Manager ─────────────────────────────────────────────────────────────────

class ETLConfigManager:
    def __init__(
        self,
        source_config:    BaseSourceConfig,
        ingestion_config: IngestionConfig,
        azure_config:     AzureSinkConfig,
    ):
        self.source    = source_config
        self.ingestion = ingestion_config
        self.azure     = azure_config
        self._job_id   = str(uuid.uuid4())

    @classmethod
    def from_request(
        cls,
        source_raw:    dict[str, Any],
        ingestion_raw: dict[str, Any],
        azure_raw:     dict[str, Any],
    ) -> ETLConfigManager:
        return cls(
            source_config    = parse_source_config(source_raw),
            ingestion_config = IngestionConfig(**ingestion_raw),
            azure_config     = AzureSinkConfig(**azure_raw),
        )

    @classmethod
    def from_json(cls, source_json: str, ingestion_json: str, azure_json: str) -> ETLConfigManager:
        return cls.from_request(
            json.loads(source_json),
            json.loads(ingestion_json),
            json.loads(azure_json),
        )

    def build_runtime_payload(self, extra_tags: Optional[dict[str, str]] = None) -> ETLRuntimePayload:
        src   = self.source
        ing   = self.ingestion
        az    = self.azure
        job   = self._job_id

        return ETLRuntimePayload(
            job_id      = job,
            status      = JobStatus.VALIDATED,
            description = ing.description,
            # Layer 1
            source_type  = src.source_type,
            source_label = _source_metadata(src)["source_label"],
            # Layer 2
            extraction_mode    = ing.extraction_mode,
            batch_size         = ing.batch_size,
            parallelism        = ing.parallelism,
            schedule_enabled   = ing.schedule.enabled,
            schedule_cron      = ing.schedule.cron_expression,
            schedule_timezone  = ing.schedule.timezone,
            watermark_column   = ing.incremental.watermark_column        if ing.incremental else None,
            watermark_type     = ing.incremental.watermark_type.value    if ing.incremental else None,
            last_watermark     = ing.incremental.last_watermark_value    if ing.incremental else None,
            lookback_minutes   = ing.incremental.lookback_window_minutes if ing.incremental else 0,
            cdc_mode           = ing.cdc.cdc_mode.value                 if ing.cdc else None,
            updated_at_column  = ing.cdc.updated_at_column              if ing.cdc else None,
            deleted_at_column  = ing.cdc.deleted_at_column              if ing.cdc else None,
            last_lsn_value     = ing.cdc.last_lsn_value                 if ing.cdc else None,
            where_clause       = ing.filter.where_clause,
            include_columns    = ing.filter.include_columns,
            exclude_columns    = ing.filter.exclude_columns,
            row_limit          = ing.filter.row_limit,
            file_format        = ing.output.file_format.value,
            compression        = ing.output.compression.value,
            partition_by       = ing.output.partition_by,
            max_file_size_mb   = ing.output.max_file_size_mb,
            add_metadata_cols  = ing.output.add_metadata_cols,
            null_check_columns = ing.quality.null_check_columns,
            row_count_min      = ing.quality.row_count_min,
            schema_drift_action= ing.quality.schema_drift_action.value,
            dedup_columns      = ing.quality.dedup_columns,
            max_retries        = ing.retry.max_retries,
            retry_interval_seconds = ing.retry.retry_interval_seconds,
            timeout_seconds    = ing.retry.timeout_seconds,
            notify_on_failure  = ing.notifications.on_failure,
            notify_email       = ing.notifications.email,
            # Layer 3
            adf_pipeline_name  = az.adf_pipeline_name,
            adls_account_name  = az.adls_account_name,
            adls_container     = az.adls_container,
            adls_sink_path     = az.adls_sink_path.format(source_type=src.source_type.value, job_id=job),
            write_mode         = az.write_mode.value,
            synapse_pool       = az.synapse_pool,
            synapse_schema     = az.synapse_schema,
            synapse_table      = az.synapse_table.format(source_type=src.source_type.value) if az.synapse_table else None,
            key_vault_name     = az.key_vault_name,
            source_secret_name = _kv_secret_name(src.source_type, job),
            tags = {"source_type": src.source_type.value, **(ing.tags or {}), **(extra_tags or {})},
        )

    def to_adf_parameters(self, payload: ETLRuntimePayload) -> dict[str, str]:
        p = payload
        return {
            "jobId":              p.job_id,
            "createdAt":          p.created_at,
            "sourceType":         p.source_type.value,
            "sourceLabel":        p.source_label,
            "sourceSecretName":   p.source_secret_name,
            "keyVaultName":       p.key_vault_name,
            "extractionMode":     p.extraction_mode.value,
            "batchSize":          str(p.batch_size),
            "parallelism":        str(p.parallelism),
            "scheduleEnabled":    str(p.schedule_enabled).lower(),
            "scheduleCron":       p.schedule_cron     or "",
            "scheduleTimezone":   p.schedule_timezone,
            "watermarkColumn":    p.watermark_column  or "",
            "watermarkType":      p.watermark_type    or "",
            "lastWatermarkValue": p.last_watermark    or "",
            "lookbackMinutes":    str(p.lookback_minutes),
            "cdcMode":            p.cdc_mode          or "",
            "updatedAtColumn":    p.updated_at_column or "",
            "deletedAtColumn":    p.deleted_at_column or "",
            "lastLsnValue":       p.last_lsn_value    or "",
            "whereClause":        p.where_clause      or "",
            "includeColumns":     ",".join(p.include_columns),
            "excludeColumns":     ",".join(p.exclude_columns),
            "rowLimit":           str(p.row_limit) if p.row_limit else "",
            "fileFormat":         p.file_format,
            "compression":        p.compression,
            "partitionBy":        ",".join(p.partition_by),
            "maxFileSizeMb":      str(p.max_file_size_mb),
            "addMetadataCols":    str(p.add_metadata_cols).lower(),
            "nullCheckColumns":   ",".join(p.null_check_columns),
            "rowCountMin":        str(p.row_count_min) if p.row_count_min else "",
            "schemaDriftAction":  p.schema_drift_action,
            "dedupColumns":       ",".join(p.dedup_columns),
            "maxRetries":         str(p.max_retries),
            "retryIntervalSecs":  str(p.retry_interval_seconds),
            "timeoutSeconds":     str(p.timeout_seconds),
            "notifyOnFailure":    str(p.notify_on_failure).lower(),
            "notifyEmail":        p.notify_email or "",
            "adfPipelineName":    p.adf_pipeline_name,
            "adlsAccountName":    p.adls_account_name,
            "adlsContainer":      p.adls_container,
            "adlsSinkPath":       p.adls_sink_path,
            "writeMode":          p.write_mode,
            "synapsePool":        p.synapse_pool   or "",
            "synapseSchema":      p.synapse_schema or "",
            "synapseTable":       p.synapse_table  or "",
            "tags":               json.dumps(p.tags),
        }

    def get_kv_secret_payload(self, job_id: str) -> dict[str, Any]:
        raw: dict[str, Any] = {}
        for name in self.source.model_fields:
            val = getattr(self.source, name)
            raw[name] = val.get_secret_value() if isinstance(val, SecretStr) else val
        return {
            "secret_name":  _kv_secret_name(self.source.source_type, job_id),
            "secret_value": json.dumps(raw),
            "content_type": "application/json",
            "tags": {
                "source_type": self.source.source_type.value,
                "job_id":      job_id,
                "created_by":  "hivemind-etl-manager",
            }
        }

    def summary(self) -> dict[str, Any]:
        return {
            "job_id":    self._job_id,
            "source":    self.source.masked_dump(),
            "ingestion": self.ingestion.model_dump(),
            "azure":     self.azure.masked_dump(),
        }