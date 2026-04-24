from pydantic import BaseModel, Field
from typing import Any, Optional


class AzureConfigUpload(BaseModel):
    config: dict[str, Any] = Field(..., description="Raw Azure / baseline config JSON")


class GenerateConfigRequest(BaseModel):
    source_type: str
    fields: dict[str, Any]


class DiffRequest(BaseModel):
    baseline: dict[str, Any] = Field(..., description="Azure / baseline config")
    candidate: dict[str, Any] = Field(..., description="New generated config")


class DiffEntry(BaseModel):
    path: str
    type: str           # match | mismatch | added | removed
    baseline_val: Any = None
    candidate_val: Any = None


class DiffResult(BaseModel):
    summary: dict[str, int]
    entries: list[DiffEntry]


class ReconcileRequest(BaseModel):
    baseline: dict[str, Any]
    candidate: dict[str, Any]
    strategy: str = Field(
        "candidate_wins",
        description="candidate_wins | baseline_wins | manual",
    )
    overrides: Optional[dict[str, Any]] = Field(
        None,
        description="Path-keyed manual overrides, e.g. {'connection.host': 'myhost'}",
    )


class ReconcileResult(BaseModel):
    merged: dict[str, Any]
    change_log: list[dict[str, Any]]


# New unified ETL config models
from enum import Enum
from pydantic import SecretStr, field_validator, model_validator
import re


class SourceType(str, Enum):
    AWS = "aws"
    GCP = "gcp"
    SNOWFLAKE = "snowflake"
    POSTGRES = "postgres"
    SALESFORCE = "salesforce"


class ExtractionMode(str, Enum):
    FULL_LOAD = "full_load"
    INCREMENTAL = "incremental"
    CDC = "cdc"


class FileFormat(str, Enum):
    PARQUET = "parquet"
    DELTA = "delta"
    CSV = "csv"
    JSON = "json"
    ORC = "orc"


class AWSAuthType(str, Enum):
    IAM_ROLE = "IAM Role"
    MANAGED_IDENTITY = "Managed Identity"
    ACCESS_KEY = "Access Key"
    SAS_TOKEN = "SAS Token"
    SERVICE_ACCOUNT = "Service Account"


class Compression(str, Enum):
    SNAPPY = "snappy"
    GZIP = "gzip"
    ZSTD = "zstd"
    NONE = "none"


class WatermarkType(str, Enum):
    TIMESTAMP = "timestamp"
    DATE = "date"
    INTEGER = "integer"


class SchemaDriftAction(str, Enum):
    WARN = "warn"
    FAIL = "fail"
    IGNORE = "ignore"
    EVOLVE = "evolve"


class WriteMode(str, Enum):
    APPEND = "append"
    OVERWRITE = "overwrite"
    MERGE = "merge"


class JobStatus(str, Enum):
    PENDING = "pending"
    VALIDATED = "validated"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class BaseSourceConfig(BaseModel):
    source_type: SourceType

    def masked_dump(self) -> dict[str, Any]:
        out = {}
        for name, field_info in self.model_fields.items():
            val = getattr(self, name)
            if isinstance(val, SecretStr):
                out[name] = "***"
            else:
                out[name] = val
        return out

    def to_json(self, masked: bool = False, indent: int = 2) -> str:
        import json
        if masked:
            return json.dumps(self.masked_dump(), default=str, indent=indent)
        return self.model_dump_json(indent=indent)


class AWSSourceConfig(BaseSourceConfig):
    source_type: SourceType = SourceType.AWS
    aws_auth_type: AWSAuthType = Field(AWSAuthType.ACCESS_KEY, description="AWS authentication mechanism")
    aws_access_key_id: Optional[str] = Field(None, description="IAM access key ID")
    aws_secret_access_key: Optional[SecretStr] = Field(None, description="IAM secret access key")
    aws_role_arn: Optional[str] = Field(None, description="IAM role ARN for role-based auth")
    aws_session_name: Optional[str] = Field(None, description="Session name for role assumption")
    aws_sas_token: Optional[SecretStr] = Field(None, description="SAS token for S3 access, if used")
    aws_managed_identity_client_id: Optional[str] = Field(None, description="Managed identity client ID if using managed identity")
    aws_region: str
    source_bucket: str
    source_prefix: str = ""
    file_format: FileFormat
    glue_database: Optional[str] = None
    glue_table: Optional[str] = None
    partition_pattern: Optional[str] = Field(None, description="Partition path pattern such as year={YYYY}/month={MM}/day={DD}")
    partition_columns: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_aws_auth(self) -> "AWSSourceConfig":
        if self.aws_auth_type == AWSAuthType.ACCESS_KEY:
            if not self.aws_access_key_id or not self.aws_secret_access_key:
                raise ValueError("Access Key auth requires aws_access_key_id and aws_secret_access_key")
        if self.aws_auth_type == AWSAuthType.SAS_TOKEN and not self.aws_sas_token:
            raise ValueError("SAS Token auth requires aws_sas_token")
        return self

    @model_validator(mode="after")
    def parse_partition_pattern(self) -> "AWSSourceConfig":
        if self.partition_pattern and not self.partition_columns:
            columns = [match.group(1) for match in __import__("re").finditer(r"([^/{}=]+)=\{[^}]+\}", self.partition_pattern)]
            self.partition_columns = columns
        return self


class GCPSourceConfig(BaseSourceConfig):
    source_type: SourceType = SourceType.GCP
    gcp_project_id: str
    service_account_json: SecretStr
    bq_dataset: Optional[str] = None
    bq_table: Optional[str] = None
    gcs_bucket: Optional[str] = None
    gcs_prefix: str = ""
    bq_location: str = "US"
    extract_query: Optional[str] = None


class SnowflakeSourceConfig(BaseSourceConfig):
    source_type: SourceType = SourceType.SNOWFLAKE
    sf_account: str
    sf_user: str
    sf_password: SecretStr
    sf_warehouse: str
    sf_database: str
    sf_schema: str = "PUBLIC"
    sf_table: str
    sf_role: Optional[str] = None
    extract_query: Optional[str] = None


class PostgresSourceConfig(BaseSourceConfig):
    source_type: SourceType = SourceType.POSTGRES
    pg_host: str
    pg_port: int = 5432
    pg_database: str
    pg_user: str
    pg_password: SecretStr
    pg_schema: str = "public"
    pg_table: str
    ssl_mode: str = "require"
    watermark_column: Optional[str] = None
    watermark_value: Optional[str] = None


class SalesforceSourceConfig(BaseSourceConfig):
    source_type: SourceType = SourceType.SALESFORCE
    sf_instance_url: str
    sf_client_id: str
    sf_client_secret: SecretStr
    sf_username: Optional[str] = None
    sf_password: Optional[SecretStr] = None
    sf_api_version: str = "v58.0"
    object_name: str
    soql_query: Optional[str] = None
    bulk_api: bool = True


class CDCMode(str, Enum):
    LOG_BASED = "log_based"
    QUERY_BASED = "query_based"
    TRIGGER = "trigger"


class ScheduleConfig(BaseModel):
    """When and how often to run the ingestion."""
    enabled: bool = Field(False, description="Enable scheduled runs")
    cron_expression: Optional[str] = Field(None, description="Cron schedule, e.g. '0 2 * * *'")
    timezone: str = Field("UTC", description="Timezone for cron evaluation")
    backfill_enabled: bool = Field(False, description="Re-run missed intervals on start")
    backfill_start_date: Optional[str] = Field(None, description="ISO date for backfill start, e.g. 2024-01-01")
    max_active_runs: int = Field(1, ge=1, le=10, description="Max concurrent runs of this job")

    @field_validator("cron_expression")
    @classmethod
    def validate_cron(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        parts = v.strip().split()
        if len(parts) not in (5, 6):
            raise ValueError(f"Invalid cron expression '{v}'. Expected 5 or 6 fields.")
        return v

    @model_validator(mode="after")
    def cron_required_when_enabled(self) -> "ScheduleConfig":
        if self.enabled and not self.cron_expression:
            raise ValueError("cron_expression is required when schedule.enabled is true")
        return self


class IncrementalConfig(BaseModel):
    """Config for incremental / watermark-based extraction."""
    watermark_column: str = Field(..., description="Column used as high-water mark")
    watermark_type: WatermarkType = Field(WatermarkType.TIMESTAMP)
    last_watermark_value: Optional[str] = Field(None, description="Last successfully extracted value")
    lookback_window_minutes: int = Field(0, ge=0, description="Overlap window to catch late-arriving data")
    upper_bound_column: Optional[str] = Field(None, description="Optional upper bound column for bounded extraction")

    @field_validator("watermark_column")
    @classmethod
    def no_spaces_in_column(cls, v: str) -> str:
        if " " in v:
            raise ValueError("watermark_column must not contain spaces — use the exact DB column name")
        return v


class CDCConfig(BaseModel):
    """Config for Change Data Capture extraction."""
    cdc_mode: CDCMode = Field(CDCMode.QUERY_BASED)
    created_at_column: Optional[str] = Field(None, description="Column tracking row creation time")
    updated_at_column: Optional[str] = Field(None, description="Column tracking last update time")
    deleted_at_column: Optional[str] = Field(None, description="Soft-delete column (null = not deleted)")
    include_deletes: bool = Field(True, description="Include soft-deleted rows in the output")
    lsn_column: Optional[str] = Field(None, description="Log sequence number column for log-based CDC")
    last_lsn_value: Optional[str] = Field(None, description="Last processed LSN value")

    @model_validator(mode="after")
    def validate_log_based(self) -> "CDCConfig":
        if self.cdc_mode == CDCMode.LOG_BASED and not self.lsn_column:
            raise ValueError("lsn_column is required when cdc_mode is log_based")
        return self


class FilterConfig(BaseModel):
    """Row and column filtering applied at extraction time."""
    where_clause: Optional[str] = Field(None, description="SQL WHERE clause, e.g. status = 'ACTIVE'")
    include_columns: list[str] = Field(default_factory=list, description="Whitelist of columns to extract. Empty = all columns.")
    exclude_columns: list[str] = Field(default_factory=list, description="Columns to drop from extraction")
    row_limit: Optional[int] = Field(None, ge=1, description="Hard cap on rows extracted (useful for sampling)")
    sample_percent: Optional[float] = Field(None, ge=0.01, le=100.0, description="Extract a % sample of rows (not deterministic)")

    @model_validator(mode="after")
    def no_overlap_columns(self) -> "FilterConfig":
        overlap = set(self.include_columns) & set(self.exclude_columns)
        if overlap:
            raise ValueError(f"Columns cannot be in both include and exclude: {overlap}")
        return self

    @model_validator(mode="after")
    def not_both_limit_and_sample(self) -> "FilterConfig":
        if self.row_limit and self.sample_percent:
            raise ValueError("Use either row_limit or sample_percent, not both")
        return self


class OutputConfig(BaseModel):
    """How extracted data should be written to the landing zone."""
    file_format: FileFormat = Field(FileFormat.PARQUET)
    compression: Compression = Field(Compression.SNAPPY)
    partition_by: list[str] = Field(default_factory=list, description="Columns to partition output files by")
    max_file_size_mb: int = Field(256, ge=16, le=2048, description="Max output file size in MB before splitting")
    add_metadata_cols: bool = Field(True, description="Append _ingested_at, _source_type, _job_id columns")
    preserve_source_schema: bool = Field(True, description="Carry forward source schema as-is (no type coercion)")

    @model_validator(mode="after")
    def compression_compat(self) -> "OutputConfig":
        if self.file_format == FileFormat.CSV and self.compression == Compression.SNAPPY:
            raise ValueError("Snappy compression is not supported for CSV. Use gzip or none.")
        return self


class QualityConfig(BaseModel):
    """Data quality checks run after extraction, before sink write."""
    null_check_columns: list[str] = Field(default_factory=list)
    not_empty_columns: list[str] = Field(default_factory=list, description="Columns that must have at least one non-null value")
    row_count_min: Optional[int] = Field(None, ge=0)
    row_count_max: Optional[int] = Field(None, ge=1)
    schema_drift_action: SchemaDriftAction = Field(SchemaDriftAction.WARN)
    dedup_columns: list[str] = Field(default_factory=list, description="Columns to deduplicate on before sink")
    custom_checks: list[str] = Field(default_factory=list, description="SQL expressions that must return 0 rows on failure")

    @model_validator(mode="after")
    def count_range(self) -> "QualityConfig":
        if self.row_count_min and self.row_count_max:
            if self.row_count_min > self.row_count_max:
                raise ValueError("row_count_min must be <= row_count_max")
        return self


class NotificationConfig(BaseModel):
    """Alert routing for job events."""
    on_success: bool = Field(False)
    on_failure: bool = Field(True)
    on_schema_drift: bool = Field(True)
    email: Optional[str] = Field(None, description="Alert email address")
    slack_webhook_url: Optional[str] = Field(None, description="Slack incoming webhook URL")
    pagerduty_key: Optional[str] = Field(None, description="PagerDuty integration key for critical failures")

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: Optional[str]) -> Optional[str]:
        if v and not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", v):
            raise ValueError(f"Invalid email: {v}")
        return v

    @field_validator("slack_webhook_url")
    @classmethod
    def validate_slack_url(cls, v: Optional[str]) -> Optional[str]:
        if v and not v.startswith("https://hooks.slack.com/"):
            raise ValueError("slack_webhook_url must start with https://hooks.slack.com/")
        return v


class RetryConfig(BaseModel):
    """Retry and fault-tolerance settings."""
    max_retries: int = Field(3, ge=0, le=10)
    retry_interval_seconds: int = Field(60, ge=10, le=3600)
    backoff_multiplier: float = Field(2.0, ge=1.0, le=5.0)
    timeout_seconds: int = Field(3600, ge=60, le=86400, description="Max wall-clock time for a single run")
    fail_fast_on_quality: bool = Field(True, description="Abort immediately on data quality failure (no retry)")


class IngestionConfig(BaseModel):
    """
    User-submitted ingestion behaviour config.
    Describes HOW to extract — batch size, schedule, filtering,
    incremental/CDC settings, output format, quality checks, and alerts.

    This is source-type agnostic: the same model works for AWS, GCP,
    Snowflake, Postgres, and Salesforce. Source-specific nuances
    (e.g. Postgres WAL for CDC) are captured in the cdc sub-config.
    """

    # Core
    extraction_mode: ExtractionMode = Field(ExtractionMode.FULL_LOAD, description="full_load | incremental | cdc")
    batch_size: int = Field(100_000, ge=100, le=5_000_000, description="Rows per micro-batch during extraction")
    parallelism: int = Field(1, ge=1, le=32, description="Number of parallel extraction threads / partitions")

    # Sub-configs
    schedule: ScheduleConfig = Field(default_factory=ScheduleConfig)
    incremental: Optional[IncrementalConfig] = Field(None, description="Required when extraction_mode = incremental")
    cdc: Optional[CDCConfig] = Field(None, description="Required when extraction_mode = cdc")
    filter: FilterConfig = Field(default_factory=FilterConfig)
    output: OutputConfig = Field(default_factory=OutputConfig)
    quality: QualityConfig = Field(default_factory=QualityConfig)
    notifications: NotificationConfig = Field(default_factory=NotificationConfig)
    retry: RetryConfig = Field(default_factory=RetryConfig)

    # Misc
    tags: dict[str, str] = Field(default_factory=dict, description="Arbitrary key-value tags propagated to ADF run and ADLS metadata")
    description: Optional[str] = Field(None, max_length=500, description="Human-readable description of this ingestion job")

    # ── Cross-field validation ─────────────────────────────────────────────

    @model_validator(mode="after")
    def incremental_requires_config(self) -> "IngestionConfig":
        if self.extraction_mode == ExtractionMode.INCREMENTAL and not self.incremental:
            raise ValueError("incremental config block is required when extraction_mode = 'incremental'")
        return self

    @model_validator(mode="after")
    def cdc_requires_config(self) -> "IngestionConfig":
        if self.extraction_mode == ExtractionMode.CDC and not self.cdc:
            raise ValueError("cdc config block is required when extraction_mode = 'cdc'")
        return self

    # ── Helpers ───────────────────────────────────────────────────────────

    def effective_watermark(self) -> Optional[str]:
        """Return the current watermark value, if applicable."""
        if self.incremental:
            return self.incremental.last_watermark_value
        if self.cdc:
            return self.cdc.last_lsn_value
        return None

    def to_adf_parameters(self) -> dict[str, str]:
        """Flatten ingestion config into ADF pipeline parameter format."""


class AzureSinkConfig(BaseModel):
    adf_subscription_id: str
    adf_resource_group: str
    adf_factory_name: str
    adf_pipeline_name: str
    adls_account_name: str
    adls_container: str
    adls_sink_path: str = "landing/{source_type}/{job_id}/"
    managed_identity_id: str
    sink_format: str = "parquet"
    write_mode: WriteMode = WriteMode.APPEND
    synapse_workspace: Optional[str] = None
    synapse_pool: Optional[str] = None
    synapse_schema: Optional[str] = None
    synapse_table: Optional[str] = None
    key_vault_name: str
    storage_sas_token: Optional[SecretStr] = None
    log_analytics_workspace_id: Optional[str] = None
    alert_email: Optional[str] = None

    def masked_dump(self) -> dict:
        out = {}
        for name in self.model_fields:
            val = getattr(self, name)
            out[name] = "***" if isinstance(val, SecretStr) else val
        return out


class ETLRuntimePayload(BaseModel):
    job_id: str
    created_at: str
    status: JobStatus
    description: Optional[str]
    source_type: SourceType
    source_label: str
    extraction_mode: ExtractionMode
    batch_size: int
    parallelism: int
    schedule_enabled: bool
    schedule_cron: Optional[str]
    schedule_timezone: str
    watermark_column: Optional[str]
    watermark_type: Optional[str]
    last_watermark: Optional[str]
    lookback_minutes: int
    cdc_mode: Optional[str]
    updated_at_column: Optional[str]
    deleted_at_column: Optional[str]
    last_lsn_value: Optional[str]
    where_clause: Optional[str]
    include_columns: list[str]
    exclude_columns: list[str]
    row_limit: Optional[int]
    file_format: str
    compression: str
    partition_by: list[str]
    max_file_size_mb: int
    add_metadata_cols: bool
    null_check_columns: list[str]
    row_count_min: Optional[int]
    schema_drift_action: str
    dedup_columns: list[str]
    max_retries: int
    retry_interval_seconds: int
    timeout_seconds: int
    notify_on_failure: bool
    notify_email: Optional[str]
    adf_pipeline_name: str
    adls_account_name: str
    adls_container: str
    adls_sink_path: str
    write_mode: str
    synapse_pool: Optional[str]
    synapse_schema: Optional[str]
    synapse_table: Optional[str]
    key_vault_name: str
    source_secret_name: str
    tags: dict[str, str]


class ETLConfigRequest(BaseModel):
    source_config: dict[str, Any]
    ingestion_config: dict[str, Any]
    azure_config: dict[str, Any]


class ETLConfigResponse(BaseModel):
    job_id: str
    runtime_payload: dict[str, Any]
    adf_parameters: dict[str, str]
    kv_secret: dict[str, Any]
