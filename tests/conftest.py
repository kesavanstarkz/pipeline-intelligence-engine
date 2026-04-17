"""
Shared pytest fixtures and configuration.
"""
import pytest
from engine.detectors.base import AnalysisPayload


@pytest.fixture
def empty_payload() -> AnalysisPayload:
    return AnalysisPayload()


@pytest.fixture
def glue_redshift_payload() -> AnalysisPayload:
    return AnalysisPayload(
        metadata={"platform": "glue", "name": "customer_etl_job"},
        config={"output_connection": "jdbc:redshift://prod-cluster:5439/analytics"},
        raw_json={"type": "GlueJob", "schedule": "cron(0 2 * * ? *)"},
    )


@pytest.fixture
def adf_databricks_payload() -> AnalysisPayload:
    return AnalysisPayload(
        metadata={"platform": "adf", "name": "ingest_pipeline"},
        config={"source": "abfss://raw@storage.dfs.core.windows.net/"},
        raw_json={"type": "adf_pipeline", "compute": "databricks_cluster"},
    )


@pytest.fixture
def snowflake_payload() -> AnalysisPayload:
    return AnalysisPayload(
        metadata={"snowflake_account": "xy12345.us-east-1", "warehouse": "COMPUTE_WH"},
        config={"input": "s3://data-lake/events/", "output": "snowflake://db/schema/table"},
    )


@pytest.fixture
def ge_dq_payload() -> AnalysisPayload:
    return AnalysisPayload(
        raw_json={
            "suite_name": "production_suite",
            "expectations": [
                {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "id"}},
                {"expectation_type": "expect_column_values_to_be_unique",   "kwargs": {"column": "email"}},
                {"expectation_type": "expect_table_row_count_to_be_between","kwargs": {"min_value": 1000}},
            ],
        }
    )
