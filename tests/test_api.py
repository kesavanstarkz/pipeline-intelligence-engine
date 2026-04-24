"""
Tests — FastAPI /analyze endpoint (integration)
Uses httpx TestClient; DataHub is mocked.
"""
from __future__ import annotations

import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient

from api.main import app

client = TestClient(app)


# ── Helper ───────────────────────────────────────────────────────────────────

def _post(payload: dict) -> dict:
    resp = client.post("/analyze", json=payload)
    assert resp.status_code == 200, resp.text
    return resp.json()


# Patch DataHub so tests don't need a live instance
@pytest.fixture(autouse=True)
def mock_datahub():
    with patch("engine.datahub_client.DataHubClient.search_entities", return_value=[]), \
         patch("engine.datahub_client.DataHubClient.get_lineage", return_value=[]), \
         patch("engine.datahub_client.DataHubClient.health_check", return_value=False):
        yield


# ── Tests ────────────────────────────────────────────────────────────────────

class TestAnalyzeEndpoint:

    def test_glue_redshift_pipeline(self):
        body = {
            "metadata": {"platform": "glue", "name": "customer_etl"},
            "config":   {"output": "jdbc:redshift://cluster:5439/dev"},
        }
        data = _post(body)
        assert "AWS Glue" in data["framework"]
        assert "Amazon Redshift" in data["framework"]
        assert "Combo: Glue → Redshift" in data["framework"]
        assert "JDBC/Redshift" in data["source"]
        assert "AWS Glue Jobs" in data["ingestion"]

    def test_adf_databricks_pipeline(self):
        body = {
            "raw_json": {
                "type": "adf_pipeline",
                "compute": "databricks_cluster",
                "input": "abfss://container@account.dfs.core.windows.net/raw",
            }
        }
        data = _post(body)
        assert "Azure Data Factory" in data["framework"]
        assert "Databricks" in data["framework"]
        assert "Combo: ADF → Databricks" in data["framework"]
        assert "ADLS Gen2" in data["source"]
        assert "ADF Pipelines" in data["ingestion"]

    def test_snowflake_standalone(self):
        body = {
            "metadata": {"snowflake_account": "xy12345", "warehouse": "COMPUTE_WH"},
            "config":   {"input": "s3://raw-bucket/events/"},
        }
        data = _post(body)
        assert "Snowflake" in data["framework"]
        assert "S3" in data["source"]

    def test_great_expectations_dq(self):
        body = {
            "raw_json": {
                "suite_name": "orders_suite",
                "expectations": [
                    {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "id"}},
                    {"expectation_type": "expect_column_values_to_be_unique",   "kwargs": {"column": "id"}},
                ]
            }
        }
        data = _post(body)
        assert any("Great Expectations" in x for x in data["dq_rules"])
        assert "GE: Not Null check" in data["dq_rules"]
        assert "GE: Uniqueness check" in data["dq_rules"]

    def test_sql_dq_validation(self):
        body = {
            "config": {
                "checks": [
                    "SELECT COUNT(*) > 0 FROM orders",
                    "SELECT * FROM users WHERE email IS NULL",
                ]
            }
        }
        data = _post(body)
        assert "SQL: Row count assertion" in data["dq_rules"]
        assert "SQL: Null check" in data["dq_rules"]

    def test_deep_configs_are_returned(self):
        body = {
            "metadata": {"platform": "glue"},
            "config": {
                "input": "s3://landing/orders/",
                "output": "jdbc:redshift://warehouse:5439/analytics",
                "checks": ["SELECT COUNT(*) > 0 FROM orders"],
                "job_name": "orders_etl",
            },
            "raw_json": {
                "suite_name": "orders_suite",
                "expectations": [
                    {
                        "expectation_type": "expect_column_values_to_not_be_null",
                        "kwargs": {"column": "order_id"},
                    }
                ],
            },
        }
        data = _post(body)
        assert data["source_config"]["S3"]["bucket"] == "landing"
        assert data["ingestion_config"]["AWS Glue Jobs"]["job_name"] == "orders_etl"
        assert data["storage_config"]["Amazon S3"]["path_count"] == 1
        assert data["dq_config"]["great_expectations"]["suite_names"] == ["orders_suite"]
        assert data["dq_config"]["sql_checks"]["count"] == 1

    def test_source_support_matrix_marks_supported_and_detection_only_sources(self):
        body = {
            "config": {
                "input": "s3://landing/orders/",
                "query_language": "graphql",
            }
        }
        data = _post(body)

        supported_names = {item["name"] for item in data["source_support"]["catalog"]["supported_source_types"]}
        unsupported_names = {item["name"] for item in data["source_support"]["catalog"]["unsupported_source_types"]}
        detected_supported = {item["name"] for item in data["source_support"]["detected"]["supported_source_types"]}
        detected_unsupported = {item["name"] for item in data["source_support"]["detected"]["unsupported_source_types"]}

        assert "S3" in supported_names
        assert "GraphQL" in unsupported_names
        assert "S3" in detected_supported
        assert "GraphQL" in detected_unsupported

    def test_storage_config_from_cloud_inventory(self):
        body = {
            "raw_json": {
                "raw_cloud_dump": [
                    {
                        "storage_accounts": [
                            {
                                "id": "azure || processedlake",
                                "configuration": {
                                    "Location": "eastus",
                                    "IsHnsEnabled": True,
                                },
                            }
                        ],
                        "fabric_items": [
                            {
                                "id": "fabric || Customer-Lakehouse",
                                "configuration": {
                                    "Type": "Lakehouse",
                                    "OneLakeFilesPath": "https://onelake.dfs.fabric.microsoft.com/ws/lh/Files",
                                },
                            }
                        ],
                    }
                ]
            }
        }
        data = _post(body)
        assert data["storage_config"]["Azure Storage"]["account_count"] == 1
        assert data["storage_config"]["Azure Storage"]["hierarchical_namespace_enabled"] is True
        assert data["storage_config"]["Microsoft Fabric / OneLake"]["path_count"] == 1

    def test_empty_payload_returns_empty_lists(self):
        data = _post({})
        assert data["framework"] == []
        assert data["source"] == []
        assert data["ingestion"] == []
        assert data["dq_rules"] == []

    def test_response_shape(self):
        data = _post({"metadata": {"platform": "glue"}})
        required_keys = {"framework", "source", "ingestion", "dq_rules", "confidence",
                         "llm_inference", "datahub_lineage", "evidence"}
        assert required_keys.issubset(data.keys())

    def test_confidence_scores_present(self):
        body = {"metadata": {"platform": "glue"}, "config": {"input": "s3://bucket/"}}
        data = _post(body)
        assert "framework" in data["confidence"]
        assert "source" in data["confidence"]

    def test_llm_disabled_by_default(self):
        data = _post({"metadata": {"platform": "snowflake"}})
        assert data["llm_inference"] is None

    def test_complex_multi_framework_pipeline(self):
        body = {
            "metadata": {"platform": "glue", "orchestrator": "airflow"},
            "config": {
                "source": "s3://data-lake/raw/",
                "destination": "jdbc:redshift://cluster:5439/analytics",
            },
            "raw_json": {
                "dq": {
                    "suite_name": "production_suite",
                    "expectations": [
                        {"expectation_type": "expect_table_row_count_to_be_between",
                         "kwargs": {"min_value": 1000}},
                    ]
                }
            }
        }
        data = _post(body)
        assert "AWS Glue" in data["framework"]
        assert "Amazon Redshift" in data["framework"]
        assert "S3" in data["source"]
        assert "JDBC/Redshift" in data["source"]
        assert "AWS Glue Jobs" in data["ingestion"]
        assert any("Great Expectations" in x for x in data["dq_rules"])


class TestHealthEndpoint:

    def test_health_returns_200(self):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "datahub_connected" in data
        assert "version" in data


class TestDetectorsEndpoint:

    def test_lists_all_detectors(self):
        resp = client.get("/detectors")
        assert resp.status_code == 200
        detectors = resp.json()["detectors"]
        assert "framework_detector" in detectors
        assert "source_detector" in detectors
        assert "ingestion_detector" in detectors
        assert "dq_detector" in detectors
