from __future__ import annotations

from unittest.mock import patch

from fastapi.testclient import TestClient

from api.main import app


client = TestClient(app)


def _post(payload: dict) -> dict:
    response = client.post("/analyze/final-config", json=payload)
    assert response.status_code == 200, response.text
    return response.json()


def test_final_config_prefers_raw_and_enriches_from_example():
    body = {
        "raw_pipeline_json": {
            "type": "adf_pipeline",
            "name": "orders_ingest",
            "activities": [
                {
                    "name": "Fetch Orders",
                    "type": "WebActivity",
                    "typeProperties": {
                        "method": "GET",
                        "url": "https://api.contoso.com/orders",
                    },
                },
                {
                    "name": "Copy To Warehouse",
                    "type": "Copy",
                    "dependsOn": [{"activity": "Fetch Orders"}],
                    "typeProperties": {
                        "sink": {
                            "datasetSettings": {
                                "type": "AzureSqlTable",
                                "table": "sales_fact",
                            }
                        },
                        "script": "select count(*) from sales_fact",
                    },
                },
                {
                    "name": "Send Email",
                    "type": "Office365Email",
                    "dependsOn": [{"activity": "Copy To Warehouse"}],
                },
            ],
        },
        "extracted_config": {
            "source_configs": {
                "source_type": "API",
                "service_name": "REST API",
                "connection_details": {
                    "endpoint": "https://api.contoso.com/orders",
                },
                "authentication_type": "Managed Identity",
            },
            "ingestion_configs": {
                "mode": "batch",
                "trigger_type": "schedule",
                "frequency": "daily",
                "destination": "Send Email",
            },
            "dq_rules": ["row_count_check", "schema_check"],
            "flow": {"text": "REST API -> Send Email"},
        },
        "example_config": {
            "pipeline_name": "Daily_Sales_Medallion_Full_ETL_2026",
            "ingestion_overview": {
                "orchestrator": "Azure Data Factory",
            },
            "sink_configuration": {
                "storage": "Azure Data Lake Gen2",
            },
            "source_configs": {
                "connection_details": {
                    "schema": "sales",
                }
            },
            "ingestion_configs": {
                "data_format": "JSON",
                "destination": "example_table",
            },
            "dq_rules": ["row_count_check", "uniqueness_check"],
        },
        "ui_inputs": {
            "platform": "adf",
            "ingestion_type": "batch",
            "dq_preference": "strict",
        },
    }

    data = _post(body)

    final_config = data["final_config"]
    assert final_config["pipeline_name"] == "orders_ingest"
    assert "ingestion_overview" in final_config
    assert "sink_configuration" in final_config
    assert "pipeline_extracted_overlay" in final_config
    assert final_config["pipeline_extracted_overlay"]["ingestion_configs"]["destination"] == "sales_fact"
    assert final_config["ingestion_configs"]["destination"] == "sales_fact"
    assert final_config["source_configs"]["connection_details"]["schema"] == "sales"
    assert final_config["dq_rules"] == ["row_count_check"]
    assert "Send Email" not in final_config["flow"]["text"]
    assert data["final_core"]["ingestion_configs"]["destination"] == "sales_fact"

    corrected_fields = {item["field"] for item in data["merge_report"]["fields_corrected"] if isinstance(item, dict)}
    assert "ingestion_configs.destination" in corrected_fields
    assert "source_configs.connection_details.endpoint" in corrected_fields or "source_configs.connection_details.endpoint" in set(data["merge_report"]["fields_from_extracted"])

    example_fields = set(data["merge_report"]["fields_from_example"])
    assert "source_configs.connection_details.schema" in example_fields

    conflicts = str(data["merge_report"]["conflicts_resolved"]).lower()
    assert "uniqueness_check" in conflicts

    assert float(data["validation_report"]["accuracy_score"]) >= 0.90


def test_final_config_uses_ui_input_only_when_mode_missing():
    body = {
        "raw_pipeline_json": {},
        "extracted_config": {
            "source_configs": {},
            "ingestion_configs": {},
            "dq_rules": [],
            "flow": {},
        },
        "example_config": {},
        "ui_inputs": {
            "platform": "fabric",
            "ingestion_type": "streaming",
            "dq_preference": "standard",
        },
    }

    data = _post(body)

    assert data["final_config"]["ingestion_configs"]["mode"] == "streaming"
    assert "Used UI ingestion_type only because ingestion mode was missing." in data["architect_notes"]


def test_final_config_endpoint_isolated_from_datahub():
    with patch("engine.datahub_client.DataHubClient.health_check", return_value=False):
        data = _post(
            {
                "raw_pipeline_json": {},
                "extracted_config": {},
                "example_config": {},
                "ui_inputs": {},
            }
        )

    assert "final_config" in data
