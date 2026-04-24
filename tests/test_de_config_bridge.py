from unittest.mock import MagicMock, patch

from engine.pipeline_engine import (
    _build_de_config_bridge_payload,
    _dispatch_pipeline_reports_to_de_config_tool,
    _push_to_de_config_tool,
)


def test_build_bridge_payload_from_data_pipeline_report():
    report = {
        "type": "DataPipeline",
        "pipeline_name": "API Ingestion",
        "platform": "Fabric",
        "original": {"raw": True},
        "reformatted": {
            "pipeline_name": "API Ingestion",
            "platform": "Fabric",
            "source_configs": {
                "source_type": "DB",
                "service_name": "DataWarehouse",
            },
            "ingestion_configs": {
                "mode": "Batch",
                "destination": "DataWarehouse",
            },
            "dq_rules": ["row_count_check", "schema_check"],
            "flow": {"text": "DataWarehouse -> Notebook 1 -> DataWarehouse", "graph": {"nodes": [], "edges": []}},
            "missing_fields_analysis": [{"field": "source_configs.connection_details.artifactId", "reason": "missing"}],
        },
    }

    payload = _build_de_config_bridge_payload(report)

    assert payload == {
        "pipeline_name": "API Ingestion",
        "platform": "Fabric",
        "flow": "DataWarehouse -> Notebook 1 -> DataWarehouse",
        "source_config": {
            "source_type": "DB",
            "service_name": "DataWarehouse",
        },
        "ingestion_config": {
            "mode": "Batch",
            "destination": "DataWarehouse",
        },
        "dq_rules": ["row_count_check", "schema_check"],
        "missing_fields_analysis": [{"field": "source_configs.connection_details.artifactId", "reason": "missing"}],
        "raw_pipeline_json": {"raw": True},
    }


def test_push_to_de_config_tool_posts_to_bridge_endpoint():
    payload = {"pipeline_name": "API Ingestion"}
    response = MagicMock()
    response.status_code = 200

    with patch("engine.pipeline_engine.settings.de_config_tool_url", "http://config-tool:9000"), \
         patch("engine.pipeline_engine.httpx.Client") as client_cls:
        client = client_cls.return_value.__enter__.return_value
        client.post.return_value = response

        _push_to_de_config_tool(payload)

    client.post.assert_called_once_with(
        "http://config-tool:9000/api/ingest/pipeline",
        json=payload,
    )
    response.raise_for_status.assert_called_once()


def test_dispatch_pipeline_reports_only_pushes_data_pipelines():
    reports = [
        {
            "type": "DataPipeline",
            "pipeline_name": "Pipeline A",
            "platform": "Fabric",
            "reformatted": {
                "source_configs": {},
                "ingestion_configs": {},
                "dq_rules": [],
                "flow": {"text": "A -> B", "graph": {"nodes": [], "edges": []}},
                "missing_fields_analysis": [],
            },
        },
        {"type": "Other", "reformatted": {}},
    ]

    with patch("engine.pipeline_engine.settings.de_config_tool_url", "http://config-tool:9000"), \
         patch("engine.pipeline_engine.threading.Thread") as thread_cls:
        thread = thread_cls.return_value

        _dispatch_pipeline_reports_to_de_config_tool(reports)

    thread_cls.assert_called_once()
    worker = thread_cls.call_args.kwargs["target"]

    with patch("engine.pipeline_engine._push_to_de_config_tool") as push_mock:
        worker()

    push_mock.assert_called_once()
    pushed_payload = push_mock.call_args.args[0]
    assert pushed_payload["pipeline_name"] == "Pipeline A"
