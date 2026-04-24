from fastapi.testclient import TestClient

from api.main import app


client = TestClient(app)


def test_analyze_data_pipelines_adf_payload():
    response = client.post(
        "/analyze/data-pipelines",
        json={
            "metadata": {"platform": "adf", "name": "ingest_pipeline"},
            "config": {
                "source": "abfss://raw@storage.dfs.core.windows.net/",
                "destination": "databricks_cluster",
            },
            "raw_json": {
                "type": "adf_pipeline",
                "activities": [
                    {"name": "transform", "type": "DatabricksNotebook"}
                ],
            },
        },
    )

    assert response.status_code == 200, response.text
    data = response.json()
    assert len(data) == 1
    assert data[0]["type"] == "DataPipeline"
    assert data[0]["pipeline_name"] == "ingest_pipeline"
    assert data[0]["platform"] == "ADF"
    assert data[0]["reformatted"]["source_configs"]["source_type"] == "File"
    assert data[0]["reformatted"]["source_configs"]["service_name"] == "ADLS Gen2"
    assert data[0]["reformatted"]["ingestion_configs"]["destination"] == "databricks_cluster"
    assert data[0]["reformatted"]["flow"]["text"] == "ADLS Gen2 -> transform -> databricks_cluster"
    assert data[0]["original"]["type"] == "adf_pipeline"


def test_analyze_data_pipelines_fabric_inventory():
    response = client.post(
        "/analyze/data-pipelines",
        json={
            "raw_json": {
                "raw_cloud_dump": [
                    {
                        "fabric_items": [
                            {
                                "id": "fabric || Silver-Transformation",
                                "configuration": {
                                    "Type": "Pipeline",
                                    "Workspace": "Analytics-Hub",
                                },
                            }
                        ]
                    }
                ]
            }
        },
    )

    assert response.status_code == 200, response.text
    data = response.json()
    assert len(data) == 1
    assert data[0]["pipeline_name"] == "Silver-Transformation"
    assert data[0]["platform"] == "Fabric"
    assert data[0]["reformatted"]["source_configs"]["connection_details"]["workspace"] == "Analytics-Hub"
    assert data[0]["reformatted"]["flow"]["graph"]["nodes"] == [{"id": "silver_transformation", "type": "process"}]
    assert data[0]["original"]["Type"] == "Pipeline"


def test_analyze_data_pipelines_fabric_definition_extracts_configs():
    response = client.post(
        "/analyze/data-pipelines",
        json={
            "raw_json": {
                "raw_cloud_dump": [
                    {
                        "fabric_items": [
                            {
                                "id": "fabric || API Ingestion",
                                "configuration": {
                                    "Type": "DataPipeline",
                                    "WorkspaceId": "fc8ac783-0fb4-419a-81bf-e206356a658a",
                                    "Definition": {
                                        "pipeline-content.json": {
                                            "properties": {
                                                "activities": [
                                                    {
                                                        "name": "Fetch Orders",
                                                        "type": "WebActivity",
                                                        "typeProperties": {
                                                            "method": "GET",
                                                            "url": "https://api.contoso.com/orders"
                                                        }
                                                    },
                                                    {
                                                        "name": "Notebook 1",
                                                        "type": "TridentNotebook",
                                                        "dependsOn": [{"activity": "Fetch Orders"}],
                                                        "typeProperties": {
                                                            "notebookId": "nb-001"
                                                        }
                                                    }
                                                ]
                                            }
                                        }
                                    }
                                },
                            }
                        ]
                    }
                ]
            }
        },
    )

    assert response.status_code == 200, response.text
    data = response.json()
    assert len(data) == 1
    assert data[0]["pipeline_name"] == "API Ingestion"
    assert data[0]["reformatted"]["source_configs"]["source_type"] == "API"
    assert data[0]["reformatted"]["source_configs"]["service_name"] == "REST API"
    assert data[0]["reformatted"]["source_configs"]["connection_details"]["endpoint"] == "https://api.contoso.com/orders"
    assert data[0]["reformatted"]["ingestion_configs"]["destination"] == "Notebook 1"
    assert data[0]["reformatted"]["flow"]["text"] == "REST API -> Fetch Orders -> Notebook 1 -> Notebook 1"
    assert data[0]["reformatted"]["flow"]["graph"]["nodes"] == [
        {"id": "source_1", "type": "source"},
        {"id": "fetch_orders", "type": "process"},
        {"id": "notebook_1", "type": "process"},
    ]
    assert data[0]["reformatted"]["flow"]["graph"]["edges"] == [
        {"from": "source_1", "to": "fetch_orders"},
        {"from": "fetch_orders", "to": "notebook_1"},
    ]
    assert "pipeline-content.json" in data[0]["original"]


def test_analyze_data_pipelines_ignores_email_notification_as_destination():
    response = client.post(
        "/analyze/data-pipelines",
        json={
            "raw_json": {
                "raw_cloud_dump": [
                    {
                        "fabric_items": [
                            {
                                "id": "fabric || Warehouse Load",
                                "configuration": {
                                    "Type": "DataPipeline",
                                    "WorkspaceId": "ws-001",
                                    "Definition": {
                                        "pipeline-content.json": {
                                            "properties": {
                                                "activities": [
                                                    {
                                                        "name": "Load Sales",
                                                        "type": "Copy",
                                                        "typeProperties": {
                                                            "sink": {
                                                                "datasetSettings": {
                                                                    "type": "WarehouseTable",
                                                                    "artifactId": "wh-001",
                                                                    "schema": "dbo",
                                                                    "table": "sales_fact",
                                                                }
                                                            }
                                                        },
                                                    },
                                                    {
                                                        "name": "Send Alert",
                                                        "type": "Office365Email",
                                                        "dependsOn": [{"activity": "Load Sales"}],
                                                    },
                                                ]
                                            }
                                        }
                                    },
                                },
                            }
                        ]
                    }
                ]
            }
        },
    )

    assert response.status_code == 200, response.text
    data = response.json()
    assert data[0]["reformatted"]["ingestion_configs"]["destination"] == "sales_fact"
    assert data[0]["reformatted"]["flow"]["graph"]["nodes"] == [
        {"id": "load_sales", "type": "ingestion"},
    ]
    assert data[0]["reformatted"]["flow"]["graph"]["edges"] == []
    assert "Send Alert" not in str(data[0]["reformatted"]["ingestion_configs"]["destination"])
    assert "Send Alert" not in data[0]["reformatted"]["flow"]["text"]


def test_analyze_data_pipelines_empty_payload():
    response = client.post("/analyze/data-pipelines", json={})

    assert response.status_code == 200, response.text
    assert response.json() == []
