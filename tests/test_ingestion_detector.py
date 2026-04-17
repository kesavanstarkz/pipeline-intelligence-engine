"""
Tests — Ingestion Detector
"""
import pytest
from engine.detectors.base import AnalysisPayload
from engine.detectors.ingestion_detector import IngestionDetector


@pytest.fixture
def detector():
    return IngestionDetector()


def _payload(**kwargs) -> AnalysisPayload:
    return AnalysisPayload(**kwargs)


class TestIngestionDetector:

    def test_detects_glue_job(self, detector):
        p = _payload(metadata={"type": "GlueJob", "name": "customer_etl"})
        r = detector.detect(p)
        assert "AWS Glue Jobs" in r.results

    def test_detects_airflow_dag(self, detector):
        p = _payload(raw_json={"dag_id": "daily_load", "schedule_interval": "@daily"})
        r = detector.detect(p)
        assert "Apache Airflow DAGs" in r.results

    def test_detects_adf_pipeline(self, detector):
        p = _payload(raw_json={"type": "adf_pipeline", "activities": []})
        r = detector.detect(p)
        assert "ADF Pipelines" in r.results

    def test_detects_spark_job(self, detector):
        p = _payload(config={"runner": "spark-submit", "class": "com.example.ETL"})
        r = detector.detect(p)
        assert "Apache Spark Jobs" in r.results

    def test_detects_databricks_job(self, detector):
        p = _payload(raw_json={"databricks_job_id": 12345, "cluster_id": "xyz"})
        r = detector.detect(p)
        assert "Databricks Jobs" in r.results

    def test_detects_dbt(self, detector):
        p = _payload(raw_json={"dbt_project": "analytics", "dbt_run": True})
        r = detector.detect(p)
        assert "dbt Jobs" in r.results

    def test_detects_kafka_connect(self, detector):
        p = _payload(raw_json={"connector.class": "io.debezium.connector.mysql.MySqlConnector"})
        r = detector.detect(p)
        assert "Kafka Connect" in r.results

    def test_detects_fivetran(self, detector):
        p = _payload(raw_json={"tool": "fivetran", "connector": "postgres"})
        r = detector.detect(p)
        assert "Fivetran" in r.results

    def test_detects_airbyte(self, detector):
        p = _payload(raw_json={"source_definition_id": "xxx", "airbyte_connection": "yyy"})
        r = detector.detect(p)
        assert "Airbyte" in r.results

    def test_datahub_data_job_entity(self, detector):
        p = _payload()
        p.datahub_entities = [{
            "type": "dataJob",
            "name": "customer_glue_job",
            "jobType": "GlueJob",
        }]
        r = detector.detect(p)
        assert "AWS Glue Jobs" in r.results

    def test_datahub_data_flow_airflow(self, detector):
        p = _payload()
        p.datahub_entities = [{
            "type": "dataFlow",
            "name": "daily_dag",
            "description": "Airflow DAG for daily ingestion",
        }]
        r = detector.detect(p)
        assert "Apache Airflow DAGs" in r.results

    def test_empty_payload(self, detector):
        p = _payload()
        r = detector.detect(p)
        assert r.results == []

    def test_step_functions(self, detector):
        p = _payload(raw_json={"type": "AWS::StepFunctions::StateMachine"})
        r = detector.detect(p)
        assert "AWS Step Functions" in r.results

    def test_flink_job(self, detector):
        p = _payload(raw_json={"job_class": "org.apache.flink.StreamExecutionEnvironment"})
        r = detector.detect(p)
        assert "Apache Flink Jobs" in r.results

    def test_multiple_ingestion_engines(self, detector):
        p = _payload(
            raw_json={"dag_id": "pipeline", "glue_job_name": "transform"},
        )
        r = detector.detect(p)
        assert "Apache Airflow DAGs" in r.results
        assert "AWS Glue Jobs" in r.results
