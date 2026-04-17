"""
Tests — Framework Detector
"""
import pytest
from engine.detectors.base import AnalysisPayload
from engine.detectors.framework_detector import FrameworkDetector


@pytest.fixture
def detector():
    return FrameworkDetector()


def _payload(**kwargs) -> AnalysisPayload:
    return AnalysisPayload(**kwargs)


class TestFrameworkDetector:

    def test_detects_glue(self, detector):
        p = _payload(metadata={"platform": "glue", "name": "my_job"})
        r = detector.detect(p)
        assert "AWS Glue" in r.results

    def test_detects_redshift_from_jdbc(self, detector):
        p = _payload(config={"connection": "jdbc:redshift://cluster:5439/dev"})
        r = detector.detect(p)
        assert "Amazon Redshift" in r.results

    def test_detects_adf(self, detector):
        p = _payload(raw_json={"type": "adf_pipeline", "name": "ingest_pipeline"})
        r = detector.detect(p)
        assert "Azure Data Factory" in r.results

    def test_detects_databricks(self, detector):
        p = _payload(metadata={"platform": "databricks", "cluster": "my-cluster"})
        r = detector.detect(p)
        assert "Databricks" in r.results

    def test_detects_snowflake(self, detector):
        p = _payload(raw_json={"warehouse": "COMPUTE_WH", "snowflake_account": "xy12345"})
        r = detector.detect(p)
        assert "Snowflake" in r.results

    def test_detects_spark(self, detector):
        p = _payload(config={"runner": "pyspark", "master": "yarn"})
        r = detector.detect(p)
        assert "Apache Spark" in r.results

    def test_detects_airflow(self, detector):
        p = _payload(raw_json={"dag_id": "my_dag", "schedule": "@daily"})
        r = detector.detect(p)
        assert "Apache Airflow" in r.results

    def test_combo_glue_redshift(self, detector):
        p = _payload(
            metadata={"platform": "glue"},
            config={"output": "jdbc:redshift://cluster:5439/db"},
        )
        r = detector.detect(p)
        assert "AWS Glue" in r.results
        assert "Amazon Redshift" in r.results
        assert "Combo: Glue → Redshift" in r.results

    def test_combo_adf_databricks(self, detector):
        p = _payload(raw_json={"type": "adf_pipeline", "compute": "databricks_cluster"})
        r = detector.detect(p)
        assert "Combo: ADF → Databricks" in r.results

    def test_datahub_entity_platform(self, detector):
        p = _payload()
        p.datahub_entities = [{"urn": "urn:li:dataPlatform:snowflake,mydb,PROD"}]
        r = detector.detect(p)
        assert "Snowflake" in r.results

    def test_no_false_positives_on_empty(self, detector):
        p = _payload()
        r = detector.detect(p)
        assert r.results == []
        assert r.confidence == 0.0

    def test_dbt_detection(self, detector):
        p = _payload(raw_json={"dbt_project": "analytics", "models": ["customers"]})
        r = detector.detect(p)
        assert "dbt" in r.results

    def test_emr_detection(self, detector):
        p = _payload(metadata={"cluster_type": "emr", "release": "emr-6.10.0"})
        r = detector.detect(p)
        assert "Amazon EMR" in r.results

    def test_confidence_nonzero_when_found(self, detector):
        p = _payload(metadata={"platform": "glue"})
        r = detector.detect(p)
        assert r.confidence > 0.0

    def test_evidence_populated(self, detector):
        p = _payload(metadata={"platform": "glue"})
        r = detector.detect(p)
        assert len(r.evidence) > 0
