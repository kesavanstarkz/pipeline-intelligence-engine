"""
Tests — Source Detector
"""
import pytest
from engine.detectors.base import AnalysisPayload
from engine.detectors.source_detector import SourceDetector


@pytest.fixture
def detector():
    return SourceDetector()


def _payload(**kwargs) -> AnalysisPayload:
    return AnalysisPayload(**kwargs)


class TestSourceDetector:

    def test_detects_s3(self, detector):
        p = _payload(config={"input_path": "s3://my-bucket/data/raw/"})
        r = detector.detect(p)
        assert "S3" in r.results

    def test_detects_s3a(self, detector):
        p = _payload(config={"path": "s3a://bucket/prefix"})
        r = detector.detect(p)
        assert "S3" in r.results

    def test_detects_adls(self, detector):
        p = _payload(config={"path": "abfss://container@account.dfs.core.windows.net/"})
        r = detector.detect(p)
        assert "ADLS Gen2" in r.results

    def test_detects_wasb(self, detector):
        p = _payload(raw_json={"storage": "wasbs://container@account.blob.core.windows.net/"})
        r = detector.detect(p)
        assert "WASB/Azure Blob" in r.results

    def test_detects_jdbc_redshift(self, detector):
        p = _payload(config={"url": "jdbc:redshift://cluster.region.redshift.amazonaws.com:5439/dev"})
        r = detector.detect(p)
        assert "JDBC/Redshift" in r.results

    def test_detects_jdbc_postgres(self, detector):
        p = _payload(config={"url": "jdbc:postgresql://localhost:5432/mydb"})
        r = detector.detect(p)
        assert "JDBC/PostgreSQL" in r.results

    def test_detects_rest_api(self, detector):
        p = _payload(config={"endpoint": "https://api.example.com/v1/events"})
        r = detector.detect(p)
        assert "REST API" in r.results

    def test_detects_kafka(self, detector):
        p = _payload(raw_json={"bootstrap_servers": "kafka-broker:9092", "topic": "events"})
        r = detector.detect(p)
        assert "Kafka Topic" in r.results

    def test_detects_kinesis(self, detector):
        p = _payload(raw_json={"type": "kinesis", "stream_name": "clickstream"})
        r = detector.detect(p)
        assert "Kinesis" in r.results

    def test_detects_sftp(self, detector):
        p = _payload(config={"source": "sftp://datahost/uploads/"})
        r = detector.detect(p)
        assert "SFTP" in r.results

    def test_detects_mongodb(self, detector):
        p = _payload(config={"uri": "mongodb+srv://user:pass@cluster.mongodb.net/mydb"})
        r = detector.detect(p)
        assert "MongoDB" in r.results

    def test_detects_gcs(self, detector):
        p = _payload(config={"bucket": "gs://my-gcs-bucket/prefix"})
        r = detector.detect(p)
        assert "GCS" in r.results

    def test_datahub_upstream_lineage(self, detector):
        p = _payload()
        p.datahub_entities = [{
            "urn": "urn:li:dataset:(urn:li:dataPlatform:redshift,mydb.schema.table,PROD)",
            "upstreamLineage": {
                "upstreams": [
                    {"dataset": "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/key,PROD)"}
                ]
            }
        }]
        r = detector.detect(p)
        assert "S3" in r.results

    def test_multiple_sources(self, detector):
        p = _payload(
            config={"input": "s3://bucket/raw"},
            raw_json={"output": "jdbc:redshift://cluster:5439/db"},
        )
        r = detector.detect(p)
        assert "S3" in r.results
        assert "JDBC/Redshift" in r.results

    def test_empty_payload(self, detector):
        p = _payload()
        r = detector.detect(p)
        assert r.results == []
        assert r.confidence == 0.0

    def test_jdbc_mysql(self, detector):
        p = _payload(config={"url": "jdbc:mysql://host:3306/mydb"})
        r = detector.detect(p)
        assert "JDBC/MySQL" in r.results
