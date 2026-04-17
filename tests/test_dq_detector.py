"""
Tests — DQ Detector
"""
import pytest
from engine.detectors.base import AnalysisPayload
from engine.detectors.dq_detector import DQDetector


@pytest.fixture
def detector():
    return DQDetector()


def _payload(**kwargs) -> AnalysisPayload:
    return AnalysisPayload(**kwargs)


class TestDQDetector:

    # ── Great Expectations ──────────────────────────────────────────────

    def test_detects_ge_suite_name(self, detector):
        p = _payload(raw_json={"suite_name": "customer_suite", "type": "GlueJob"})
        r = detector.detect(p)
        assert any("Great Expectations Suite" in x for x in r.results)

    def test_detects_ge_data_context(self, detector):
        p = _payload(config={"dq_init": "DataContext()"})
        r = detector.detect(p)
        assert any("Great Expectations" in x for x in r.results)

    def test_detects_ge_expectation_type_not_null(self, detector):
        p = _payload(raw_json={
            "expectations": [
                {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "id"}}
            ]
        })
        r = detector.detect(p)
        assert "GE: Not Null check" in r.results

    def test_detects_ge_expectation_uniqueness(self, detector):
        p = _payload(raw_json={
            "expectations": [
                {"expectation_type": "expect_column_values_to_be_unique", "kwargs": {"column": "email"}}
            ]
        })
        r = detector.detect(p)
        assert "GE: Uniqueness check" in r.results

    def test_detects_ge_expectation_between(self, detector):
        p = _payload(raw_json={
            "expectations": [
                {"expectation_type": "expect_column_values_to_be_between",
                 "kwargs": {"column": "age", "min_value": 0, "max_value": 120}}
            ]
        })
        r = detector.detect(p)
        assert "GE: Range check" in r.results

    def test_detects_ge_row_count(self, detector):
        p = _payload(raw_json={
            "expectations": [
                {"expectation_type": "expect_table_row_count_to_be_between",
                 "kwargs": {"min_value": 100, "max_value": 1000000}}
            ]
        })
        r = detector.detect(p)
        assert "GE: Row count range" in r.results

    def test_detects_ge_schema_validation(self, detector):
        p = _payload(raw_json={
            "expectations": [
                {"expectation_type": "expect_table_columns_to_match_ordered_list",
                 "kwargs": {"column_list": ["id", "name", "email"]}}
            ]
        })
        r = detector.detect(p)
        assert "GE: Schema validation" in r.results

    def test_detects_ge_checkpoint(self, detector):
        p = _payload(config={"checkpoint_name": "daily_checkpoint"})
        r = detector.detect(p)
        assert any("Great Expectations" in x for x in r.results)

    # ── SQL DQ ──────────────────────────────────────────────────────────

    def test_detects_sql_null_check(self, detector):
        p = _payload(raw_json={"validation_sql": "SELECT * FROM orders WHERE order_id IS NULL"})
        r = detector.detect(p)
        assert "SQL: Null check" in r.results

    def test_detects_sql_row_count(self, detector):
        p = _payload(config={"assertion": "SELECT COUNT(*) > 1000 FROM events"})
        r = detector.detect(p)
        assert "SQL: Row count assertion" in r.results

    def test_detects_sql_between(self, detector):
        p = _payload(raw_json={"check": "amount BETWEEN 0 AND 999999"})
        r = detector.detect(p)
        assert "SQL: Range/BETWEEN check" in r.results

    def test_detects_sql_freshness(self, detector):
        p = _payload(config={"freshness_check": "created_at > NOW() - INTERVAL '1 hour'"})
        r = detector.detect(p)
        assert "SQL: Data freshness check" in r.results

    def test_detects_sql_duplicate_check(self, detector):
        p = _payload(raw_json={"validation": "duplicate_check on email column"})
        r = detector.detect(p)
        assert "SQL: Duplicate check" in r.results

    # ── Custom / Third-party ────────────────────────────────────────────

    def test_detects_soda(self, detector):
        p = _payload(raw_json={"tool": "soda", "checks": []})
        r = detector.detect(p)
        assert "Soda DQ" in r.results

    def test_detects_dbt_tests(self, detector):
        p = _payload(raw_json={"dbt_test": "unique", "column": "order_id"})
        r = detector.detect(p)
        assert "dbt Tests" in r.results

    def test_detects_monte_carlo(self, detector):
        p = _payload(metadata={"monitor": "monte_carlo", "table": "orders"})
        r = detector.detect(p)
        assert "Monte Carlo" in r.results

    def test_detects_pandera(self, detector):
        p = _payload(config={"validator": "pandera.DataFrameSchema"})
        r = detector.detect(p)
        assert "Pandera (Python DQ)" in r.results

    def test_detects_custom_dq(self, detector):
        p = _payload(raw_json={"class": "CustomDQCheck", "rules": []})
        r = detector.detect(p)
        assert "Custom DQ Framework" in r.results

    # ── DataHub Assertions ──────────────────────────────────────────────

    def test_datahub_assertion_entity(self, detector):
        p = _payload()
        p.datahub_entities = [{
            "type": "assertion",
            "assertionInfo": {"type": "DATASET_FRESHNESS"},
        }]
        r = detector.detect(p)
        assert any("DataHub Assertion" in x for x in r.results)

    def test_empty_payload(self, detector):
        p = _payload()
        r = detector.detect(p)
        assert r.results == []
        assert r.confidence == 0.0

    def test_multiple_ge_expectations(self, detector):
        p = _payload(raw_json={
            "expectations": [
                {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "id"}},
                {"expectation_type": "expect_column_values_to_be_unique",   "kwargs": {"column": "id"}},
                {"expectation_type": "expect_column_values_to_be_between",  "kwargs": {"column": "age"}},
            ]
        })
        r = detector.detect(p)
        assert "GE: Not Null check" in r.results
        assert "GE: Uniqueness check" in r.results
        assert "GE: Range check" in r.results
