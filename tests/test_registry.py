"""
Tests — Detector Registry
"""
import pytest
from engine.registry import get_all_detectors, get_detector, register_detector
from engine.detectors.base import AnalysisPayload, BaseDetector, DetectionResult


class TestRegistry:

    def test_all_detectors_returned(self):
        detectors = get_all_detectors()
        names = [d.name for d in detectors]
        assert "framework_detector" in names
        assert "source_detector" in names
        assert "ingestion_detector" in names
        assert "dq_detector" in names

    def test_get_detector_by_name(self):
        d = get_detector("framework_detector")
        assert d is not None
        assert d.name == "framework_detector"

    def test_get_unknown_detector_returns_none(self):
        assert get_detector("nonexistent") is None

    def test_register_new_detector(self):
        class MyTestDetector(BaseDetector):
            name = "my_test_detector_unique_xyz"

            def detect(self, payload: AnalysisPayload) -> DetectionResult:
                return DetectionResult(results=["MyTool"], confidence=1.0)

        register_detector(MyTestDetector)
        d = get_detector("my_test_detector_unique_xyz")
        assert d is not None
        result = d.detect(AnalysisPayload())
        assert "MyTool" in result.results

    def test_register_duplicate_raises(self):
        class DupDetector(BaseDetector):
            name = "framework_detector"  # already registered

            def detect(self, payload):
                return DetectionResult()

        with pytest.raises(ValueError, match="already registered"):
            register_detector(DupDetector)
