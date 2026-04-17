"""
Pipeline Intelligence Engine — Core Orchestrator
─────────────────────────────────────────────────
Ties together:
  1. DataHub enrichment   — pull entities / lineage for the payload
  2. Detector pipeline    — run all registered detectors in order
  3. LLM inference layer  — optional, runs only if enabled + API key set
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from engine.datahub_client import DataHubClient, datahub_client as _default_client
from engine.detectors.base import AnalysisPayload, DetectionResult
from engine.registry import get_all_detectors
from engine.config_extractor import extract_source_configs, extract_ingestion_configs, extract_expert_config
from llm.inference import llm_infer

logger = logging.getLogger(__name__)


class AnalysisResult:
    """Final structured output returned by the API."""

    def __init__(
        self,
        framework: List[str],
        source: List[str],
        ingestion: List[str],
        dq_rules: List[str],
        confidence: Dict[str, float],
        llm_inference: Optional[Dict[str, Any]],
        datahub_lineage: List[Dict[str, Any]],
        evidence: Optional[Dict[str, List[str]]] = None,
    ):
        self.framework = framework
        self.source = source
        self.ingestion = ingestion
        self.dq_rules = dq_rules
        self.confidence = confidence
        self.llm_inference = llm_inference
        self.datahub_lineage = datahub_lineage
        self.evidence = evidence or {}
        
        self.pipelines = None
        self.nodes = None
        self.flow = None
        self.source_config = None
        self.ingestion_config = None
        self.storage_config = None
        self.dq_config = None
        self.validation = None
        self.expert_extraction = None
        self.detailed_inventory = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "framework":        self.framework,
            "source":           self.source,
            "ingestion":        self.ingestion,
            "dq_rules":         self.dq_rules,
            "confidence":       self.confidence,
            "llm_inference":    self.llm_inference,
            "datahub_lineage":  self.datahub_lineage,
            "evidence":         self.evidence,
            "pipelines":        self.pipelines,
            "nodes":            self.nodes,
            "flow":             self.flow,
            "source_config":    self.source_config,
            "ingestion_config": self.ingestion_config,
            "storage_config":   self.storage_config,
            "dq_config":        self.dq_config,
            "validation":       self.validation,
            "expert_extraction": self.expert_extraction,
            "detailed_inventory": self.detailed_inventory,
        }


class PipelineIntelligenceEngine:
    """
    Main engine. Instantiated once at app startup.
    """

    DETECTOR_CATEGORY_MAP = {
        "framework_detector":  "framework",
        "source_detector":     "source",
        "ingestion_detector":  "ingestion",
        "dq_detector":         "dq_rules",
    }

    def __init__(self, dh_client: Optional[DataHubClient] = None):
        self._dh = dh_client or _default_client

    # ------------------------------------------------------------------

    def analyze(
        self,
        metadata: Dict[str, Any],
        config: Dict[str, Any],
        raw_json: Dict[str, Any],
        use_llm: bool = False,
    ) -> AnalysisResult:
        # 1. Build unified payload
        payload = AnalysisPayload(
            metadata=metadata,
            config=config,
            raw_json=raw_json,
        )

        # 2. Enrich from DataHub (best-effort — never fail the request)
        datahub_entities: List[Dict[str, Any]] = []
        datahub_lineage: List[Dict[str, Any]] = []
        try:
            datahub_entities, datahub_lineage = self._enrich_from_datahub(payload)
            payload.datahub_entities = datahub_entities
        except Exception as exc:  # noqa: BLE001
            logger.warning("DataHub enrichment skipped: %s", exc)

        # 3. Run all detectors
        category_results: Dict[str, List[str]] = {
            "framework": [],
            "source": [],
            "ingestion": [],
            "dq_rules": [],
        }
        confidence_map: Dict[str, float] = {}
        evidence_map: Dict[str, List[str]] = {}

        for detector in get_all_detectors():
            try:
                result: DetectionResult = detector.detect(payload)
                category = self.DETECTOR_CATEGORY_MAP.get(detector.name, detector.name)
                category_results[category].extend(result.results)
                # Deduplicate while preserving order
                category_results[category] = list(dict.fromkeys(category_results[category]))
                confidence_map[category] = round(result.confidence, 2)
                evidence_map[category] = result.evidence
            except Exception as exc:  # noqa: BLE001
                logger.error("Detector %s failed: %s", detector.name, exc)

        # 4. Optional LLM inference
        llm_result: Optional[Dict[str, Any]] = None
        if use_llm:
            try:
                llm_result = llm_infer(payload, category_results)
                if llm_result:
                    # Dynamically override the manual rules with DeepSeek logic
                    if "framework" in llm_result and isinstance(llm_result["framework"], list):
                        category_results["framework"] = llm_result["framework"]
                    if "source" in llm_result and isinstance(llm_result["source"], list):
                        category_results["source"] = llm_result["source"]
                    if "ingestion" in llm_result and isinstance(llm_result["ingestion"], list):
                        category_results["ingestion"] = llm_result["ingestion"]
                    if "dq_rules" in llm_result and isinstance(llm_result["dq_rules"], list):
                        category_results["dq_rules"] = llm_result["dq_rules"]
            except Exception as exc:  # noqa: BLE001
                logger.warning("LLM inference skipped: %s", exc)

        res = AnalysisResult(
            framework=category_results["framework"],
            source=category_results["source"],
            ingestion=category_results["ingestion"],
            dq_rules=category_results["dq_rules"],
            confidence=confidence_map,
            llm_inference=llm_result,
            datahub_lineage=datahub_lineage,
            evidence=evidence_map,
        )

        # Always extract structured configs from the payload (rule-based, no LLM needed)
        res.source_config = extract_source_configs(
            category_results["source"], payload
        ) or None
        res.ingestion_config = extract_ingestion_configs(
            category_results["ingestion"], payload
        ) or None
        res.expert_extraction = extract_expert_config(payload) or None

        # Rule-based detailed inventory fallback (from raw cloud scan results)
        if not res.detailed_inventory and raw_json.get("raw_cloud_dump"):
            inventory = []
            dump = raw_json["raw_cloud_dump"][0] if isinstance(raw_json["raw_cloud_dump"], list) and raw_json["raw_cloud_dump"] else {}
            for service, resources in dump.items():
                for res_meta in resources:
                    if isinstance(res_meta, dict):
                        inventory.append({
                            "id": res_meta.get("id"),
                            "service": str(service).upper(),
                            "config": res_meta.get("configuration", {})
                        })
            res.detailed_inventory = inventory if inventory else None

        if llm_result:
            if "pipelines" in llm_result and isinstance(llm_result["pipelines"], list):
                res.pipelines = llm_result["pipelines"]
            
            # Deep Discovery Config Mapping
            if "nodes" in llm_result and isinstance(llm_result["nodes"], list):
                res.nodes = llm_result["nodes"]
            if "flow" in llm_result and isinstance(llm_result["flow"], dict):
                res.flow = llm_result["flow"]
            if "source" in llm_result and isinstance(llm_result["source"], dict):
                res.source_config = llm_result["source"]
            if "ingestion" in llm_result and isinstance(llm_result["ingestion"], dict):
                res.ingestion_config = llm_result["ingestion"]
            if "storage" in llm_result and isinstance(llm_result["storage"], dict):
                res.storage_config = llm_result["storage"]
            if "dq" in llm_result and isinstance(llm_result["dq"], dict):
                res.dq_config = llm_result["dq"]
            if "validation" in llm_result and isinstance(llm_result["validation"], dict):
                res.validation = llm_result["validation"]
            if "expert_extraction" in llm_result and isinstance(llm_result["expert_extraction"], dict):
                res.expert_extraction = llm_result["expert_extraction"]
            if "detailed_inventory" in llm_result and isinstance(llm_result["detailed_inventory"], list):
                res.detailed_inventory = llm_result["detailed_inventory"]
            
        return res

    # ------------------------------------------------------------------

    def _enrich_from_datahub(
        self, payload: AnalysisPayload
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Try to find relevant DataHub entities based on metadata hints.
        Returns (entities, lineage_edges).
        """
        entities: List[Dict[str, Any]] = []
        lineage: List[Dict[str, Any]] = []

        # Extract search hints from payload
        search_terms = self._extract_search_terms(payload)

        for term in search_terms[:5]:  # cap to 5 lookups per request
            hits = self._dh.search_entities(
                query=term,
                entity_types=["dataset", "dataJob", "dataFlow"],
            )
            entities.extend(hits)

            # Fetch lineage for first dataset hit
            for hit in hits[:2]:
                urn = hit.get("urn", "")
                if urn:
                    upstream = self._dh.get_lineage(urn, direction="UPSTREAM")
                    lineage.extend(upstream)

        # Deduplicate by URN
        seen_urns: set[str] = set()
        unique_entities: List[Dict[str, Any]] = []
        for e in entities:
            urn = e.get("urn", "")
            if urn not in seen_urns:
                seen_urns.add(urn)
                unique_entities.append(e)

        return unique_entities, lineage

    @staticmethod
    def _extract_search_terms(payload: AnalysisPayload) -> List[str]:
        """Pull likely entity names / identifiers from the payload."""
        terms: List[str] = []
        for key in ("name", "pipeline_name", "job_name", "flow_name", "dataset"):
            for source in (payload.metadata, payload.config, payload.raw_json):
                val = source.get(key)
                if val and isinstance(val, str):
                    terms.append(val)
        # Fallback: use platform if present
        platform = payload.metadata.get("platform") or payload.raw_json.get("platform")
        if platform:
            terms.append(str(platform))
        return list(dict.fromkeys(terms))  # deduplicate
