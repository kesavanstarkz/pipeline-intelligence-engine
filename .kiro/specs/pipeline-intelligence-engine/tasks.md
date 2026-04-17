# Implementation Plan: Pipeline Intelligence Engine

## Overview

Implement the full Pipeline Intelligence Engine: a cloud-agnostic, plugin-driven metadata analysis system with a FastAPI backend, interactive D3/Dagre web UI, Docker deployment, and a comprehensive property-based test suite covering all 24 correctness properties from the design.

The existing codebase provides partial implementations of all major modules. Tasks are ordered so each step builds on the previous one, ending with full integration and wiring.

---

## Tasks

- [x] 1. Harden core data models and base classes
  - Extend `AnalysisPayload` in `engine/detectors/base.py` to add the `cloud_environment: Optional[CloudEnvironment]` field as specified in design §2.2
  - Add `priority: int = 100` class attribute to `BaseDetector` so detectors can declare execution order
  - Add `unregister_detector(name: str)` to `engine/registry.py` for test isolation (design §2.3)
  - Extend `AnalyzeResponse` in `api/models.py` to include `cloud_environments: Optional[List[CloudEnvironment]]`
  - Add `CloudEnvironment` dataclass to `engine/detectors/base.py` with fields: `provider`, `account_id`, `region`, `project_id`, `node_urns`
  - _Requirements: 3.1, 3.2, 10.1, 11.1, 11.2_

- [x] 2. Implement URN/ARN parser module
  - Create `engine/urn_parser.py` with `ParsedURN` and `ParsedARN` dataclasses as specified in design §2.5
  - Implement `parse_urn(urn: str) -> Optional[ParsedURN]` — extract platform, environment, flow_id, job_id from DataHub URN strings (e.g. `urn:li:dataJob:(urn:li:dataFlow:(glue,my_flow,PROD),my_job)`)
  - Implement `ParsedURN.to_urn() -> str` for round-trip re-serialisation
  - Implement `parse_arn(arn: str) -> Optional[ParsedARN]` — extract service, region, account_id, resource_id from AWS ARN strings
  - Handle malformed/partial URNs and ARNs gracefully (return `None`)
  - _Requirements: 3.1, 3.2, 3.7_

  - [ ]* 2.1 Write property test for URN round-trip (Property 3)
    - **Property 3: URN Round-Trip** — for any valid DataHub URN, `parse_urn(urn).to_urn()` produces a string identifying the same entity
    - **Validates: Requirements 3.1, 3.7**
    - Use `hypothesis` `st.from_regex` to generate valid URN strings; assert component preservation

  - [ ]* 2.2 Write property test for ARN component extraction (Property 4)
    - **Property 4: ARN Component Extraction** — for any valid AWS ARN, `parse_arn` extracts non-empty service, region, account_id, and resource_id
    - **Validates: Requirements 3.2, 11.1**
    - Use `st.from_regex` to generate valid ARN strings; assert all four fields are non-empty strings

  - [ ]* 2.3 Write unit tests for URN/ARN parser
    - Create `tests/test_urn_parser.py` covering: Glue dataJob URN, dataFlow URN, Lambda ARN, S3 ARN, malformed inputs returning `None`
    - _Requirements: 3.1, 3.2_

- [x] 3. Implement Pipeline Graph module
  - Create `engine/pipeline_graph.py` with `PipelineNode`, `PipelineEdge`, and `PipelineGraph` classes as specified in design §2.6
  - Implement `PipelineGraph.add_node` with URN-based deduplication
  - Implement `add_edge`, `get_node`, `get_upstream`, `get_downstream`, `nodes()`, `edges()`
  - Implement `to_dict() -> Dict` producing `{"nodes": [...], "edges": [...]}` with URN-identified elements
  - Implement `PipelineGraph.from_lineage(cls, seed_urn, lineage)` classmethod — seed-only graph when lineage is empty
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5_

  - [ ]* 3.1 Write property test for node deduplication (Property 8)
    - **Property 8: Pipeline Graph Node Deduplication** — adding nodes with repeated URNs results in each URN appearing exactly once in `nodes()`
    - **Validates: Requirements 4.3**
    - Use `st.lists(st.text())` to generate URN sequences with duplicates; assert `len(graph.nodes()) == len(set(urns))`

  - [ ]* 3.2 Write property test for serialisation round-trip (Property 9)
    - **Property 9: Pipeline Graph Serialisation Round-Trip** — `to_dict()` produces a dict with `nodes` and `edges` lists whose lengths match the original graph
    - **Validates: Requirements 4.4**
    - Generate arbitrary graphs; assert key presence and count invariants

  - [ ]* 3.3 Write property test for empty lineage seed-only graph (Property 10)
    - **Property 10: Empty Lineage Produces Seed-Only Graph** — `from_lineage(seed_urn, [])` produces exactly one node and zero edges
    - **Validates: Requirements 4.5**
    - Use `st.text(min_size=1)` for seed URNs; assert `len(nodes) == 1` and `len(edges) == 0`

  - [ ]* 3.4 Write unit tests for Pipeline Graph
    - Create `tests/test_pipeline_graph.py` covering: add/get node, add edge, upstream/downstream neighbours, `to_dict` shape, `from_lineage` with non-empty lineage
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5_

- [x] 4. Enhance DataHub client
  - Add `get_node_config(self, urn: str) -> Dict` to `engine/datahub_client.py` — fetches `datasetProperties`, `dataFlowInfo`, `dataJobInfo`, `ownership`, `upstreamLineage` aspects in sequence and merges into a single dict, omitting any aspect that returns `None` (design §2.1)
  - Ensure `health_check` calls `GET /config` (already present) and returns `False` on any exception
  - Verify `get_lineage` supports both `UPSTREAM` and `DOWNSTREAM` directions
  - Add structured logging at WARNING level for every failed API call (urn + aspect name)
  - _Requirements: 2.1, 2.2, 2.4, 2.5, 2.6, 5.1, 5.2, 5.3, 5.4_

  - [ ]* 4.1 Write property test for node config absent-aspect omission (Property 11)
    - **Property 11: Node Config Omits Absent Aspects** — `get_node_config` response contains exactly the returned aspect keys and no key with a `None` value
    - **Validates: Requirements 5.3**
    - Mock `get_aspect` to return `None` for random subsets of aspects; assert no `None` values in result dict

- [~] 5. Implement detector enhancements with dynamic pattern matching
  - Update `engine/detectors/framework_detector.py`: add `AWS Lambda` and `Amazon S3` to `FRAMEWORK_PATTERNS` so the reference pipeline (Req 12.1) is detected; add `GCP` patterns (BigQuery already present, add Dataflow, Pub/Sub, Dataproc)
  - Update `engine/detectors/source_detector.py`: add `BigQuery` source pattern; add `Pub/Sub` streaming source
  - Update `engine/detectors/ingestion_detector.py`: add `GCP Dataflow` and `GCP Cloud Composer` ingestion patterns
  - Ensure all detectors record evidence for every detected item (design §2.2, Property 5)
  - Verify `FrameworkDetector` combination rules cover `AWS Lambda + Amazon S3` → `Combo: Lambda → S3`
  - _Requirements: 1.1, 3.3, 3.4, 3.5, 3.6_

  - [ ]* 5.1 Write property test for detection evidence completeness (Property 5)
    - **Property 5: Detection Evidence Completeness** — for any payload producing a non-empty result list, the evidence list is non-empty and references each detected item
    - **Validates: Requirements 3.5**
    - Use `st.fixed_dictionaries` to generate payloads with known patterns; assert `len(evidence) >= len(results)` when results non-empty

  - [ ]* 5.2 Write property test for zero-detection confidence (Property 6)
    - **Property 6: Zero-Detection Confidence** — any payload producing an empty result list has confidence exactly 0.0
    - **Validates: Requirements 3.6, 8.3**
    - Generate payloads with no recognisable patterns; assert `confidence == 0.0` when `results == []`

  - [ ]* 5.3 Write property test for combination rule completeness (Property 7)
    - **Property 7: Combination Rule Completeness** — any payload triggering all required frameworks for a combination rule produces the combo label in results
    - **Validates: Requirements 3.4**
    - Parametrise over all `COMBINATION_RULES`; assert combo label present when all required frameworks detected

- [~] 6. Implement plugin architecture stubs and registry enhancements
  - Create `engine/detectors/plugins/` directory with `__init__.py`
  - Create `engine/detectors/plugins/example_plugin.py` — a stub `BaseDetector` subclass named `ExamplePluginDetector` with `name = "example_plugin"` and a no-op `detect` method, demonstrating the plugin pattern
  - Implement `unregister_detector(name: str)` in `engine/registry.py` — removes from `_REGISTERED` and `_INSTANCES`; raises `KeyError` if not found
  - Verify `register_detector` raises `ValueError` on duplicate name (already present; add test coverage)
  - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5_

  - [ ]* 6.1 Write property test for plugin registration inclusion (Property 19)
    - **Property 19: Plugin Registration Inclusion** — after `register_detector(cls)`, `get_all_detectors()` includes an instance of that class
    - **Validates: Requirements 10.4**
    - Dynamically create `BaseDetector` subclasses with unique names; assert presence in detector list; clean up with `unregister_detector`

  - [ ]* 6.2 Write property test for duplicate registration rejection (Property 20)
    - **Property 20: Duplicate Registration Rejection** — registering a detector with an already-registered name raises `ValueError`
    - **Validates: Requirements 10.2**
    - Use `st.text(min_size=1)` for names; register once, assert `ValueError` on second registration; clean up

- [~] 7. Implement Cloud Environment Grouper
  - Create `engine/cloud_grouper.py` with `CloudEnvironmentGrouper` class
  - Implement `group(arns: List[str]) -> List[CloudEnvironment]` — parses each ARN with `parse_arn`, groups by `(account_id, region)`, returns one `CloudEnvironment` per unique pair with `node_urns` populated
  - Handle Azure resource IDs: extract subscription ID from `/subscriptions/{sub_id}/...` paths
  - Integrate grouper into `PipelineIntelligenceEngine.analyze()` — attach `cloud_environments` to `AnalysisResult` and include in `AnalyzeResponse`
  - _Requirements: 11.1, 11.2, 11.3, 11.4_

  - [ ]* 7.1 Write property test for cloud environment grouping correctness (Property 22)
    - **Property 22: Cloud Environment Grouping Correctness** — N distinct (account_id, region) combinations produce exactly N `CloudEnvironment` buckets, each containing only its own ARNs
    - **Validates: Requirements 11.4**
    - Use `st.lists` of generated ARNs with controlled account/region values; assert bucket count and membership

  - [ ]* 7.2 Write unit tests for cloud grouper
    - Create `tests/test_cloud_grouper.py` covering: single-account multi-region, multi-account single-region, Azure subscription grouping, empty input
    - _Requirements: 11.1, 11.2, 11.4_

- [~] 8. Implement GCP scanner and enhance ScannerManager
  - Create `engine/scanner/gcp.py` implementing `CloudScanner` with `name = "gcp"` — uses `google-cloud-resource-manager` or `google-cloud-storage` SDK; `can_scan` checks `settings.gcp_project_id`; `scan` discovers GCS buckets, BigQuery datasets, Dataflow jobs
  - Add `gcp_project_id: Optional[str]` and `gcp_service_account_key: Optional[str]` to `config/settings.py`
  - Implement `ScannerManager.register_scanner(scanner: CloudScanner)` in `engine/scanner/manager.py`
  - Register `GCPScanner` in `ScannerManager.__init__` (guarded by import try/except for optional SDK)
  - Add `name: str` class attribute to `CloudScanner` base class in `engine/scanner/base.py`
  - _Requirements: 1.1, 11.1, 11.2_

- [~] 9. Enhance LLM inference layer
  - Verify `_safe_parse_json` in `llm/inference.py` strips both markdown fences and `<think>...</think>` tags (already present; add edge-case coverage)
  - Add fallback: when `json.JSONDecodeError`, return `{"raw_response": text}` (already present; verify)
  - Ensure `llm_infer` returns `None` (not raises) when `settings.llm_enabled` is `False`
  - Ensure `llm_infer` returns `None` (not raises) on any `httpx` exception
  - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6_

  - [ ]* 9.1 Write property test for LLM response cleaning (Property 18)
    - **Property 18: LLM Response Cleaning** — any string wrapping valid JSON in markdown fences or `<think>` tags is successfully parsed by `_safe_parse_json`
    - **Validates: Requirements 9.5**
    - Use `st.dictionaries` to generate JSON objects; wrap in fences/tags via `st.one_of`; assert parsed result equals original object

  - [ ]* 9.2 Write property test for LLM disabled produces null inference (Property 15)
    - **Property 15: LLM Disabled Produces Null Inference** — any payload analysed with `use_llm=False` has `llm_inference == None`
    - **Validates: Requirements 9.4**
    - Use `st.fixed_dictionaries` for payloads; mock engine; assert `llm_inference is None`

  - [ ]* 9.3 Write property test for LLM failure preserves rule-based results (Property 17)
    - **Property 17: LLM Failure Preserves Rule-Based Results** — when LLM raises or returns invalid JSON, `framework`, `source`, `ingestion`, `dq_rules` equal the rule-based results
    - **Validates: Requirements 9.3**
    - Mock `llm_infer` to raise; assert result fields unchanged from pre-LLM values

  - [ ]* 9.4 Write property test for LLM override replaces rule-based results (Property 16)
    - **Property 16: LLM Override Replaces Rule-Based Results** — when LLM returns valid `framework`, `source`, `ingestion` arrays, those fields in `AnalysisResult` equal the LLM arrays
    - **Validates: Requirements 9.2**
    - Mock `llm_infer` to return controlled lists; assert result fields match LLM output

- [~] 10. Implement and complete FastAPI endpoints
  - Add `GET /config/{urn}` endpoint to `api/main.py`:
    - URL-decode the `urn` path parameter before passing to `datahub_client.get_node_config`
    - Return HTTP 404 with `{"detail": "URN '...' not found in DataHub"}` when `get_node_config` returns `{}`
    - Return HTTP 503 with `{"detail": "DataHub GMS is unreachable at ..."}` when DataHub is unreachable
  - Ensure `POST /analyze` returns HTTP 500 with non-empty `detail` on engine exception
  - Ensure `GET /health` returns `datahub_connected: true/false` without raising on unreachable DataHub
  - Ensure `GET /detectors` lists all registered detectors by name
  - Add `nodes` and `cloud_environments` fields to the `AnalyzeResponse` returned by `POST /analyze`
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 7.1, 7.2, 7.3, 7.4, 7.5, 15.1, 15.2, 15.3_

  - [ ]* 10.1 Write property test for analyze response shape (Property 12)
    - **Property 12: Analyze Response Shape** — for any `AnalyzeRequest`, `POST /analyze` response contains all required keys: `framework`, `source`, `ingestion`, `dq_rules`, `confidence`, `llm_inference`, `datahub_lineage`, `evidence`
    - **Validates: Requirements 6.2, 8.4**
    - Use `st.fixed_dictionaries` for request bodies; assert all required keys present in response JSON

  - [ ]* 10.2 Write property test for API error propagation (Property 13)
    - **Property 13: API Error Propagation** — any exception from `PipelineIntelligenceEngine.analyze()` results in HTTP 500 with non-empty `detail`
    - **Validates: Requirements 6.4**
    - Mock engine to raise arbitrary exceptions; assert status 500 and `detail` non-empty

  - [ ]* 10.3 Write property test for URL decoding transparency (Property 23)
    - **Property 23: URL Decoding Transparency** — URL-encoding a URN and passing it to `GET /config/{urn}` results in the original decoded URN being passed to `DataHubClient`
    - **Validates: Requirements 7.5**
    - Use `st.from_regex` for URN strings; URL-encode; mock `get_node_config`; assert called with decoded URN

- [~] 11. Implement confidence scoring and engine orchestration enhancements
  - Verify `PipelineIntelligenceEngine.analyze()` assigns `confidence = 0.0` for any category with empty results
  - Verify confidence values are clamped to `[0.0, 1.0]` before being placed in `AnalysisResult.confidence`
  - When LLM provides confidence values, use them; otherwise retain rule-based values (design §2.8)
  - Wire `CloudEnvironmentGrouper` into `analyze()` — extract ARNs from `raw_json` and `config`, group, attach to result
  - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5_

  - [ ]* 11.1 Write property test for confidence score range (Property 14)
    - **Property 14: Confidence Score Range** — all confidence scores in `AnalysisResult.confidence` are in `[0.0, 1.0]`
    - **Validates: Requirements 8.1**
    - Use `st.fixed_dictionaries` for payloads; assert all values `0.0 <= v <= 1.0`

  - [ ]* 11.2 Write property test for DataHub resilience (Property 1)
    - **Property 1: DataHub Resilience** — if DataHub raises any exception during enrichment, `analyze()` still returns a valid `AnalysisResult` with non-null lists
    - **Validates: Requirements 1.4, 2.4**
    - Mock `DataHubClient` to raise arbitrary exceptions; assert result has non-null `framework`, `source`, `ingestion`, `dq_rules`

  - [ ]* 11.3 Write property test for detector fault isolation (Property 21)
    - **Property 21: Detector Fault Isolation** — a detector raising during `detect()` does not prevent other detectors from running; result contains outputs from non-failing detectors
    - **Validates: Requirements 15.5**
    - Register a detector that always raises; assert other detectors' results still present in `AnalysisResult`

- [~] 12. Checkpoint — run full test suite
  - Run `pytest tests/ -x -q` and ensure all existing tests pass
  - Fix any regressions introduced by tasks 1–11
  - Ensure all tests pass, ask the user if questions arise.

- [~] 13. Create ingestion recipes directory
  - Create `ingestion/` directory with one YAML recipe file per supported source connector:
    - `ingestion/glue.yml` — DataHub Glue ingestion source config
    - `ingestion/redshift.yml` — DataHub Redshift ingestion source config
    - `ingestion/s3.yml` — DataHub S3 ingestion source config
    - `ingestion/adf.yml` — DataHub Azure Data Factory ingestion source config
    - `ingestion/databricks.yml` — DataHub Databricks ingestion source config
    - `ingestion/snowflake.yml` — DataHub Snowflake ingestion source config
    - `ingestion/bigquery.yml` — DataHub BigQuery ingestion source config
  - Each YAML file must include `source.type`, `source.config` (with placeholder credentials), and `sink` pointing to DataHub GMS
  - _Requirements: 1.2, 1.5_

- [~] 14. Enhance web UI — node-click config panel and confidence display
  - Update `static/app.js` `openSidePanel` function to call `GET /config/{urn}` when a node is clicked and display the returned `NodeConfig` aspects in the Intelligence Panel
  - Display a human-readable error message in the panel when `GET /config/{urn}` returns 404 or 503 (not a raw HTTP error)
  - Add a confidence summary section below the graph that renders per-category confidence scores from the `POST /analyze` response
  - Ensure the graph updates without a page reload when a new `POST /analyze` response is received (already present; verify and fix any edge cases)
  - _Requirements: 13.1, 13.2, 13.3, 13.4, 13.5_

- [~] 15. Enhance web UI — auto-discovery mode and GCP/Azure cloud cards
  - Add GCP and Azure cloud provider cards to the Auto Discovery zone in `templates/index.html` (alongside the existing AWS card), each showing connection status based on `GET /api/config/keys`
  - On page load, call `GET /api/config/keys` and update each cloud card's status indicator
  - Add a `resourceMap` entry in `static/app.js` for GCP services (BigQuery, Dataflow, GCS) and Azure services (ADF, Synapse, ADLS)
  - _Requirements: 13.1, 13.3_

- [~] 16. Write property-based test file
  - Create `tests/test_properties.py` with all property tests not already placed as sub-tasks in tasks 2–11
  - Implement the following remaining properties in `test_properties.py`:
    - **Property 2: DataHub Search Result Unification** — unified list from `search_entities` contains all per-type items with no URN duplicates (_Requirements: 2.3_)
    - **Property 24: Settings Environment Variable Loading** — `Settings` constructed from env vars exposes matching attribute values (_Requirements: 14.5_)
  - Ensure all property tests use `@settings(max_examples=100)` and include the feature/property annotation comment
  - Add `hypothesis` to `requirements.txt` if not already present
  - _Requirements: all properties_

  - [ ]* 16.1 Write property test for DataHub search result unification (Property 2)
    - **Property 2: DataHub Search Result Unification** — unified list from `search_entities` contains all per-type items with no URN duplicates
    - **Validates: Requirements 2.3**
    - Mock per-type responses with overlapping URNs; assert unified list has no duplicates and contains all items

  - [ ]* 16.2 Write property test for settings environment variable loading (Property 24)
    - **Property 24: Settings Environment Variable Loading** — `Settings` constructed from env vars exposes matching attribute values
    - **Validates: Requirements 14.5**
    - Use `st.text` for credential values; set env vars via `monkeypatch`; assert `Settings()` attributes match

- [~] 17. Write integration and smoke tests
  - Create `tests/test_integration.py` with `TestClient`-based tests:
    - Reference pipeline detection: POST the `tests/mocks/payload-api-lambda-s3.json` payload and assert `["AWS Lambda", "Amazon S3"]` in `framework`
    - `GET /config/{urn}` with mocked DataHub returning aspects — assert response shape and no `None` values
    - `GET /config/{urn}` with DataHub returning `{}` — assert HTTP 404
    - `POST /scan-cloud` with mocked `AWSScanner.scan` — assert response has `pipelines` list
    - `GET /health` with reachable DataHub — assert `datahub_connected: true`
    - `GET /health` with unreachable DataHub — assert `datahub_connected: false` and HTTP 200
  - Create `tests/test_smoke.py`:
    - Assert `hypothesis` is importable
    - Assert `docker-compose.yml` exists at project root
    - Assert `infra/sample-api-lambda-s3.yml` exists
    - Assert `ingestion/` directory contains at least 5 YAML files
    - Assert all four built-in detectors are registered on import
  - _Requirements: 12.1, 12.2, 14.4, 15.1, 15.2, 15.3_

- [~] 18. Create Docker and deployment files
  - Create `Dockerfile` at project root:
    - Base image `python:3.11-slim`
    - `WORKDIR /app`, `COPY requirements.txt .`, `RUN pip install --no-cache-dir -r requirements.txt`
    - `COPY . .`, `EXPOSE 8000`, `CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]`
  - Create `docker-compose.yml` at project root with three services:
    - `datahub-quickstart`: extends `scripts/docker-compose.datahub.yml` or uses `acryldata/datahub-quickstart` image
    - `pipeline-ie`: builds from `Dockerfile`, depends on `datahub-quickstart`, passes all env vars from `.env`, maps port 8000
    - `ollama`: uses `ollama/ollama` image, maps port 11434
  - Add `hypothesis` to `requirements.txt` (pinned version, e.g. `hypothesis==6.100.0`)
  - _Requirements: 14.1, 14.2, 14.3, 14.4, 14.5_

- [~] 19. Add CloudFormation reference pipeline enhancements
  - Verify `infra/sample-api-lambda-s3.yml` provisions API Gateway, Lambda, and S3 (already present)
  - Create `infra/README.md` documenting how to deploy the reference stack with `sam deploy` and how to use the resulting endpoint with `POST /analyze`
  - _Requirements: 12.3, 12.4_

- [~] 20. Final checkpoint — full test suite and smoke tests
  - Run `pytest tests/ -q` and ensure all tests pass, including integration and smoke tests
  - Verify `GET /health` returns correct shape
  - Verify `POST /analyze` with the Lambda/S3 mock payload returns `AWS Lambda` and `Amazon S3` in `framework`
  - Ensure all tests pass, ask the user if questions arise.

---

## Notes

- Tasks marked with `*` are optional and can be skipped for a faster MVP; all 24 correctness properties are covered across the optional sub-tasks
- Property tests use `hypothesis` with `@settings(max_examples=100)` minimum
- Each task references specific requirements for traceability
- The existing partial implementations are extended in-place — no files are deleted or replaced wholesale
- Cloud scanner SDKs (GCP, Azure) are guarded with `try/except ImportError` so the engine runs without them installed
- Ingestion recipe YAML files use placeholder credentials; real values are injected via environment variables at runtime
