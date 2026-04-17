# Design Document: Pipeline Intelligence Engine

## Overview

The Pipeline Intelligence Engine is a cloud-agnostic metadata analysis system that detects, classifies, and visualises data pipeline topologies across any cloud environment. It ingests pipeline metadata from heterogeneous sources — AWS, Azure, GCP, Databricks, Snowflake, and any future platform — into DataHub as a unified metadata graph, then applies a layered detection strategy (rule-based patterns → plugin detectors → LLM fallback) to identify frameworks, data sources, ingestion engines, and data quality rules.

The key design principle is **dynamic extensibility**: the engine must detect any pipeline framework or cloud service without requiring code changes to the core. This is achieved through a plugin architecture for detectors, a registry-driven scanner layer for cloud providers, and an LLM fallback that handles novel or unrecognised patterns.

### Key Design Goals

- **Cloud-agnostic**: Works across AWS, Azure, GCP, Databricks, Snowflake, and any future platform via the plugin scanner layer.
- **Dynamically extensible**: New platforms are picked up by registering a new `BaseDetector` or `CloudScanner` subclass — no core changes required.
- **LLM fallback**: Unknown or novel frameworks are handled by a local LLM (Ollama/DeepSeek) that enriches and fills gaps in rule-based detection.
- **DataHub as the metadata backbone**: All pipeline entities flow through DataHub's GMS REST API, providing a single source of truth for lineage and configuration.
- **Fault-tolerant**: Every external call (DataHub, LLM, cloud SDKs) is wrapped in best-effort error handling so a single failure never blocks the analysis pipeline.

---

## Architecture

The system is composed of five layers that interact in a strict top-down flow:

```mermaid
graph TD
    subgraph "Client Layer"
        UI[Web UI\nD3 + Dagre Graph]
        EXT[External Clients\nCI/CD, Scripts]
    end

    subgraph "API Layer"
        API[FastAPI\nPOST /analyze\nGET /config/{urn}\nGET /health\nPOST /scan-cloud]
    end

    subgraph "Engine Layer"
        PIE[PipelineIntelligenceEngine\nOrchestrator]
        REG[Detector Registry\nget_all_detectors\nregister_detector]
        subgraph "Detector Pipeline"
            FD[FrameworkDetector]
            SD[SourceDetector]
            ID[IngestionDetector]
            DQ[DQDetector]
            PL[Plugin Detectors\nuser-defined]
        end
        LLM[LLM Inference Layer\nOllama / DeepSeek]
    end

    subgraph "Data Layer"
        DHC[DataHubClient\nGMS REST API]
        SCN[ScannerManager\nCloudScanner plugins]
        subgraph "Cloud Scanners"
            AWS[AWSScanner]
            AZ[AzureScanner]
            GCP[GCPScanner]
            DB[DatabricksScanner]
            SF[SnowflakeScanner]
        end
    end

    subgraph "External Services"
        DH[DataHub GMS\n:8080]
        AWSC[AWS APIs\nboto3]
        AZC[Azure APIs\nazure-sdk]
        GCPC[GCP APIs\ngoogle-cloud]
        DBC[Databricks SDK]
        SFC[Snowflake Connector]
        OLLAMA[Ollama\nDeepSeek-R1]
    end

    UI --> API
    EXT --> API
    API --> PIE
    PIE --> REG
    REG --> FD & SD & ID & DQ & PL
    PIE --> LLM
    PIE --> DHC
    PIE --> SCN
    SCN --> AWS & AZ & GCP & DB & SF
    AWS --> AWSC
    AZ --> AZC
    GCP --> GCPC
    DB --> DBC
    SF --> SFC
    DHC --> DH
    LLM --> OLLAMA
```

### Request Flow

1. A client sends `POST /analyze` (manual JSON) or `POST /scan-cloud` (live cloud scan).
2. The API layer validates the request and delegates to `PipelineIntelligenceEngine.analyze()`.
3. The engine builds an `AnalysisPayload` and enriches it with DataHub entities (best-effort).
4. All registered detectors run in order, each returning a `DetectionResult`.
5. If `use_llm=True`, the LLM layer enriches and overrides rule-based results.
6. The engine assembles an `AnalysisResult` and the API serialises it as JSON.
7. The UI renders the pipeline graph and populates the intelligence panel.

---

## Components and Interfaces

### 2.1 DataHub Client (`engine/datahub_client.py`)

A thin, stateless HTTP wrapper around the DataHub GMS REST API. It never raises exceptions to callers — all errors are caught, logged, and return `None` or `[]`.

```python
class DataHubClient:
    def search_entities(self, query: str, entity_types: List[str], count: int) -> List[Dict]
    def get_entity(self, urn: str) -> Optional[Dict]
    def get_lineage(self, urn: str, direction: str) -> List[Dict]   # "UPSTREAM" | "DOWNSTREAM"
    def get_aspect(self, urn: str, aspect: str) -> Optional[Dict]
    def get_data_jobs(self, flow_urn: str) -> List[Dict]
    def get_node_config(self, urn: str) -> Dict                     # NEW: aggregates all aspects
    def health_check(self) -> bool
```

The `get_node_config` method fetches the minimum required aspects (`datasetProperties`, `dataFlowInfo`, `dataJobInfo`, `ownership`, `upstreamLineage`) in parallel and merges them into a single dict, omitting any aspect that returns `None`.

Authentication is via a bearer token read from `settings.datahub_token`. The base URL is read from `settings.datahub_gms_url`.

### 2.2 Detector Base Classes (`engine/detectors/base.py`)

```python
@dataclass
class AnalysisPayload:
    metadata: Dict[str, Any]          # free-form pipeline metadata
    config: Dict[str, Any]            # pipeline config block
    raw_json: Dict[str, Any]          # any additional JSON
    datahub_entities: List[Dict]      # populated after DataHub enrichment
    cloud_environment: Optional[CloudEnvironment]  # NEW: account/region grouping

    def all_text(self) -> str         # single lowercase string for pattern matching

@dataclass
class DetectionResult:
    results: List[str]
    confidence: float                 # 0.0 – 1.0
    evidence: List[str]
    raw: Optional[Dict]

    def merge(self, other: DetectionResult) -> DetectionResult

class BaseDetector(ABC):
    name: str                         # unique identifier, used as registry key
    priority: int = 100               # lower = runs earlier (NEW)

    @abstractmethod
    def detect(self, payload: AnalysisPayload) -> DetectionResult: ...
```

### 2.3 Detector Registry (`engine/registry.py`)

The registry is the single point of truth for which detectors are active. It supports runtime registration for plugins.

```python
def get_all_detectors() -> List[BaseDetector]
def get_detector(name: str) -> Optional[BaseDetector]
def register_detector(detector_cls: Type[BaseDetector]) -> None   # raises ValueError on duplicate name
def unregister_detector(name: str) -> None                        # NEW: for testing
```

Detectors are executed in registration order. The four built-in detectors are registered at import time: `FrameworkDetector`, `SourceDetector`, `IngestionDetector`, `DQDetector`.

### 2.4 Dynamic Detection Engine

The detection engine is designed to be **open for extension, closed for modification**. Adding support for a new cloud platform requires only:

1. Creating a new `BaseDetector` subclass with patterns for the new platform.
2. Calling `register_detector(MyNewDetector)` — either at startup or from a plugin file.

No changes to `PipelineIntelligenceEngine`, `registry.py`, or any existing detector are needed.

For platforms that cannot be detected from static patterns alone (novel frameworks, proprietary tools), the LLM fallback handles classification dynamically.

### 2.5 URN/ARN Parser (`engine/urn_parser.py`) — NEW

A dedicated module for parsing and re-serialising DataHub URNs and AWS ARNs.

```python
@dataclass
class ParsedURN:
    platform: str
    environment: str
    flow_id: Optional[str]
    job_id: Optional[str]
    raw: str

    def to_urn(self) -> str           # round-trip serialisation

@dataclass
class ParsedARN:
    service: str
    region: str
    account_id: str
    resource_id: str
    raw: str

def parse_urn(urn: str) -> Optional[ParsedURN]
def parse_arn(arn: str) -> Optional[ParsedARN]
```

### 2.6 Pipeline Graph (`engine/pipeline_graph.py`) — NEW

An in-memory directed graph built from DataHub lineage edges.

```python
@dataclass
class PipelineNode:
    urn: str
    node_type: str                    # "dataset" | "dataJob" | "dataFlow"
    platform: str
    environment: Optional[CloudEnvironment]
    aspects: Dict[str, Any]

@dataclass
class PipelineEdge:
    source_urn: str
    target_urn: str
    edge_type: str                    # "DownstreamOf" | "IsPartOf"

class PipelineGraph:
    def add_node(self, node: PipelineNode) -> None      # deduplicates by URN
    def add_edge(self, edge: PipelineEdge) -> None
    def get_node(self, urn: str) -> Optional[PipelineNode]
    def get_upstream(self, urn: str) -> List[PipelineNode]
    def get_downstream(self, urn: str) -> List[PipelineNode]
    def nodes(self) -> List[PipelineNode]
    def edges(self) -> List[PipelineEdge]
    def to_dict(self) -> Dict                           # {"nodes": [...], "edges": [...]}
    def from_lineage(cls, seed_urn: str, lineage: List[Dict]) -> PipelineGraph  # classmethod
```

### 2.7 Cloud Scanner Layer (`engine/scanner/`)

The scanner layer actively queries cloud provider APIs for live resource data. It follows the same plugin pattern as detectors.

```python
class CloudScanner(ABC):
    name: str                         # NEW: unique identifier
    def can_scan(self, settings: Settings) -> bool
    def scan(self, settings: Settings) -> Dict[str, Any]

class ScannerManager:
    def register_scanner(self, scanner: CloudScanner) -> None   # NEW
    def scan_all(self, settings: Settings) -> Dict[str, Any]
```

Scanners are registered in `ScannerManager.__init__`. Adding a new cloud provider requires only implementing `CloudScanner` and calling `register_scanner`.

### 2.8 LLM Inference Layer (`llm/inference.py`)

The LLM layer is an optional enrichment step. It receives the `AnalysisPayload` and rule-based results, sends them to a local Ollama instance, and returns a structured JSON response that can override or augment the rule-based results.

The LLM is particularly valuable for:
- Novel or proprietary frameworks not in the pattern registry.
- Generating human-readable node titles and subtitles for the UI.
- Inferring pipeline topology (source → compute → storage) from raw resource lists.
- Producing confidence scores for detected components.

The LLM is invoked only when `use_llm=True` and `settings.llm_enabled=True`. Failures are silently swallowed and the rule-based results are returned unchanged.

### 2.9 FastAPI Application (`api/main.py`)

```
POST /analyze          — trigger analysis on a JSON payload
POST /scan-cloud       — trigger live cloud scan + analysis
GET  /config/{urn}     — fetch node configuration from DataHub
GET  /health           — liveness + DataHub connectivity
GET  /detectors        — list registered detectors
GET  /api/config/keys  — get configured provider status (boolean)
POST /api/config/keys  — set cloud credentials
GET  /                 — serve the SPA dashboard
```

### 2.10 Web UI (`templates/index.html`, `static/app.js`)

A single-page application built with Tailwind CSS, D3.js, and Dagre-D3 for graph layout. It provides:

- **Auto Discovery mode**: triggers `POST /scan-cloud` and renders the live topology.
- **Manual Input mode**: accepts JSON in three text areas and calls `POST /analyze`.
- **Interactive graph**: nodes are clickable; clicking opens the Intelligence Panel with node config.
- **Intelligence Panel**: displays AI synthesis, node configuration, DQ rules, and raw evidence.

---

## Data Models

### 3.1 API Request/Response

```python
class AnalyzeRequest(BaseModel):
    metadata: Dict[str, Any] = {}
    config: Dict[str, Any] = {}
    raw_json: Dict[str, Any] = {}
    use_llm: bool = False

class AnalyzeResponse(BaseModel):
    framework: List[str]
    source: List[str]
    ingestion: List[str]
    dq_rules: List[str]
    confidence: Dict[str, Optional[float]]
    llm_inference: Optional[Dict[str, Any]]
    datahub_lineage: List[Dict[str, Any]]
    pipelines: Optional[List[Dict[str, Any]]]
    evidence: Optional[Dict[str, List[str]]]
    nodes: Optional[List[Dict[str, Any]]]
    flow: Optional[Dict[str, Any]]
    source_config: Optional[Dict[str, Any]]
    ingestion_config: Optional[Dict[str, Any]]
    storage_config: Optional[Dict[str, Any]]
    dq_config: Optional[Dict[str, Any]]
    validation: Optional[Dict[str, Any]]
    cloud_environments: Optional[List[CloudEnvironment]]   # NEW
```

### 3.2 Cloud Environment Grouping

```python
@dataclass
class CloudEnvironment:
    provider: str                     # "aws" | "azure" | "gcp" | "databricks" | "snowflake"
    account_id: Optional[str]         # AWS account ID or Azure subscription ID
    region: Optional[str]             # AWS region or Azure location
    project_id: Optional[str]         # GCP project ID
    node_urns: List[str]              # URNs of nodes belonging to this environment
```

Cloud environment grouping is performed by the `CloudEnvironmentGrouper` utility, which parses ARNs and Azure resource IDs to extract account/region/subscription metadata and groups nodes accordingly.

### 3.3 Node Configuration

```python
# GET /config/{urn} response
class NodeConfig(BaseModel):
    urn: str
    aspects: Dict[str, Any]           # only present aspects, no null values
    environment: Optional[CloudEnvironment]
```

### 3.4 Settings

```python
class Settings(BaseSettings):
    # DataHub
    datahub_gms_url: str = "http://localhost:8080"
    datahub_token: Optional[str] = None

    # LLM
    ollama_base_url: str = "http://localhost:11434"
    anthropic_api_key: Optional[str] = None
    llm_model: str = "deepseek-r1:1.5b"
    llm_enabled: bool = True

    # AWS
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None

    # Azure
    azure_tenant_id: Optional[str] = None
    azure_client_id: Optional[str] = None
    azure_client_secret: Optional[str] = None

    # GCP (NEW)
    gcp_project_id: Optional[str] = None
    gcp_service_account_key: Optional[str] = None

    # Databricks
    databricks_host: Optional[str] = None
    databricks_token: Optional[str] = None

    # Snowflake
    snowflake_account: Optional[str] = None
    snowflake_user: Optional[str] = None
    snowflake_password: Optional[str] = None
```

---

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system — essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: DataHub Resilience

*For any* `AnalysisPayload`, if the DataHub client raises any exception during enrichment, the `PipelineIntelligenceEngine.analyze()` method SHALL still return a valid `AnalysisResult` with non-null `framework`, `source`, `ingestion`, and `dq_rules` lists (which may be empty).

**Validates: Requirements 1.4, 2.4**

### Property 2: DataHub Search Result Unification

*For any* combination of per-entity-type result lists returned by the DataHub GMS, the unified list returned by `search_entities` SHALL contain all items from all per-type lists, with no item appearing more than once (deduplication by URN).

**Validates: Requirements 2.3**

### Property 3: URN Round-Trip

*For any* valid DataHub URN string, parsing it with `parse_urn` and then calling `to_urn()` on the result SHALL produce a string that identifies the same entity as the original URN (i.e., the platform, environment, flow ID, and job ID components are preserved).

**Validates: Requirements 3.1, 3.7**

### Property 4: ARN Component Extraction

*For any* valid AWS ARN string, `parse_arn` SHALL extract a non-empty service name, a non-empty region, a non-empty account ID, and a non-empty resource identifier.

**Validates: Requirements 3.2, 11.1**

### Property 5: Detection Evidence Completeness

*For any* `AnalysisPayload` that produces a non-empty detection result list for any category, the corresponding evidence list SHALL be non-empty and SHALL contain at least one entry referencing each detected item.

**Validates: Requirements 3.5**

### Property 6: Zero-Detection Confidence

*For any* `AnalysisPayload` that produces an empty result list for a given detection category, the confidence score for that category SHALL be exactly 0.0.

**Validates: Requirements 3.6, 8.3**

### Property 7: Combination Rule Completeness

*For any* `AnalysisPayload` that triggers all frameworks required by a defined combination rule, the combination label SHALL appear in the framework detection results.

**Validates: Requirements 3.4**

### Property 8: Pipeline Graph Node Deduplication

*For any* sequence of `PipelineNode` additions to a `PipelineGraph` where some URNs are repeated, the graph's `nodes()` list SHALL contain each URN exactly once.

**Validates: Requirements 4.3**

### Property 9: Pipeline Graph Serialisation Round-Trip

*For any* `PipelineGraph`, serialising it to a dict via `to_dict()` SHALL produce an object with a `nodes` key (a list) and an `edges` key (a list), where the count of nodes and edges matches the original graph.

**Validates: Requirements 4.4**

### Property 10: Empty Lineage Produces Seed-Only Graph

*For any* seed URN, constructing a `PipelineGraph` via `from_lineage(seed_urn, [])` SHALL produce a graph containing exactly one node (the seed) and zero edges.

**Validates: Requirements 4.5**

### Property 11: Node Config Omits Absent Aspects

*For any* subset of aspects returned by DataHub for a given URN, the `get_node_config` response SHALL contain exactly those aspect keys and SHALL NOT contain any key with a `None` value.

**Validates: Requirements 5.3**

### Property 12: Analyze Response Shape

*For any* `AnalyzeRequest` (with any combination of metadata, config, raw_json, and use_llm values), the `POST /analyze` response SHALL contain all of the following keys: `framework`, `source`, `ingestion`, `dq_rules`, `confidence`, `llm_inference`, `datahub_lineage`, and `evidence`.

**Validates: Requirements 6.2, 8.4**

### Property 13: API Error Propagation

*For any* exception raised by `PipelineIntelligenceEngine.analyze()`, the `POST /analyze` endpoint SHALL return an HTTP 500 response with a non-empty `detail` field.

**Validates: Requirements 6.4**

### Property 14: Confidence Score Range

*For any* `AnalysisPayload`, all confidence scores in the resulting `AnalysisResult.confidence` dict SHALL be in the range [0.0, 1.0].

**Validates: Requirements 8.1**

### Property 15: LLM Disabled Produces Null Inference

*For any* `AnalysisPayload` analysed with `use_llm=False`, the `llm_inference` field in the result SHALL be `None`.

**Validates: Requirements 9.4**

### Property 16: LLM Override Replaces Rule-Based Results

*For any* LLM response that contains valid `framework`, `source`, or `ingestion` arrays, the corresponding fields in the final `AnalysisResult` SHALL equal the LLM-provided arrays (not the rule-based arrays).

**Validates: Requirements 9.2**

### Property 17: LLM Failure Preserves Rule-Based Results

*For any* exception or invalid JSON response from the LLM service, the `framework`, `source`, `ingestion`, and `dq_rules` fields in the final `AnalysisResult` SHALL equal the rule-based detection results.

**Validates: Requirements 9.3**

### Property 18: LLM Response Cleaning

*For any* string that wraps valid JSON in markdown code fences (` ```json ... ``` `) or `<think>...</think>` tags, the `_safe_parse_json` function SHALL successfully parse and return the inner JSON object.

**Validates: Requirements 9.5**

### Property 19: Plugin Registration Inclusion

*For any* new `BaseDetector` subclass registered via `register_detector`, a subsequent call to `get_all_detectors()` SHALL include an instance of that class.

**Validates: Requirements 10.4**

### Property 20: Duplicate Registration Rejection

*For any* detector name that is already present in the registry, calling `register_detector` with a class of the same name SHALL raise a `ValueError`.

**Validates: Requirements 10.2**

### Property 21: Detector Fault Isolation

*For any* detector that raises an exception during `detect()`, the `PipelineIntelligenceEngine.analyze()` method SHALL still return a valid `AnalysisResult` containing results from all non-failing detectors.

**Validates: Requirements 15.5**

### Property 22: Cloud Environment Grouping Correctness

*For any* list of AWS ARNs from N distinct (account_id, region) combinations, the `CloudEnvironmentGrouper` SHALL produce exactly N `CloudEnvironment` buckets, each containing only the ARNs from its corresponding account/region pair.

**Validates: Requirements 11.4**

### Property 23: URL Decoding Transparency

*For any* URN string, URL-encoding it and passing it as the `{urn}` path parameter to `GET /config/{urn}` SHALL result in the original (decoded) URN being passed to the `DataHubClient`.

**Validates: Requirements 7.5**

### Property 24: Settings Environment Variable Loading

*For any* set of credential environment variables (e.g. `AWS_ACCESS_KEY_ID`, `DATAHUB_TOKEN`), the `Settings` object constructed from those variables SHALL expose the corresponding attribute values matching the environment variable values.

**Validates: Requirements 14.5**

---

## Error Handling

### Layered Fault Tolerance

Every external call is wrapped in a try/except that logs and returns a safe default:

| Layer | Failure Mode | Behaviour |
|---|---|---|
| DataHub enrichment | GMS unreachable / HTTP error | Log warning, continue with empty entities |
| Individual detector | Any exception | Log error with detector name, skip detector, continue |
| LLM inference | Unreachable / invalid JSON | Log warning, return rule-based results unchanged |
| Cloud scanner | SDK exception | Log error with scanner name, skip scanner |
| `GET /config/{urn}` | URN not found | HTTP 404 |
| `GET /config/{urn}` | DataHub unreachable | HTTP 503 |
| `POST /analyze` | Engine exception | HTTP 500 with detail message |

### Error Response Shapes

```json
// 404 — URN not found
{"detail": "URN 'urn:li:dataset:...' not found in DataHub"}

// 503 — DataHub unreachable
{"detail": "DataHub GMS is unreachable at http://localhost:8080"}

// 500 — Engine failure
{"detail": "Analysis failed: <exception message>"}
```

### Logging Strategy

All log lines are structured at the appropriate level:

- `INFO`: Incoming requests (path, method), engine startup, scanner triggers.
- `WARNING`: DataHub enrichment skipped, LLM inference skipped, detector skipped.
- `ERROR`: Detector exception (with detector name), cloud scanner failure, unhandled exception.

---

## Testing Strategy

### Dual Testing Approach

The testing strategy combines **unit/example-based tests** for specific behaviours and **property-based tests** for universal correctness guarantees.

**Property-based testing library**: [`hypothesis`](https://hypothesis.readthedocs.io/) (Python). Each property test runs a minimum of 100 iterations.

Tag format for property tests:
```python
@settings(max_examples=100)
@given(...)
def test_property_N_description():
    # Feature: pipeline-intelligence-engine, Property N: <property_text>
    ...
```

### Unit Tests (Example-Based)

Located in `tests/`. Each test module mirrors a source module:

| Test File | Covers |
|---|---|
| `test_framework_detector.py` | All named frameworks, combination rules, DataHub enrichment |
| `test_source_detector.py` | All source patterns, URN classification |
| `test_ingestion_detector.py` | All ingestion patterns, DataHub job types |
| `test_dq_detector.py` | GE expectations, SQL patterns, custom DQ frameworks |
| `test_api.py` | All API endpoints, error responses, response shape |
| `test_registry.py` | Registration, duplicate rejection, ordering |
| `test_urn_parser.py` | URN/ARN parsing examples, edge cases |
| `test_pipeline_graph.py` | Graph construction, serialisation, neighbour queries |
| `test_cloud_grouper.py` | Account/region grouping examples |

### Property-Based Tests

Located in `tests/test_properties.py`. Each test implements one correctness property from the design:

```python
from hypothesis import given, settings, strategies as st

# Property 1: DataHub Resilience
@settings(max_examples=100)
@given(st.fixed_dictionaries({
    "metadata": st.dictionaries(st.text(), st.text()),
    "config": st.dictionaries(st.text(), st.text()),
    "raw_json": st.dictionaries(st.text(), st.text()),
}))
def test_property_1_datahub_resilience(payload_dict):
    # Feature: pipeline-intelligence-engine, Property 1: DataHub resilience
    ...

# Property 3: URN Round-Trip
@settings(max_examples=200)
@given(st.from_regex(r"urn:li:dataJob:\(urn:li:dataFlow:\([a-z]+,[a-z_]+,PROD\),[a-z_]+\)"))
def test_property_3_urn_round_trip(urn):
    # Feature: pipeline-intelligence-engine, Property 3: URN round-trip
    ...
```

### Integration Tests

Located in `tests/test_integration.py`. These use `TestClient` with mocked DataHub and cloud SDKs:

- Reference pipeline detection (API Gateway → Lambda → S3).
- `GET /config/{urn}` with mocked DataHub responses.
- `POST /scan-cloud` with mocked AWS SDK responses.
- Health endpoint with reachable and unreachable DataHub.

### Smoke Tests

Verified at startup or in a dedicated `tests/test_smoke.py`:

- All named platforms have a corresponding detector or scanner.
- `requirements.txt` contains all required packages.
- `docker-compose` file exists.
- CloudFormation template exists in `infra/`.

---

## Docker and Deployment

### Dockerfile

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8000
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

### docker-compose.yml

The compose file starts three services:

1. **datahub-quickstart**: DataHub GMS + frontend (from `scripts/docker-compose.datahub.yml`).
2. **pipeline-ie**: The FastAPI application, linked to DataHub.
3. **ollama**: Local LLM server for DeepSeek-R1 inference.

Environment variables for cloud credentials are passed through from the host `.env` file. No container rebuild is required to update credentials.

### Environment Variables

All settings are loaded from environment variables (or `.env` file) via `pydantic-settings`. The `POST /api/config/keys` endpoint allows runtime credential updates that are persisted to `.env`.

### Port Configuration

| Service | Default Port | Override |
|---|---|---|
| FastAPI | 8000 | `PORT` env var |
| DataHub GMS | 8080 | `DATAHUB_GMS_URL` |
| DataHub Frontend | 9002 | — |
| Ollama | 11434 | `OLLAMA_BASE_URL` |
