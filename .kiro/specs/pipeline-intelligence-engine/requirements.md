# Requirements Document

## Introduction

The Pipeline Intelligence Engine is a modular, extensible system that ingests pipeline metadata from heterogeneous cloud data platforms (AWS Glue, Amazon Redshift, Azure Data Factory, Azure Synapse, Databricks, Snowflake, EMR, Lambda, S3, and others) into DataHub as a unified metadata graph. A Python-based Detection Engine queries this graph via the DataHub Graph API to identify pipeline nodes (datasets and jobs), parse their URNs/ARNs, infer the technology stack, and assign confidence scores. A FastAPI service exposes endpoints to trigger analysis and retrieve per-node configuration. A web UI visualises the resulting pipeline graph and allows users to inspect any node's full configuration on demand. Each component is containerised for portability.

---

## Glossary

- **Detection_Engine**: The Python subsystem that queries DataHub, applies rule-based and LLM-based logic, and produces structured detection results.
- **DataHub_Client**: The thin HTTP wrapper around the DataHub GMS REST API used by the Detection_Engine.
- **Pipeline_Graph**: The in-memory directed graph of pipeline nodes (datasets and jobs) and their lineage edges, built from DataHub entity data.
- **URN**: Uniform Resource Name — the DataHub identifier for any entity (e.g. `urn:li:dataJob:(urn:li:dataFlow:(glue,my_flow,PROD),my_job)`).
- **ARN**: Amazon Resource Name — the AWS identifier for cloud resources (e.g. `arn:aws:glue:us-east-1:123456789012:job/my_job`).
- **Detector**: A self-contained plugin that receives an `AnalysisPayload` and returns a `DetectionResult` with results, confidence score, and evidence.
- **AnalysisPayload**: The unified input object passed to every Detector, containing metadata, config, raw_json, and enriched DataHub entities.
- **DetectionResult**: The standardised output of a Detector containing a list of detected items, a confidence score (0–1), and a human-readable evidence trail.
- **Confidence_Score**: A float in the range [0.0, 1.0] representing the Detection_Engine's certainty about a detection result.
- **LLM_Layer**: The optional inference layer that sends payload and rule-based results to a local LLM (Ollama/DeepSeek) for enrichment and gap-filling.
- **Plugin**: A user-defined Detector subclass that can be registered at runtime without modifying core engine files.
- **Node_Config**: The full set of DataHub aspect data (e.g. `datasetProperties`, `dataFlowInfo`, `dataJobInfo`) for a single pipeline node.
- **Ingestion_Recipe**: A YAML file that configures a DataHub ingestion source connector (e.g. Glue, Redshift, ADF).
- **API_Gateway**: The FastAPI application that exposes HTTP endpoints for triggering analysis and retrieving node configurations.
- **UI**: The single-page web application that visualises the Pipeline_Graph and displays Node_Config on node click.
- **GMS**: DataHub's Generalized Metadata Service — the REST API backend.
- **Scanner**: The cloud-provider SDK layer that actively queries AWS, Azure, Databricks, and Snowflake for live resource data.

---

## Requirements

### Requirement 1: DataHub Metadata Ingestion

**User Story:** As a data platform engineer, I want to ingest pipeline metadata from multiple cloud sources into DataHub, so that all pipeline artifacts are available in a single unified metadata graph.

#### Acceptance Criteria

1. THE Detection_Engine SHALL support ingestion of pipeline metadata from at least the following sources: AWS Glue, Amazon Redshift, Azure Data Factory, Azure Synapse, Databricks, Snowflake, Amazon EMR, AWS Lambda, and Amazon S3.
2. WHEN an Ingestion_Recipe YAML file is provided for a supported source, THE Detection_Engine SHALL invoke the DataHub ingestion pipeline using that recipe without requiring code changes.
3. WHEN a Python ingestion script is executed for a supported source, THE Detection_Engine SHALL use the DataHub Pipeline API to programmatically ingest entities into DataHub.
4. IF a DataHub GMS endpoint is unreachable during ingestion, THEN THE Detection_Engine SHALL log a warning and continue processing remaining sources without raising an unhandled exception.
5. THE Detection_Engine SHALL store ingestion recipes in an `ingestion/` directory, with one YAML file per source connector.

---

### Requirement 2: DataHub Graph Querying

**User Story:** As a data engineer, I want the Detection Engine to query the DataHub Graph API for pipeline entities and their aspects, so that I can retrieve a complete picture of all jobs, flows, and datasets in the metadata graph.

#### Acceptance Criteria

1. WHEN the Detection_Engine queries DataHub, THE DataHub_Client SHALL fetch entities of types `dataJob`, `dataFlow`, `dataProcessInstance`, and `dataset`.
2. WHEN a URN is provided, THE DataHub_Client SHALL retrieve the following aspects for that entity: `datasetProperties`, `dataFlowInfo`, `dataJobInfo`, and `upstreamLineage`.
3. WHEN a search query is issued, THE DataHub_Client SHALL return results across all supported entity types in a single unified list.
4. IF a DataHub API call returns an HTTP error status, THEN THE DataHub_Client SHALL log the error with the URN and aspect name, and return `None` for that call without propagating the exception.
5. THE DataHub_Client SHALL support bearer-token authentication by reading the token from the application settings.
6. WHEN lineage is requested for a URN, THE DataHub_Client SHALL support both `UPSTREAM` and `DOWNSTREAM` traversal directions.

---

### Requirement 3: URN/ARN Parsing and Framework Detection

**User Story:** As a data platform engineer, I want the Detection Engine to parse URNs and ARNs and apply rule-based logic to identify the technology stack, so that I can understand which frameworks are in use across my pipelines.

#### Acceptance Criteria

1. WHEN a DataHub URN is parsed, THE Detection_Engine SHALL extract the platform name, environment, flow identifier, and job identifier from the URN string.
2. WHEN an AWS ARN is parsed, THE Detection_Engine SHALL extract the service name, region, account ID, and resource identifier from the ARN string.
3. THE Detection_Engine SHALL detect the following frameworks from URN/ARN patterns and payload content: AWS Glue, Amazon Redshift, Azure Data Factory, Azure Synapse, Azure Databricks, Snowflake, Amazon EMR, AWS Lambda, and Amazon S3.
4. WHEN multiple frameworks are detected in a single payload, THE Detection_Engine SHALL apply combination rules to produce composite labels (e.g. "Combo: Glue → Redshift").
5. WHEN a framework is detected, THE Detection_Engine SHALL record the matching pattern and framework name as evidence in the DetectionResult.
6. IF no framework patterns match the payload, THEN THE Detection_Engine SHALL return an empty results list with a Confidence_Score of 0.0.
7. FOR ALL valid URN strings, parsing then re-serialising the extracted components SHALL produce a URN that identifies the same entity as the original (round-trip property).

---

### Requirement 4: Pipeline Graph Construction

**User Story:** As a data engineer, I want the Detection Engine to build a directed pipeline graph from DataHub lineage edges, so that I can understand the flow of data between nodes.

#### Acceptance Criteria

1. WHEN DataHub lineage data is retrieved, THE Detection_Engine SHALL construct a directed Pipeline_Graph where each node represents a dataset or job URN and each edge represents a lineage relationship.
2. THE Pipeline_Graph SHALL expose the list of all nodes, the list of all directed edges, and a method to retrieve the direct upstream and downstream neighbours of any node.
3. WHEN a node is added to the Pipeline_Graph, THE Detection_Engine SHALL deduplicate nodes by URN so that each URN appears at most once.
4. WHEN the Pipeline_Graph is serialised to JSON, THE Detection_Engine SHALL produce an object containing a `nodes` array and an `edges` array, each element identified by URN.
5. IF a lineage API call returns an empty list, THEN THE Detection_Engine SHALL produce a Pipeline_Graph containing only the seed node with no edges.

---

### Requirement 5: Node Configuration Extraction

**User Story:** As a data engineer, I want to retrieve the full configuration of any pipeline node by URN, so that I can inspect its properties without navigating the DataHub UI.

#### Acceptance Criteria

1. WHEN a URN is provided to the node configuration extractor, THE Detection_Engine SHALL fetch all available aspects for that URN from DataHub and return them as a single JSON object.
2. THE Detection_Engine SHALL fetch at minimum the following aspects per node: `datasetProperties`, `dataFlowInfo`, `dataJobInfo`, `ownership`, and `upstreamLineage`.
3. IF an aspect is not present for a given URN, THEN THE Detection_Engine SHALL omit that aspect key from the returned JSON rather than returning a null value.
4. WHEN node configuration is requested for a URN that does not exist in DataHub, THE Detection_Engine SHALL return an empty JSON object and log a warning.

---

### Requirement 6: FastAPI Analysis Endpoint

**User Story:** As a developer, I want a REST API endpoint to trigger pipeline analysis, so that I can integrate the Detection Engine into automated workflows and CI/CD pipelines.

#### Acceptance Criteria

1. THE API_Gateway SHALL expose a `POST /analyze` endpoint that accepts a JSON body containing optional `metadata`, `config`, `raw_json`, and `use_llm` fields.
2. WHEN `POST /analyze` is called, THE API_Gateway SHALL invoke the Detection_Engine and return a JSON response containing `framework`, `source`, `ingestion`, `dq_rules`, `confidence`, `datahub_lineage`, `pipelines`, and `evidence` fields.
3. WHEN `use_llm` is `true` in the request, THE API_Gateway SHALL pass the payload to the LLM_Layer after rule-based detection completes.
4. IF the Detection_Engine raises an unhandled exception during analysis, THEN THE API_Gateway SHALL return an HTTP 500 response with a descriptive error message and log the full stack trace.
5. THE API_Gateway SHALL return HTTP 200 for all successful analysis requests, including requests where no frameworks are detected.
6. WHEN all three input fields (`metadata`, `config`, `raw_json`) are empty, THE API_Gateway SHALL still invoke the Detection_Engine and return a valid response with empty detection lists.

---

### Requirement 7: FastAPI Node Configuration Endpoint

**User Story:** As a UI developer, I want a REST API endpoint to fetch the full configuration of any pipeline node by URN, so that the UI can display node details on demand.

#### Acceptance Criteria

1. THE API_Gateway SHALL expose a `GET /config/{urn}` endpoint that accepts a URL-encoded URN path parameter.
2. WHEN `GET /config/{urn}` is called with a valid URN, THE API_Gateway SHALL return the Node_Config JSON object for that URN.
3. IF the URN does not exist in DataHub, THEN THE API_Gateway SHALL return an HTTP 404 response with a message indicating the URN was not found.
4. IF the DataHub GMS is unreachable when `GET /config/{urn}` is called, THEN THE API_Gateway SHALL return an HTTP 503 response with a message indicating the upstream service is unavailable.
5. THE API_Gateway SHALL URL-decode the URN path parameter before passing it to the DataHub_Client.

---

### Requirement 8: Confidence Scoring

**User Story:** As a data platform engineer, I want each detection result to include a confidence score, so that I can prioritise high-confidence detections and flag uncertain results for review.

#### Acceptance Criteria

1. THE Detection_Engine SHALL assign a Confidence_Score in the range [0.0, 1.0] to each detection category (framework, source, ingestion, dq_rules).
2. WHEN multiple Detectors contribute results to the same category, THE Detection_Engine SHALL use the minimum Confidence_Score across all contributing Detectors for that category.
3. WHEN no items are detected in a category, THE Detection_Engine SHALL assign a Confidence_Score of 0.0 for that category.
4. THE API_Gateway SHALL include the per-category Confidence_Score map in every `POST /analyze` response.
5. WHEN the LLM_Layer overrides rule-based results, THE Detection_Engine SHALL preserve the LLM-provided confidence values if present, otherwise retain the rule-based confidence values.

---

### Requirement 9: LLM Fallback Inference

**User Story:** As a data platform engineer, I want an optional LLM inference layer that enriches detection results for uncertain or unrecognised cases, so that the engine can handle novel pipeline patterns beyond the rule set.

#### Acceptance Criteria

1. WHEN `use_llm` is `true` and the LLM service is reachable, THE LLM_Layer SHALL send the AnalysisPayload and rule-based results to the configured LLM model and return a structured JSON response.
2. WHEN the LLM_Layer returns valid framework, source, or ingestion arrays, THE Detection_Engine SHALL replace the corresponding rule-based result lists with the LLM-provided lists.
3. IF the LLM service is unreachable or returns an invalid response, THEN THE Detection_Engine SHALL log a warning and return the rule-based results unchanged.
4. WHEN `use_llm` is `false`, THE LLM_Layer SHALL not be invoked and the `llm_inference` field in the response SHALL be `null`.
5. THE LLM_Layer SHALL strip markdown code fences and `<think>` tags from the LLM response before attempting JSON parsing.
6. WHEN the LLM response cannot be parsed as valid JSON, THE LLM_Layer SHALL return a fallback object containing the raw response text under a `raw_response` key.

---

### Requirement 10: Plugin Architecture for Extensible Detection

**User Story:** As a developer, I want to register custom detector plugins at runtime without modifying core engine files, so that I can extend framework detection for proprietary or emerging platforms.

#### Acceptance Criteria

1. THE Detection_Engine SHALL provide a `register_detector` function that accepts a Detector subclass and adds it to the active detector pipeline.
2. WHEN a Detector with a duplicate name is registered, THE Detection_Engine SHALL raise a `ValueError` with a message identifying the conflicting name.
3. THE Detection_Engine SHALL store plugin stub classes in a `detector/plugins/` directory, each implementing the `BaseDetector` interface.
4. WHEN `get_all_detectors()` is called after a plugin is registered, THE Detection_Engine SHALL include the newly registered Detector in the returned list.
5. THE Detection_Engine SHALL execute all registered Detectors in registration order for every analysis request.

---

### Requirement 11: Cloud Environment Grouping

**User Story:** As a data platform engineer, I want pipeline nodes to be grouped by cloud account/region or Azure subscription, so that I can understand the organisational boundaries of my pipelines.

#### Acceptance Criteria

1. WHEN AWS pipeline nodes are detected, THE Detection_Engine SHALL extract the AWS account ID and region from each ARN and attach them as node metadata.
2. WHEN Azure pipeline nodes are detected, THE Detection_Engine SHALL extract the Azure subscription ID from each resource identifier and attach it as node metadata.
3. THE API_Gateway SHALL include account/region grouping metadata in the `POST /analyze` response when cloud nodes are present.
4. WHEN nodes from multiple AWS accounts or regions are present in a single analysis, THE Detection_Engine SHALL group them into separate environment buckets in the response.

---

### Requirement 12: Test Pipeline Validation (REST → Lambda → S3)

**User Story:** As a developer, I want a reference test pipeline (API Gateway → Lambda → S3) deployed via CloudFormation, so that I can validate end-to-end detection and node configuration retrieval against a known topology.

#### Acceptance Criteria

1. THE Detection_Engine SHALL detect `["AWS Lambda", "Amazon S3"]` as the framework list when analysing the reference REST → Lambda → S3 pipeline payload.
2. WHEN the reference pipeline is ingested into DataHub, THE DataHub_Client SHALL be able to retrieve node configurations for all Lambda and S3 URNs via `GET /config/{urn}`.
3. THE infra directory SHALL contain a CloudFormation template that provisions the API Gateway, Lambda function, and S3 bucket for the reference test pipeline.
4. WHEN the reference pipeline CloudFormation stack is deployed, THE Detection_Engine SHALL produce a Pipeline_Graph with at least three nodes (API Gateway, Lambda, S3) and two directed edges.

---

### Requirement 13: Web UI Pipeline Visualisation

**User Story:** As a data platform engineer, I want a web UI that visualises the pipeline graph and lets me click any node to see its full configuration, so that I can explore and understand my data pipelines without writing API calls.

#### Acceptance Criteria

1. THE UI SHALL render the Pipeline_Graph as an interactive node-link diagram where each node represents a pipeline component and each edge represents a data flow direction.
2. WHEN a node is clicked in the UI, THE UI SHALL call `GET /config/{urn}` and display the returned Node_Config JSON in a side panel or modal.
3. WHEN the `POST /analyze` response is received, THE UI SHALL update the graph to reflect the detected nodes, edges, and framework labels without requiring a page reload.
4. IF `GET /config/{urn}` returns an error, THEN THE UI SHALL display a human-readable error message in the node detail panel rather than a raw HTTP error.
5. THE UI SHALL display the Confidence_Score for each detection category in a summary section below the graph.

---

### Requirement 14: Docker and Deployment

**User Story:** As a developer, I want the entire system to run in Docker, so that I can reproduce the environment consistently across development, testing, and production.

#### Acceptance Criteria

1. THE API_Gateway SHALL be packaged in a Dockerfile that installs all dependencies from `requirements.txt` and starts the FastAPI application using `uvicorn`.
2. THE Detection_Engine SHALL list the following packages in `requirements.txt`: `acryl-datahub`, `fastapi`, `uvicorn`, `pydantic`, `requests`, `httpx`, `anthropic`, and `boto3`.
3. WHEN the Docker container is started, THE API_Gateway SHALL be accessible on a configurable port (default 8000) within 30 seconds.
4. THE Detection_Engine SHALL provide a `docker-compose` file that starts both the DataHub quickstart stack and the FastAPI container in a single command.
5. WHEN environment variables for cloud credentials are passed to the Docker container, THE API_Gateway SHALL load them via the application settings without requiring a container rebuild.

---

### Requirement 15: Health and Observability

**User Story:** As an operator, I want a health endpoint and structured logging, so that I can monitor the system's connectivity and diagnose issues quickly.

#### Acceptance Criteria

1. THE API_Gateway SHALL expose a `GET /health` endpoint that returns the DataHub connectivity status, LLM enabled flag, and application version.
2. WHEN the DataHub GMS is reachable, THE API_Gateway SHALL return `"datahub_connected": true` in the health response.
3. WHEN the DataHub GMS is unreachable, THE API_Gateway SHALL return `"datahub_connected": false` in the health response without returning an HTTP error status.
4. THE API_Gateway SHALL emit structured log lines at INFO level for every incoming request, including the endpoint path and response status code.
5. WHEN a Detector raises an exception during analysis, THE Detection_Engine SHALL log the detector name and exception message at ERROR level and continue executing remaining Detectors.
