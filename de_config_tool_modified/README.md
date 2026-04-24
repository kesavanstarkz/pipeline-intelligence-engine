# DE Config Management Tool

> **Dynamic framework config extraction** for any data pipeline.
> AWS · Azure · Databricks · Snowflake · AI-powered inference for unknown frameworks.

---

## Quick Start

```bash
# 1. Install core dependencies
pip install fastapi "uvicorn[standard]" python-multipart jinja2 httpx pydantic boto3

# 2. Set API key for AI inference (optional — falls back to pattern matching)
export ANTHROPIC_API_KEY=sk-ant-api03-...

# 3. Run
uvicorn main:app --reload --port 8000
# Open http://localhost:8000
```

Or with Docker:
```bash
cp .env.example .env   # add your keys
docker-compose up --build
```

---

## Architecture

```
Frontend (index.html)
    │
    ├── Source Config Generator    (form → structured JSON)
    ├── Deep Diff                  (compare two configs)
    ├── Reconcile + Export         (merge strategies)
    └── Multi-Platform Extractor   (NEW — all platforms below)
              │
FastAPI Backend
    │
    ├── /api/extract/*             Universal extraction router
    ├── /api/lambda/*              AWS Lambda-specific extractor
    ├── /api/config/*              Config CRUD
    ├── /api/diff/*                Config comparison
    └── /api/browse/*              Cloud storage browsers
              │
Services Layer
    ├── pipeline_orchestrator.py   Routes to correct extractor
    ├── lambda_extractor.py        AWS Lambda / Glue / Step Fn / EMR
    ├── azure_extractor.py         ADF / Synapse / ADLS Gen2
    ├── databricks_extractor.py    Jobs / DLT / Notebooks / Delta
    ├── snowflake_extractor.py     Tasks / Streams / dbt / Dyn Tables
    └── llm_inference.py           Claude AI — last fallback
```

---

## Unified Output Schema

Every platform returns the same shape:

```json
{
  "pipeline_name": "crm-ingestor-prod",
  "platform": "AWS",
  "framework": "API Ingestion Framework",
  "source_config": {
    "type": "rest_api",
    "connection": { "base_url": "https://api.crm.com/v2", "auth_type": "bearer_token" },
    "extraction_mode": "incremental",
    "watermark_column": "updated_at"
  },
  "ingestion_config": {
    "pipeline_type": "lambda",
    "batch_size": 500,
    "schedule": "0 */6 * * *",
    "output": { "bucket": "data-lake-prod", "format": "parquet" }
  },
  "dq_config": {
    "framework": "custom",
    "rules": [
      { "column": "id", "rule_type": "not_null", "severity": "critical", "action": "reject" }
    ],
    "row_count_min": 100,
    "schema_drift_action": "warn"
  },
  "raw_metadata": { ... }
}
```

---

## Supported Platforms

| Platform | Frameworks | Demo Pipelines |
|----------|-----------|----------------|
| ☁️ AWS | Lambda · Glue · Step Functions · EMR | 6 |
| 🔷 Azure | ADF · Synapse · ADLS Gen2 | 3 |
| 🧱 Databricks | DLT · Spark Jobs · Notebooks | 3 |
| ❄️ Snowflake | Tasks · Streams · dbt · Dynamic Tables | 3 |
| 🤖 AI Infer | Any unknown/custom pipeline | 5 code examples |

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/extract/config` | Extract from any pipeline |
| POST | `/api/extract/infer` | Claude AI inference from code |
| POST | `/api/extract/classify` | Classify framework from snippet |
| GET | `/api/extract/platforms` | List platforms + demo pipelines |
| GET | `/api/extract/health` | Dependency health check |
| GET | `/api/extract/llm-status` | LLM availability status |

### Extract any pipeline (cURL)

```bash
curl -X POST http://localhost:8000/api/extract/config \
  -H "Content-Type: application/json" \
  -d '{"pipeline_name": "dlt-bronze-silver", "platform": "databricks", "demo_mode": true}'
```

### AI inference from code

```bash
curl -X POST http://localhost:8000/api/extract/infer \
  -H "Content-Type: application/json" \
  -d '{
    "code_or_config": "def handler(event, ctx):\n    conn = psycopg2.connect(host=os.environ[\"DB_HOST\"])\n    ...",
    "pipeline_name": "my-pipeline"
  }'
```

---

## Extraction Strategy (Layered)

```
1. Direct config    → ENV vars · S3/ADLS JSON files · DB config tables
2. Service metadata → Lambda API · ADF SDK · Databricks SDK · SHOW TASKS
3. Code parsing     → Regex patterns · AST analysis · SQL parsing
4. LLM inference    → Claude AI: code → structured JSON (last resort)
```

---

## Environment Variables

```bash
# AI inference (falls back to pattern matching if not set)
ANTHROPIC_API_KEY=sk-ant-api03-...

# AWS (or use IAM role / ~/.aws/credentials)
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=us-east-1

# Azure
AZURE_TENANT_ID=...
AZURE_CLIENT_ID=...
AZURE_CLIENT_SECRET=...

# Databricks
DATABRICKS_HOST=https://adb-xxx.azuredatabricks.net
DATABRICKS_TOKEN=dapi...

# Snowflake
SNOWFLAKE_ACCOUNT=myorg-myaccount
SNOWFLAKE_USER=etl_user
SNOWFLAKE_PASSWORD=...
```

---

## Project Structure

```
de_config_tool/
├── main.py
├── requirements.txt
├── .env.example
├── Dockerfile
├── docker-compose.yml
├── routers/
│   ├── extract.py              ← NEW: universal extractor
│   ├── lambda_extract.py
│   ├── config.py / diff.py / export.py / browse.py
├── services/
│   ├── pipeline_orchestrator.py  ← NEW: platform router
│   ├── azure_extractor.py        ← NEW
│   ├── databricks_extractor.py   ← NEW
│   ├── snowflake_extractor.py    ← NEW
│   ├── llm_inference.py          ← NEW: Claude AI
│   ├── health.py                 ← NEW
│   ├── lambda_extractor.py       (existing)
│   ├── etl_config_manager.py     (existing)
│   ├── deep_diff.py              (existing)
│   └── file_browsers/            (existing)
└── templates/
    └── index.html                ← Extended with Multi-Platform UI
```

---

## Adding a New Platform

1. Create `services/my_platform_extractor.py` — add mock templates + real extractor class
2. Register in `services/pipeline_orchestrator.py` — add `detect_platform()` patterns + `_extract_myplatform()` method
3. Import + expose in `routers/extract.py` — add to `/api/extract/platforms`
4. Add UI panel in `templates/index.html` — copy any `mp-panel-*` block and update IDs

---

## Enterprise Scaling

| Concern | Recommendation |
|---------|---------------|
| Auth | OAuth2/OIDC middleware (Okta, Azure AD) |
| Storage | PostgreSQL for extracted configs with versioning |
| Caching | Redis (TTL 5 min) for repeated pipeline lookups |
| Async | Celery + SQS for long-running extractions |
| Multi-tenant | Workspace isolation with row-level security |
| CI/CD | GitHub Actions → Docker Hub → ECS/AKS |
| Monitoring | Prometheus + Grafana |
