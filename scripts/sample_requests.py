"""
Sample Request/Response Showcase
──────────────────────────────────
Run the API first:
    uvicorn api.main:app --reload --port 8000

Then execute:
    python scripts/sample_requests.py
"""
import json
import sys
import httpx

BASE_URL = "http://localhost:8000"


def post_analyze(label: str, payload: dict) -> dict:
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    print("REQUEST:")
    print(json.dumps(payload, indent=2))

    try:
        resp = httpx.post(f"{BASE_URL}/analyze", json=payload, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        print("\nRESPONSE:")
        # Pretty print — omit evidence for brevity
        display = {k: v for k, v in data.items() if k != "evidence"}
        print(json.dumps(display, indent=2))
        return data
    except httpx.ConnectError:
        print("ERROR: API not running. Start with: uvicorn api.main:app --reload")
        sys.exit(1)


if __name__ == "__main__":

    # ── 1. AWS Glue + Redshift ────────────────────────────────────────────
    post_analyze(
        "Scenario 1: AWS Glue → Redshift Pipeline",
        {
            "metadata": {"platform": "glue", "name": "customer_etl"},
            "config": {
                "input":  "s3://raw-data-bucket/customers/",
                "output": "jdbc:redshift://prod-cluster.region.redshift.amazonaws.com:5439/analytics"
            },
            "raw_json": {
                "type": "GlueJob",
                "schedule": "cron(0 2 * * ? *)",
                "dq": {
                    "suite_name": "customer_suite",
                    "expectations": [
                        {"expectation_type": "expect_column_values_to_not_be_null",
                         "kwargs": {"column": "customer_id"}},
                        {"expectation_type": "expect_column_values_to_be_unique",
                         "kwargs": {"column": "email"}},
                    ]
                }
            }
        }
    )

    # ── 2. ADF + Databricks + ADLS ──────────────────────────────────────
    post_analyze(
        "Scenario 2: ADF → Databricks (ADLS source)",
        {
            "metadata": {"platform": "adf", "region": "eastus"},
            "config": {
                "source":      "abfss://raw@datalake.dfs.core.windows.net/events/",
                "destination": "databricks_cluster",
            },
            "raw_json": {
                "type":       "adf_pipeline",
                "activities": [{"type": "DatabricksNotebook", "notebook": "/ETL/transform"}],
                "monitor":    "monte_carlo",
            }
        }
    )

    # ── 3. Snowflake standalone + Airflow + Soda ─────────────────────────
    post_analyze(
        "Scenario 3: Snowflake Standalone + Airflow + Soda DQ",
        {
            "metadata": {
                "snowflake_account": "xy12345.us-east-1",
                "warehouse":         "COMPUTE_WH",
                "dag_id":            "nightly_snowflake_load",
            },
            "config": {
                "source":      "s3a://analytics-bucket/events/dt=2024-01-01/",
                "destination": "snowflake://prod_db/analytics/events"
            },
            "raw_json": {
                "dq_tool": "soda",
                "checks":  [
                    {"type": "row_count", "min": 1000},
                    {"type": "no_nulls",  "column": "event_id"},
                ]
            }
        }
    )

    # ── 4. Spark + Kafka + Custom DQ ────────────────────────────────────
    post_analyze(
        "Scenario 4: Spark Streaming + Kafka + Custom DQ",
        {
            "metadata": {"runner": "pyspark", "mode": "streaming"},
            "config": {
                "bootstrap_servers": "kafka-broker-1:9092,kafka-broker-2:9092",
                "topic":             "user-events",
                "checkpoint":        "s3://checkpoints/user-events/",
            },
            "raw_json": {
                "job_class":  "com.company.streaming.UserEventProcessor",
                "dq_class":   "CustomDQCheck",
                "validation": "SELECT COUNT(*) > 0 FROM user_events WHERE event_id IS NULL"
            }
        }
    )

    # ── 5. Unknown / dynamic pipeline ──────────────────────────────────
    post_analyze(
        "Scenario 5: Unknown Pipeline (partial metadata only)",
        {
            "metadata": {"name": "mystery_etl", "owner": "team_x"},
            "config":   {"connection": "jdbc:postgresql://analytics-db:5432/warehouse"},
        }
    )

    print(f"\n{'='*60}")
    print("  All samples complete.")
    print(f"{'='*60}\n")
