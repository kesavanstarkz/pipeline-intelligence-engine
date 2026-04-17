"""
Unit tests for engine/urn_parser.py

Covers:
- Glue dataJob URN parsing
- dataFlow URN parsing
- dataset URN parsing
- Lambda ARN parsing
- S3 ARN (no region/account) parsing
- Malformed inputs returning None
- Round-trip re-serialisation (to_urn)
"""
import pytest

from engine.urn_parser import ParsedARN, ParsedURN, parse_arn, parse_urn


# ---------------------------------------------------------------------------
# parse_urn — dataJob
# ---------------------------------------------------------------------------

class TestParseUrnDataJob:
    def test_glue_data_job_urn(self):
        urn = "urn:li:dataJob:(urn:li:dataFlow:(glue,my_flow,PROD),my_job)"
        result = parse_urn(urn)
        assert result is not None
        assert result.platform == "glue"
        assert result.flow_id == "my_flow"
        assert result.environment == "PROD"
        assert result.job_id == "my_job"
        assert result.raw == urn

    def test_data_job_urn_with_underscores(self):
        urn = "urn:li:dataJob:(urn:li:dataFlow:(glue,etl_pipeline,DEV),transform_step)"
        result = parse_urn(urn)
        assert result is not None
        assert result.platform == "glue"
        assert result.flow_id == "etl_pipeline"
        assert result.environment == "DEV"
        assert result.job_id == "transform_step"

    def test_data_job_urn_different_platform(self):
        urn = "urn:li:dataJob:(urn:li:dataFlow:(spark,my_flow,STAGING),my_job)"
        result = parse_urn(urn)
        assert result is not None
        assert result.platform == "spark"
        assert result.environment == "STAGING"


# ---------------------------------------------------------------------------
# parse_urn — dataFlow
# ---------------------------------------------------------------------------

class TestParseUrnDataFlow:
    def test_basic_data_flow_urn(self):
        urn = "urn:li:dataFlow:(glue,my_flow,PROD)"
        result = parse_urn(urn)
        assert result is not None
        assert result.platform == "glue"
        assert result.flow_id == "my_flow"
        assert result.environment == "PROD"
        assert result.job_id is None
        assert result.raw == urn

    def test_data_flow_urn_dev_env(self):
        urn = "urn:li:dataFlow:(databricks,pipeline_abc,DEV)"
        result = parse_urn(urn)
        assert result is not None
        assert result.platform == "databricks"
        assert result.flow_id == "pipeline_abc"
        assert result.environment == "DEV"
        assert result.job_id is None


# ---------------------------------------------------------------------------
# parse_urn — dataset
# ---------------------------------------------------------------------------

class TestParseUrnDataset:
    def test_basic_dataset_urn(self):
        urn = "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket,PROD)"
        result = parse_urn(urn)
        assert result is not None
        assert result.platform == "s3"
        assert result.flow_id == "my-bucket"   # name stored in flow_id
        assert result.environment == "PROD"
        assert result.job_id is None
        assert result.raw == urn

    def test_redshift_dataset_urn(self):
        urn = "urn:li:dataset:(urn:li:dataPlatform:redshift,mydb.public.orders,PROD)"
        result = parse_urn(urn)
        assert result is not None
        assert result.platform == "redshift"
        assert result.flow_id == "mydb.public.orders"
        assert result.environment == "PROD"


# ---------------------------------------------------------------------------
# parse_urn — round-trip (to_urn)
# ---------------------------------------------------------------------------

class TestToUrn:
    def test_data_job_round_trip(self):
        urn = "urn:li:dataJob:(urn:li:dataFlow:(glue,my_flow,PROD),my_job)"
        parsed = parse_urn(urn)
        assert parsed is not None
        assert parsed.to_urn() == urn

    def test_data_flow_round_trip(self):
        urn = "urn:li:dataFlow:(glue,my_flow,PROD)"
        parsed = parse_urn(urn)
        assert parsed is not None
        assert parsed.to_urn() == urn

    def test_dataset_round_trip(self):
        urn = "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket,PROD)"
        parsed = parse_urn(urn)
        assert parsed is not None
        assert parsed.to_urn() == urn

    def test_data_job_components_preserved(self):
        urn = "urn:li:dataJob:(urn:li:dataFlow:(glue,etl_pipeline,DEV),transform_step)"
        parsed = parse_urn(urn)
        assert parsed is not None
        reconstructed = parsed.to_urn()
        reparsed = parse_urn(reconstructed)
        assert reparsed is not None
        assert reparsed.platform == parsed.platform
        assert reparsed.environment == parsed.environment
        assert reparsed.flow_id == parsed.flow_id
        assert reparsed.job_id == parsed.job_id


# ---------------------------------------------------------------------------
# parse_urn — malformed inputs
# ---------------------------------------------------------------------------

class TestParseUrnMalformed:
    def test_empty_string_returns_none(self):
        assert parse_urn("") is None

    def test_none_input_returns_none(self):
        assert parse_urn(None) is None  # type: ignore[arg-type]

    def test_random_string_returns_none(self):
        assert parse_urn("not-a-urn") is None

    def test_partial_urn_returns_none(self):
        assert parse_urn("urn:li:dataJob:") is None

    def test_missing_closing_paren_returns_none(self):
        assert parse_urn("urn:li:dataFlow:(glue,my_flow,PROD") is None

    def test_wrong_prefix_returns_none(self):
        assert parse_urn("urn:li:unknown:(glue,my_flow,PROD)") is None

    def test_arn_string_returns_none(self):
        assert parse_urn("arn:aws:glue:us-east-1:123456789012:job/my_job") is None


# ---------------------------------------------------------------------------
# parse_arn — standard ARNs
# ---------------------------------------------------------------------------

class TestParseArn:
    def test_glue_job_arn(self):
        arn = "arn:aws:glue:us-east-1:123456789012:job/my_job"
        result = parse_arn(arn)
        assert result is not None
        assert result.service == "glue"
        assert result.region == "us-east-1"
        assert result.account_id == "123456789012"
        assert result.resource_id == "job/my_job"
        assert result.raw == arn

    def test_lambda_function_arn(self):
        arn = "arn:aws:lambda:us-east-1:123456789012:function:MyFunction"
        result = parse_arn(arn)
        assert result is not None
        assert result.service == "lambda"
        assert result.region == "us-east-1"
        assert result.account_id == "123456789012"
        assert result.resource_id == "function:MyFunction"

    def test_emr_cluster_arn(self):
        arn = "arn:aws:elasticmapreduce:us-east-1:123456789012:cluster/j-XXXXXXXX"
        result = parse_arn(arn)
        assert result is not None
        assert result.service == "elasticmapreduce"
        assert result.region == "us-east-1"
        assert result.account_id == "123456789012"
        assert result.resource_id == "cluster/j-XXXXXXXX"

    def test_s3_arn_no_region_no_account(self):
        arn = "arn:aws:s3:::my-bucket"
        result = parse_arn(arn)
        assert result is not None
        assert result.service == "s3"
        assert result.region == ""
        assert result.account_id == ""
        assert result.resource_id == "my-bucket"

    def test_redshift_arn(self):
        arn = "arn:aws:redshift:us-west-2:123456789012:cluster:my-cluster"
        result = parse_arn(arn)
        assert result is not None
        assert result.service == "redshift"
        assert result.region == "us-west-2"
        assert result.account_id == "123456789012"
        assert result.resource_id == "cluster:my-cluster"


# ---------------------------------------------------------------------------
# parse_arn — malformed inputs
# ---------------------------------------------------------------------------

class TestParseArnMalformed:
    def test_empty_string_returns_none(self):
        assert parse_arn("") is None

    def test_none_input_returns_none(self):
        assert parse_arn(None) is None  # type: ignore[arg-type]

    def test_wrong_prefix_returns_none(self):
        assert parse_arn("aws:glue:us-east-1:123456789012:job/my_job") is None

    def test_too_few_parts_returns_none(self):
        assert parse_arn("arn:aws:glue:us-east-1") is None

    def test_random_string_returns_none(self):
        assert parse_arn("not-an-arn") is None

    def test_urn_string_returns_none(self):
        assert parse_arn("urn:li:dataFlow:(glue,my_flow,PROD)") is None
