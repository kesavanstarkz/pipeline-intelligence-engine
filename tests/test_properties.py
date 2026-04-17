"""
Property-based tests for the Pipeline Intelligence Engine.

Each test implements one correctness property from the design document.
Uses hypothesis with @settings(max_examples=20) for fast CI runs.

Tag format: # Feature: pipeline-intelligence-engine, Property N: <property_text>
"""
from hypothesis import given, settings, strategies as st

from engine.urn_parser import parse_arn, parse_urn


# ---------------------------------------------------------------------------
# Property 3: URN Round-Trip
# Validates: Requirements 3.1, 3.7
# ---------------------------------------------------------------------------

# Generators for valid URN component parts (alphanumeric + underscore/hyphen)
_urn_part = st.from_regex(r"[a-z][a-z0-9_]{0,19}", fullmatch=True)
_env = st.sampled_from(["PROD", "DEV", "STAGING", "TEST"])


@st.composite
def data_job_urns(draw):
    platform = draw(_urn_part)
    flow_id = draw(_urn_part)
    env = draw(_env)
    job_id = draw(_urn_part)
    return f"urn:li:dataJob:(urn:li:dataFlow:({platform},{flow_id},{env}),{job_id})"


@st.composite
def data_flow_urns(draw):
    platform = draw(_urn_part)
    flow_id = draw(_urn_part)
    env = draw(_env)
    return f"urn:li:dataFlow:({platform},{flow_id},{env})"


@st.composite
def dataset_urns(draw):
    platform = draw(_urn_part)
    name = draw(_urn_part)
    env = draw(_env)
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},{env})"


@settings(max_examples=20)
@given(urn=st.one_of(data_job_urns(), data_flow_urns(), dataset_urns()))
def test_property_3_urn_round_trip(urn):
    # Feature: pipeline-intelligence-engine, Property 3: URN round-trip
    # Validates: Requirements 3.1, 3.7
    parsed = parse_urn(urn)
    assert parsed is not None, f"parse_urn returned None for valid URN: {urn}"

    reconstructed = parsed.to_urn()

    # Re-parse the reconstructed URN and verify all components are preserved
    reparsed = parse_urn(reconstructed)
    assert reparsed is not None, (
        f"to_urn() produced an unparseable URN: {reconstructed!r} (original: {urn!r})"
    )
    assert reparsed.platform == parsed.platform, (
        f"platform mismatch: {reparsed.platform!r} != {parsed.platform!r}"
    )
    assert reparsed.environment == parsed.environment, (
        f"environment mismatch: {reparsed.environment!r} != {parsed.environment!r}"
    )
    assert reparsed.flow_id == parsed.flow_id, (
        f"flow_id mismatch: {reparsed.flow_id!r} != {parsed.flow_id!r}"
    )
    assert reparsed.job_id == parsed.job_id, (
        f"job_id mismatch: {reparsed.job_id!r} != {parsed.job_id!r}"
    )


# ---------------------------------------------------------------------------
# Property 4: ARN Component Extraction
# Validates: Requirements 3.2, 11.1
# ---------------------------------------------------------------------------

_aws_service = st.sampled_from([
    "glue", "lambda", "s3", "elasticmapreduce", "redshift",
    "ec2", "ecs", "rds", "sagemaker", "kinesis",
])
_region = st.sampled_from([
    "us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1", "ca-central-1",
])
_account = st.from_regex(r"[0-9]{12}", fullmatch=True)
_resource = st.from_regex(r"[a-z][a-z0-9/_-]{1,39}", fullmatch=True)


@st.composite
def standard_arns(draw):
    """Generate standard AWS ARNs with non-empty region and account."""
    service = draw(_aws_service)
    region = draw(_region)
    account = draw(_account)
    resource = draw(_resource)
    return f"arn:aws:{service}:{region}:{account}:{resource}"


@settings(max_examples=20)
@given(arn=standard_arns())
def test_property_4_arn_component_extraction(arn):
    # Feature: pipeline-intelligence-engine, Property 4: ARN component extraction
    # Validates: Requirements 3.2, 11.1
    result = parse_arn(arn)
    assert result is not None, f"parse_arn returned None for valid ARN: {arn}"

    assert isinstance(result.service, str) and result.service, (
        f"service is empty for ARN: {arn}"
    )
    assert isinstance(result.region, str) and result.region, (
        f"region is empty for ARN: {arn}"
    )
    assert isinstance(result.account_id, str) and result.account_id, (
        f"account_id is empty for ARN: {arn}"
    )
    assert isinstance(result.resource_id, str) and result.resource_id, (
        f"resource_id is empty for ARN: {arn}"
    )


# ---------------------------------------------------------------------------
# Property 8: Pipeline Graph Node Deduplication
# Validates: Requirements 4.3
# ---------------------------------------------------------------------------

from engine.pipeline_graph import PipelineGraph, PipelineNode, PipelineEdge


@st.composite
def pipeline_node_urns(draw):
    """Generate valid DataHub URNs for use as pipeline node URNs."""
    return draw(st.one_of(data_job_urns(), data_flow_urns(), dataset_urns()))


@st.composite
def node_lists_with_duplicates(draw):
    """
    Generate a list of (urn, node) pairs where some URNs may be repeated.
    Returns a list of PipelineNode objects with possible duplicate URNs.
    """
    # Pick 1–5 distinct URNs
    distinct_urns = draw(
        st.lists(pipeline_node_urns(), min_size=1, max_size=5, unique=True)
    )
    # For each distinct URN, decide how many times to add it (1–3)
    nodes = []
    for urn in distinct_urns:
        count = draw(st.integers(min_value=1, max_value=3))
        for _ in range(count):
            nodes.append(
                PipelineNode(
                    urn=urn,
                    node_type="dataset",
                    platform="glue",
                    environment=None,
                    aspects={},
                )
            )
    # Shuffle so duplicates are not necessarily adjacent
    draw(st.permutations(nodes))
    return nodes, distinct_urns


@settings(max_examples=20)
@given(data=node_lists_with_duplicates())
def test_property_8_node_deduplication(data):
    # Feature: pipeline-intelligence-engine, Property 8: Pipeline Graph Node Deduplication
    # Validates: Requirements 4.3
    nodes, distinct_urns = data
    graph = PipelineGraph()
    for node in nodes:
        graph.add_node(node)

    result_urns = [n.urn for n in graph.nodes()]
    # Each URN must appear exactly once
    assert len(result_urns) == len(distinct_urns), (
        f"Expected {len(distinct_urns)} unique nodes, got {len(result_urns)}: {result_urns}"
    )
    assert set(result_urns) == set(distinct_urns), (
        f"URN set mismatch: {set(result_urns)} != {set(distinct_urns)}"
    )


# ---------------------------------------------------------------------------
# Property 9: Pipeline Graph Serialisation Round-Trip
# Validates: Requirements 4.4
# ---------------------------------------------------------------------------

@st.composite
def pipeline_graphs(draw):
    """Generate a PipelineGraph with random nodes and edges."""
    urns = draw(
        st.lists(pipeline_node_urns(), min_size=0, max_size=6, unique=True)
    )
    graph = PipelineGraph()
    for urn in urns:
        graph.add_node(PipelineNode(
            urn=urn,
            node_type="dataset",
            platform="glue",
            environment=None,
            aspects={},
        ))

    # Add some edges between existing nodes
    if len(urns) >= 2:
        num_edges = draw(st.integers(min_value=0, max_value=min(len(urns) * 2, 8)))
        for _ in range(num_edges):
            src = draw(st.sampled_from(urns))
            tgt = draw(st.sampled_from(urns))
            if src != tgt:
                graph.add_edge(PipelineEdge(
                    source_urn=src,
                    target_urn=tgt,
                    edge_type="DownstreamOf",
                ))

    return graph


@settings(max_examples=20)
@given(graph=pipeline_graphs())
def test_property_9_serialisation_round_trip(graph):
    # Feature: pipeline-intelligence-engine, Property 9: Pipeline Graph Serialisation Round-Trip
    # Validates: Requirements 4.4
    d = graph.to_dict()

    assert "nodes" in d, "to_dict() must contain 'nodes' key"
    assert "edges" in d, "to_dict() must contain 'edges' key"
    assert isinstance(d["nodes"], list), "'nodes' must be a list"
    assert isinstance(d["edges"], list), "'edges' must be a list"

    assert len(d["nodes"]) == len(graph.nodes()), (
        f"nodes count mismatch: dict has {len(d['nodes'])}, graph has {len(graph.nodes())}"
    )
    assert len(d["edges"]) == len(graph.edges()), (
        f"edges count mismatch: dict has {len(d['edges'])}, graph has {len(graph.edges())}"
    )


# ---------------------------------------------------------------------------
# Property 10: Empty Lineage Produces Seed-Only Graph
# Validates: Requirements 4.5
# ---------------------------------------------------------------------------

@settings(max_examples=20)
@given(seed_urn=st.one_of(data_job_urns(), data_flow_urns(), dataset_urns()))
def test_property_10_empty_lineage_seed_only(seed_urn):
    # Feature: pipeline-intelligence-engine, Property 10: Empty Lineage Produces Seed-Only Graph
    # Validates: Requirements 4.5
    graph = PipelineGraph.from_lineage(seed_urn, [])

    assert len(graph.nodes()) == 1, (
        f"Expected exactly 1 node for empty lineage, got {len(graph.nodes())}"
    )
    assert len(graph.edges()) == 0, (
        f"Expected 0 edges for empty lineage, got {len(graph.edges())}"
    )
    assert graph.nodes()[0].urn == seed_urn, (
        f"Seed node URN mismatch: {graph.nodes()[0].urn!r} != {seed_urn!r}"
    )


# ---------------------------------------------------------------------------
# Property 11: Node Config Omits Absent Aspects
# Validates: Requirements 5.3
# ---------------------------------------------------------------------------

from unittest.mock import MagicMock, patch

from engine.datahub_client import DataHubClient

_ALL_ASPECTS = [
    "datasetProperties",
    "dataFlowInfo",
    "dataJobInfo",
    "ownership",
    "upstreamLineage",
]


@st.composite
def aspect_presence_maps(draw):
    """
    Generate a dict mapping each aspect name to either a non-None dict or None.
    At least one aspect may be None to exercise the omission logic.
    """
    presence = {}
    for aspect in _ALL_ASPECTS:
        is_present = draw(st.booleans())
        if is_present:
            presence[aspect] = draw(
                st.fixed_dictionaries({"value": st.text(min_size=1, max_size=20)})
            )
        else:
            presence[aspect] = None
    return presence


@settings(max_examples=20)
@given(presence=aspect_presence_maps())
def test_property_11_node_config_omits_absent_aspects(presence):
    # Feature: pipeline-intelligence-engine, Property 11: Node Config Omits Absent Aspects
    # Validates: Requirements 5.3
    client = DataHubClient.__new__(DataHubClient)

    def mock_get_aspect(urn: str, aspect: str):
        return presence.get(aspect)

    client.get_aspect = mock_get_aspect  # type: ignore[method-assign]

    result = client.get_node_config("urn:li:dataset:(urn:li:dataPlatform:glue,test,PROD)")

    # No None values in result
    for key, val in result.items():
        assert val is not None, (
            f"Key {key!r} has None value — get_node_config must omit absent aspects"
        )

    # Every present aspect must appear in result
    expected_keys = {asp for asp, val in presence.items() if val is not None}
    assert set(result.keys()) == expected_keys, (
        f"Result keys {set(result.keys())} != expected {expected_keys}"
    )
