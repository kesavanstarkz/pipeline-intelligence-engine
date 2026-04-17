"""
Unit tests for engine/pipeline_graph.py.

Covers:
- add_node / get_node
- add_edge
- get_upstream / get_downstream
- to_dict shape
- from_lineage with non-empty lineage
- deduplication behaviour
"""
import pytest

from engine.pipeline_graph import PipelineEdge, PipelineGraph, PipelineNode


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:glue,my_dataset,PROD)"
FLOW_URN = "urn:li:dataFlow:(glue,my_flow,PROD)"
JOB_URN = "urn:li:dataJob:(urn:li:dataFlow:(glue,my_flow,PROD),my_job)"
JOB_URN_2 = "urn:li:dataJob:(urn:li:dataFlow:(glue,my_flow,PROD),my_job_2)"


def make_node(urn: str, node_type: str = "dataset", platform: str = "glue") -> PipelineNode:
    return PipelineNode(
        urn=urn,
        node_type=node_type,
        platform=platform,
        environment=None,
        aspects={},
    )


def make_edge(src: str, tgt: str, edge_type: str = "DownstreamOf") -> PipelineEdge:
    return PipelineEdge(source_urn=src, target_urn=tgt, edge_type=edge_type)


# ---------------------------------------------------------------------------
# add_node / get_node
# ---------------------------------------------------------------------------

class TestAddGetNode:
    def test_add_and_retrieve_node(self):
        graph = PipelineGraph()
        node = make_node(DATASET_URN)
        graph.add_node(node)
        assert graph.get_node(DATASET_URN) is node

    def test_get_node_missing_returns_none(self):
        graph = PipelineGraph()
        assert graph.get_node("urn:li:dataset:(urn:li:dataPlatform:glue,missing,PROD)") is None

    def test_nodes_returns_all_added(self):
        graph = PipelineGraph()
        n1 = make_node(DATASET_URN)
        n2 = make_node(FLOW_URN, node_type="dataFlow")
        graph.add_node(n1)
        graph.add_node(n2)
        assert set(n.urn for n in graph.nodes()) == {DATASET_URN, FLOW_URN}

    def test_empty_graph_has_no_nodes(self):
        graph = PipelineGraph()
        assert graph.nodes() == []


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

class TestNodeDeduplication:
    def test_adding_same_urn_twice_keeps_one(self):
        graph = PipelineGraph()
        n1 = make_node(DATASET_URN, platform="glue")
        n2 = make_node(DATASET_URN, platform="redshift")  # different platform, same URN
        graph.add_node(n1)
        graph.add_node(n2)
        assert len(graph.nodes()) == 1

    def test_first_write_wins(self):
        """The first node added for a URN is the one that is kept."""
        graph = PipelineGraph()
        n1 = make_node(DATASET_URN, platform="glue")
        n2 = make_node(DATASET_URN, platform="redshift")
        graph.add_node(n1)
        graph.add_node(n2)
        assert graph.get_node(DATASET_URN).platform == "glue"

    def test_adding_three_duplicates_keeps_one(self):
        graph = PipelineGraph()
        for _ in range(3):
            graph.add_node(make_node(DATASET_URN))
        assert len(graph.nodes()) == 1

    def test_distinct_urns_all_kept(self):
        graph = PipelineGraph()
        urns = [DATASET_URN, FLOW_URN, JOB_URN]
        for urn in urns:
            graph.add_node(make_node(urn))
        assert len(graph.nodes()) == 3


# ---------------------------------------------------------------------------
# add_edge / edges
# ---------------------------------------------------------------------------

class TestAddEdge:
    def test_add_and_retrieve_edge(self):
        graph = PipelineGraph()
        edge = make_edge(DATASET_URN, JOB_URN)
        graph.add_edge(edge)
        assert len(graph.edges()) == 1
        assert graph.edges()[0] == edge

    def test_empty_graph_has_no_edges(self):
        graph = PipelineGraph()
        assert graph.edges() == []

    def test_duplicate_edge_deduplicated(self):
        graph = PipelineGraph()
        graph.add_edge(make_edge(DATASET_URN, JOB_URN))
        graph.add_edge(make_edge(DATASET_URN, JOB_URN))
        assert len(graph.edges()) == 1

    def test_different_edge_types_not_deduplicated(self):
        graph = PipelineGraph()
        graph.add_edge(make_edge(DATASET_URN, JOB_URN, "DownstreamOf"))
        graph.add_edge(make_edge(DATASET_URN, JOB_URN, "IsPartOf"))
        assert len(graph.edges()) == 2

    def test_multiple_distinct_edges(self):
        graph = PipelineGraph()
        graph.add_edge(make_edge(DATASET_URN, JOB_URN))
        graph.add_edge(make_edge(JOB_URN, FLOW_URN))
        assert len(graph.edges()) == 2


# ---------------------------------------------------------------------------
# get_upstream / get_downstream
# ---------------------------------------------------------------------------

class TestNeighbours:
    def _build_graph(self) -> PipelineGraph:
        """
        Build a simple graph:
          DATASET_URN --> JOB_URN --> FLOW_URN
        """
        graph = PipelineGraph()
        for urn in [DATASET_URN, JOB_URN, FLOW_URN]:
            graph.add_node(make_node(urn))
        graph.add_edge(make_edge(DATASET_URN, JOB_URN))
        graph.add_edge(make_edge(JOB_URN, FLOW_URN))
        return graph

    def test_get_downstream_of_source(self):
        graph = self._build_graph()
        downstream = graph.get_downstream(DATASET_URN)
        assert len(downstream) == 1
        assert downstream[0].urn == JOB_URN

    def test_get_upstream_of_target(self):
        graph = self._build_graph()
        upstream = graph.get_upstream(JOB_URN)
        assert len(upstream) == 1
        assert upstream[0].urn == DATASET_URN

    def test_get_upstream_of_source_is_empty(self):
        graph = self._build_graph()
        assert graph.get_upstream(DATASET_URN) == []

    def test_get_downstream_of_sink_is_empty(self):
        graph = self._build_graph()
        assert graph.get_downstream(FLOW_URN) == []

    def test_get_upstream_unknown_urn_is_empty(self):
        graph = self._build_graph()
        assert graph.get_upstream("urn:li:dataset:(urn:li:dataPlatform:glue,x,PROD)") == []

    def test_get_downstream_unknown_urn_is_empty(self):
        graph = self._build_graph()
        assert graph.get_downstream("urn:li:dataset:(urn:li:dataPlatform:glue,x,PROD)") == []

    def test_multiple_upstreams(self):
        """Two sources feeding into one job."""
        graph = PipelineGraph()
        src1 = "urn:li:dataset:(urn:li:dataPlatform:glue,src1,PROD)"
        src2 = "urn:li:dataset:(urn:li:dataPlatform:glue,src2,PROD)"
        for urn in [src1, src2, JOB_URN]:
            graph.add_node(make_node(urn))
        graph.add_edge(make_edge(src1, JOB_URN))
        graph.add_edge(make_edge(src2, JOB_URN))
        upstream_urns = {n.urn for n in graph.get_upstream(JOB_URN)}
        assert upstream_urns == {src1, src2}

    def test_multiple_downstreams(self):
        """One source feeding into two jobs."""
        graph = PipelineGraph()
        for urn in [DATASET_URN, JOB_URN, JOB_URN_2]:
            graph.add_node(make_node(urn))
        graph.add_edge(make_edge(DATASET_URN, JOB_URN))
        graph.add_edge(make_edge(DATASET_URN, JOB_URN_2))
        downstream_urns = {n.urn for n in graph.get_downstream(DATASET_URN)}
        assert downstream_urns == {JOB_URN, JOB_URN_2}


# ---------------------------------------------------------------------------
# to_dict
# ---------------------------------------------------------------------------

class TestToDict:
    def test_empty_graph_shape(self):
        graph = PipelineGraph()
        d = graph.to_dict()
        assert d == {"nodes": [], "edges": []}

    def test_nodes_list_shape(self):
        graph = PipelineGraph()
        graph.add_node(make_node(DATASET_URN))
        d = graph.to_dict()
        assert len(d["nodes"]) == 1
        node_dict = d["nodes"][0]
        assert "urn" in node_dict
        assert "node_type" in node_dict
        assert "platform" in node_dict
        assert node_dict["urn"] == DATASET_URN

    def test_edges_list_shape(self):
        graph = PipelineGraph()
        graph.add_node(make_node(DATASET_URN))
        graph.add_node(make_node(JOB_URN))
        graph.add_edge(make_edge(DATASET_URN, JOB_URN))
        d = graph.to_dict()
        assert len(d["edges"]) == 1
        edge_dict = d["edges"][0]
        assert "source" in edge_dict
        assert "target" in edge_dict
        assert "edge_type" in edge_dict
        assert edge_dict["source"] == DATASET_URN
        assert edge_dict["target"] == JOB_URN

    def test_counts_match_graph(self):
        graph = PipelineGraph()
        for urn in [DATASET_URN, JOB_URN, FLOW_URN]:
            graph.add_node(make_node(urn))
        graph.add_edge(make_edge(DATASET_URN, JOB_URN))
        graph.add_edge(make_edge(JOB_URN, FLOW_URN))
        d = graph.to_dict()
        assert len(d["nodes"]) == 3
        assert len(d["edges"]) == 2


# ---------------------------------------------------------------------------
# from_lineage
# ---------------------------------------------------------------------------

class TestFromLineage:
    def test_empty_lineage_produces_seed_only(self):
        graph = PipelineGraph.from_lineage(DATASET_URN, [])
        assert len(graph.nodes()) == 1
        assert len(graph.edges()) == 0
        assert graph.get_node(DATASET_URN) is not None

    def test_seed_node_type_inferred_dataset(self):
        graph = PipelineGraph.from_lineage(DATASET_URN, [])
        assert graph.get_node(DATASET_URN).node_type == "dataset"

    def test_seed_node_type_inferred_flow(self):
        graph = PipelineGraph.from_lineage(FLOW_URN, [])
        assert graph.get_node(FLOW_URN).node_type == "dataFlow"

    def test_seed_node_type_inferred_job(self):
        graph = PipelineGraph.from_lineage(JOB_URN, [])
        assert graph.get_node(JOB_URN).node_type == "dataJob"

    def test_non_empty_lineage_adds_nodes_and_edges(self):
        lineage = [
            {"entity": {"urn": DATASET_URN}, "type": "DownstreamOf"},
        ]
        graph = PipelineGraph.from_lineage(JOB_URN, lineage)
        assert len(graph.nodes()) == 2
        assert len(graph.edges()) == 1
        assert graph.get_node(DATASET_URN) is not None
        assert graph.get_node(JOB_URN) is not None

    def test_lineage_edge_direction(self):
        """
        The lineage item represents: related_urn is upstream of seed_urn.
        So the edge should be: related_urn --> seed_urn.
        """
        lineage = [
            {"entity": {"urn": DATASET_URN}, "type": "DownstreamOf"},
        ]
        graph = PipelineGraph.from_lineage(JOB_URN, lineage)
        # DATASET_URN is upstream of JOB_URN
        upstream = graph.get_upstream(JOB_URN)
        assert len(upstream) == 1
        assert upstream[0].urn == DATASET_URN

    def test_multiple_lineage_items(self):
        src1 = "urn:li:dataset:(urn:li:dataPlatform:glue,src1,PROD)"
        src2 = "urn:li:dataset:(urn:li:dataPlatform:glue,src2,PROD)"
        lineage = [
            {"entity": {"urn": src1}, "type": "DownstreamOf"},
            {"entity": {"urn": src2}, "type": "DownstreamOf"},
        ]
        graph = PipelineGraph.from_lineage(JOB_URN, lineage)
        assert len(graph.nodes()) == 3
        assert len(graph.edges()) == 2

    def test_lineage_with_duplicate_urns_deduplicates(self):
        """If the same URN appears twice in lineage, it should only be added once."""
        lineage = [
            {"entity": {"urn": DATASET_URN}, "type": "DownstreamOf"},
            {"entity": {"urn": DATASET_URN}, "type": "DownstreamOf"},
        ]
        graph = PipelineGraph.from_lineage(JOB_URN, lineage)
        assert len(graph.nodes()) == 2  # seed + one unique related node
        assert len(graph.edges()) == 1  # deduplicated edge

    def test_lineage_item_missing_entity_urn_skipped(self):
        """Items with missing or empty entity URN should be skipped gracefully."""
        lineage = [
            {"entity": {}, "type": "DownstreamOf"},
            {"entity": {"urn": ""}, "type": "DownstreamOf"},
        ]
        graph = PipelineGraph.from_lineage(DATASET_URN, lineage)
        assert len(graph.nodes()) == 1
        assert len(graph.edges()) == 0

    def test_seed_platform_inferred(self):
        graph = PipelineGraph.from_lineage(DATASET_URN, [])
        node = graph.get_node(DATASET_URN)
        assert node.platform == "glue"
