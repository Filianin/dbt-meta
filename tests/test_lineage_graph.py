"""Tests for LineageGraph (rustworkx-backed column dependency graph)."""

import pytest

from dbt_meta.lineage.graph import LineageGraph, make_node_id, split_node_id


class TestNodeIdHelpers:
    def test_make_node_id_joins_with_dot(self):
        assert make_node_id("core_clients", "client_id") == "core_clients.client_id"

    def test_split_returns_tuple(self):
        assert split_node_id("core_clients.client_id") == ("core_clients", "client_id")

    def test_split_uses_last_separator(self):
        # Models with dots in their name (e.g. dataset-qualified) split correctly
        assert split_node_id("schema.table.col") == ("schema.table", "col")

    def test_split_invalid_raises(self):
        with pytest.raises(ValueError):
            split_node_id("no_separator_here")


class TestLineageGraphMutation:
    def test_add_node_returns_index(self):
        g = LineageGraph()
        idx = g.add_node("a.x", {"data_type": "INT64"})
        assert isinstance(idx, int)
        assert g.node_count == 1

    def test_add_node_idempotent(self):
        g = LineageGraph()
        idx1 = g.add_node("a.x", {"data_type": "INT64"})
        idx2 = g.add_node("a.x", {"extra": "k"})
        assert idx1 == idx2
        # attrs merged
        node = g.get_node("a.x")
        assert node["data_type"] == "INT64"
        assert node["extra"] == "k"

    def test_add_edge_creates_missing_nodes(self):
        g = LineageGraph()
        g.add_edge("a.x", "b.y")
        assert g.node_count == 2
        assert g.edge_count == 1

    def test_add_edge_dedupes(self):
        g = LineageGraph()
        g.add_edge("a.x", "b.y")
        g.add_edge("a.x", "b.y")
        assert g.edge_count == 1


class TestLineageGraphQueries:
    @pytest.fixture
    def chain_graph(self):
        # raw -> stg -> core
        g = LineageGraph()
        g.add_edge("raw_clients.id", "stg_clients.client_id")
        g.add_edge("stg_clients.client_id", "core_clients.client_id")
        return g

    def test_parents_direct(self, chain_graph):
        assert chain_graph.parents("core_clients.client_id") == ["stg_clients.client_id"]
        assert chain_graph.parents("stg_clients.client_id") == ["raw_clients.id"]
        assert chain_graph.parents("raw_clients.id") == []

    def test_children_direct(self, chain_graph):
        assert chain_graph.children("raw_clients.id") == ["stg_clients.client_id"]
        assert chain_graph.children("core_clients.client_id") == []

    def test_ancestors_transitive(self, chain_graph):
        anc = chain_graph.ancestors("core_clients.client_id")
        assert sorted(anc) == ["raw_clients.id", "stg_clients.client_id"]

    def test_descendants_transitive(self, chain_graph):
        desc = chain_graph.descendants("raw_clients.id")
        assert sorted(desc) == ["core_clients.client_id", "stg_clients.client_id"]

    def test_missing_node_returns_empty(self, chain_graph):
        assert chain_graph.parents("nonexistent.col") == []
        assert chain_graph.ancestors("nonexistent.col") == []

    def test_has_node(self, chain_graph):
        assert chain_graph.has_node("core_clients.client_id")
        assert not chain_graph.has_node("missing.col")

    def test_get_node_returns_none_when_missing(self, chain_graph):
        assert chain_graph.get_node("missing.col") is None


class TestLineageGraphSerialization:
    def test_to_dict_shape(self):
        g = LineageGraph()
        g.add_edge("a.x", "b.y", {"transform": "passthrough"})
        d = g.to_dict()
        assert "nodes" in d
        assert "edges" in d
        node_ids = {n["id"] for n in d["nodes"]}
        assert node_ids == {"a.x", "b.y"}
        assert d["edges"][0]["src"] == "a.x"
        assert d["edges"][0]["dst"] == "b.y"
        assert d["edges"][0]["transform"] == "passthrough"

    def test_roundtrip_preserves_topology(self):
        g = LineageGraph()
        g.add_edge("raw.id", "stg.client_id", {"transform": "renamed"})
        g.add_edge("stg.client_id", "core.client_id", {"transform": "passthrough"})
        g.add_node("core.client_id", {"data_type": "INT64"})

        roundtrip = LineageGraph.from_dict(g.to_dict())

        assert roundtrip.node_count == 3
        assert roundtrip.edge_count == 2
        assert sorted(roundtrip.ancestors("core.client_id")) == ["raw.id", "stg.client_id"]
        assert roundtrip.get_node("core.client_id")["data_type"] == "INT64"

    def test_from_dict_empty(self):
        g = LineageGraph.from_dict({})
        assert g.node_count == 0
        assert g.edge_count == 0
