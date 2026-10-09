"""Tests for ``ManifestGraph`` — the single dependency graph behind
``run-set``, ``contour`` and ``optimize refresh``.
"""

import json

import pytest

from dbt_meta.manifest.graph import ManifestGraph
from dbt_meta.manifest.parser import ManifestParser

M = "model.p."


def _node(*parents, materialized="table"):
    return {"depends_on": {"nodes": list(parents)}, "config": {"materialized": materialized}}


def _graph(nodes, **kw):
    return ManifestGraph({"nodes": nodes}, **kw)


@pytest.fixture
def diamond():
    #   a → b → d
    #   a → c → d
    return _graph(
        {
            M + "a": _node(),
            M + "b": _node(M + "a"),
            M + "c": _node(M + "a"),
            M + "d": _node(M + "b", M + "c"),
        }
    )


class TestClosures:
    def test_chain_ancestors_and_descendants_include_self(self):
        g = _graph({M + "a": _node(), M + "b": _node(M + "a"), M + "c": _node(M + "b")})
        assert g.ancestors(M + "c") == {M + "a", M + "b", M + "c"}
        assert g.descendants(M + "a") == {M + "a", M + "b", M + "c"}

    def test_diamond_reaches_both_branches_once(self, diamond):
        assert diamond.ancestors(M + "d") == {M + "a", M + "b", M + "c", M + "d"}
        assert diamond.descendants(M + "b") == {M + "b", M + "d"}

    def test_cycle_terminates(self):
        g = _graph({M + "a": _node(M + "b"), M + "b": _node(M + "a")})
        assert g.ancestors(M + "a") == {M + "a", M + "b"}
        assert g.descendants(M + "a") == {M + "a", M + "b"}

    def test_self_loop(self):
        g = _graph({M + "a": _node(M + "a")})
        assert g.ancestors(M + "a") == {M + "a"}

    def test_unknown_uid_is_just_itself(self, diamond):
        assert diamond.ancestors(M + "ghost") == {M + "ghost"}
        assert diamond.descendants(M + "ghost") == {M + "ghost"}

    def test_deep_chain_has_no_recursion_limit(self):
        nodes = {f"{M}n0": _node()}
        for i in range(1, 5000):
            nodes[f"{M}n{i}"] = _node(f"{M}n{i - 1}")
        g = _graph(nodes)
        assert len(g.ancestors(f"{M}n4999")) == 5000

    def test_closures_are_memoized_and_immutable(self, diamond):
        first = diamond.ancestors(M + "d")
        assert diamond.ancestors(M + "d") is first
        assert diamond.descendants(M + "a") is diamond.descendants(M + "a")
        assert isinstance(first, frozenset)
        with pytest.raises(AttributeError):
            first.add(M + "x")  # type: ignore[attr-defined]


class TestEdges:
    def test_parent_outside_the_graph_is_dropped(self):
        g = _graph({M + "a": _node("source.p.raw.t", "model.p.missing")})
        assert g.parents[M + "a"] == []
        assert g.ancestors(M + "a") == {M + "a"}

    def test_prefix_filter_drops_seed_and_snapshot_edges(self):
        nodes = {
            "seed.p.s": _node(),
            "snapshot.p.snap": _node(M + "a"),
            M + "a": _node("seed.p.s"),
            M + "b": _node("snapshot.p.snap"),
        }
        full = _graph(nodes)
        assert full.descendants("seed.p.s") == {"seed.p.s", M + "a", "snapshot.p.snap", M + "b"}
        models = _graph(nodes, prefixes=("model.",))
        assert set(models.nodes) == {M + "a", M + "b"}
        assert models.ancestors(M + "b") == {M + "b"}
        assert models.descendants(M + "a") == {M + "a"}

    def test_non_graph_node_types_are_excluded(self):
        g = _graph({"test.p.t": _node(M + "a"), M + "a": _node()})
        assert set(g.nodes) == {M + "a"}
        assert g.children[M + "a"] == []

    def test_missing_or_null_depends_on(self):
        g = _graph(
            {M + "a": {}, M + "b": {"depends_on": None}, M + "c": {"depends_on": {"nodes": None}}}
        )
        assert g.parents == {M + "a": [], M + "b": [], M + "c": []}

    def test_empty_manifest(self):
        g = ManifestGraph({})
        assert g.nodes == {}
        assert g.topological(set()) == []
        assert g.resolve("a") is None


class TestResolve:
    def test_default_resolves_models_and_seeds_not_snapshots(self):
        g = _graph({M + "a": _node(), "seed.p.s": _node(), "snapshot.p.snap": _node()})
        assert g.resolve("a") == M + "a"
        assert g.resolve("s") == "seed.p.s"
        assert g.resolve("snap") is None
        assert g.resolve("snap", prefixes=("snapshot.",)) == "snapshot.p.snap"

    def test_first_match_wins_on_duplicate_short_names(self):
        g = _graph({"model.p1.x": _node(), "model.p2.x": _node()})
        assert g.resolve("x") == "model.p1.x"

    def test_name_with_prefix_filtered_out(self):
        g = _graph({"seed.p.s": _node()})
        assert g.resolve("s", prefixes=("model.",)) is None


class TestTopological:
    def test_parents_first_with_name_tie_break(self, diamond):
        assert diamond.topological(set(diamond.nodes)) == [M + "a", M + "b", M + "c", M + "d"]

    def test_only_given_uids_are_ordered(self, diamond):
        assert diamond.topological({M + "d", M + "b"}) == [M + "b", M + "d"]

    def test_independent_nodes_sorted(self):
        g = _graph({M + "z": _node(), M + "a": _node(), M + "m": _node()})
        assert g.topological(set(g.nodes)) == [M + "a", M + "m", M + "z"]

    def test_cycle_members_appended_sorted(self):
        g = _graph({M + "r": _node(), M + "b": _node(M + "a"), M + "a": _node(M + "b")})
        assert g.topological(set(g.nodes)) == [M + "r", M + "a", M + "b"]

    def test_accepts_frozenset(self, diamond):
        assert diamond.topological(diamond.ancestors(M + "b")) == [M + "a", M + "b"]


class TestPath:
    def test_shortest_path_inside_within(self, diamond):
        assert diamond.path(M + "a", M + "d", set(diamond.nodes)) == [M + "a", M + "b", M + "d"]

    def test_within_excludes_a_branch(self, diamond):
        assert diamond.path(M + "a", M + "d", {M + "a", M + "c", M + "d"}) == [
            M + "a",
            M + "c",
            M + "d",
        ]

    def test_unreachable(self):
        g = _graph({M + "a": _node(), M + "b": _node()})
        assert g.path(M + "a", M + "b", {M + "a", M + "b"}) == []

    def test_start_equals_end(self, diamond):
        assert diamond.path(M + "b", M + "b", set()) == [M + "b"]


class TestNodeKinds:
    def test_materialized_and_hidden(self):
        g = _graph(
            {
                "seed.p.s": _node(materialized="seed"),
                M + "e": _node(materialized="ephemeral"),
                M + "t": _node(),
                "snapshot.p.snap": _node(materialized="snapshot"),
            }
        )
        assert g.materialized("seed.p.s") == "seed"
        assert g.is_ephemeral(M + "e") and g.is_hidden(M + "e")
        assert not g.is_hidden(M + "t")
        assert g.is_hidden("snapshot.p.snap")

    def test_missing_config(self):
        g = _graph({M + "a": {"depends_on": {"nodes": []}}})
        assert g.materialized(M + "a") == ""


class TestParserGraph:
    def test_built_once_per_prefix_set(self, tmp_path):
        path = tmp_path / "manifest.json"
        path.write_text(json.dumps({"nodes": {M + "a": _node(), "seed.p.s": _node()}}))
        parser = ManifestParser(str(path))
        default = parser.graph()
        assert parser.graph() is default
        models = parser.graph(("model.",))
        assert models is not default
        assert parser.graph(("model.",)) is models
        assert set(models.nodes) == {M + "a"}
