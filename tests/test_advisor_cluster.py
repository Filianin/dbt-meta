"""Tests for ClusterAdvisor."""

import pytest

from dbt_meta.usage import ClusterAdvisor


def _model(short, *, sql, partition_by=None, alias=None):
    return f"model.pkg.{short}", {
        "name": short,
        "alias": alias or short,
        "schema": "ds",
        "database": "proj",
        "compiled_code": sql,
        "config": {"partition_by": partition_by} if partition_by else {},
        "package_name": "pkg",
        "resource_type": "model",
        "depends_on": {"nodes": []},
    }


def _build_manifest(target, downstreams):
    """Construct a manifest with a target and downstream models referencing it."""
    nodes = {}
    child_map = {target[0]: [d[0] for d in downstreams]}
    nodes[target[0]] = target[1]
    for uid, payload in downstreams:
        nodes[uid] = payload
        child_map.setdefault(uid, [])
    return {"nodes": nodes, "sources": {}, "child_map": child_map}


def _catalog(unique_id, columns):
    return {
        "nodes": {
            unique_id: {
                "metadata": {"database": "proj", "schema": "ds", "name": "target"},
                "columns": {c.lower(): {"name": c, "type": t} for c, t in columns.items()},
            }
        },
        "sources": {},
    }


class TestClusterAdvisorBasic:
    def test_recommends_join_filter_columns(self):
        target = _model("target", sql="SELECT 1", alias="target_table")
        downstream = _model(
            "ds1",
            sql="SELECT * FROM proj.ds.target_table t WHERE t.country = 'EE' AND t.amount > 10",
        )
        manifest = _build_manifest(target, [downstream])
        catalog = _catalog(target[0], {"country": "STRING", "amount": "INT64"})

        advisor = ClusterAdvisor(manifest, catalog)
        res = advisor.recommend("target")

        cols = [r.column for r in res.recommendations]
        assert "country" in cols
        assert "amount" in cols

    def test_excludes_partition_column(self):
        target = _model(
            "target",
            sql="SELECT 1",
            alias="target_table",
            partition_by={"field": "event_date", "data_type": "date"},
        )
        downstream = _model(
            "ds1",
            sql="SELECT * FROM proj.ds.target_table t WHERE t.event_date = '2026-01-01' AND t.client_id = 5",
        )
        manifest = _build_manifest(target, [downstream])
        catalog = _catalog(target[0], {"event_date": "DATE", "client_id": "INT64"})

        advisor = ClusterAdvisor(manifest, catalog)
        res = advisor.recommend("target")

        cols = [r.column for r in res.recommendations]
        assert "event_date" not in cols
        assert "client_id" in cols
        assert any(e["column"] == "event_date" for e in res.excluded)

    def test_excludes_struct_array_geography(self):
        target = _model("target", sql="SELECT 1", alias="t")
        downstream = _model(
            "ds1",
            sql="SELECT * FROM proj.ds.t t WHERE t.s = 1 AND t.a = 1 AND t.g = 1 AND t.j = 1",
        )
        manifest = _build_manifest(target, [downstream])
        catalog = _catalog(
            target[0],
            {"s": "STRUCT<x INT64>", "a": "ARRAY<INT64>", "g": "GEOGRAPHY", "j": "JSON"},
        )

        advisor = ClusterAdvisor(manifest, catalog)
        res = advisor.recommend("target")
        assert res.recommendations == []
        excluded = {e["column"] for e in res.excluded}
        assert excluded == {"s", "a", "g", "j"}

    def test_top_n_caps_at_4(self):
        target = _model("target", sql="SELECT 1", alias="t")
        # 6 columns, all heavily used
        downstreams = [
            _model(
                f"ds{i}",
                sql=f"SELECT * FROM proj.ds.t t WHERE t.c1=1 AND t.c2=2 AND t.c3=3 AND t.c4=4 AND t.c5=5 AND t.c6=6",
            )
            for i in range(3)
        ]
        manifest = _build_manifest(target, downstreams)
        catalog = _catalog(target[0], {f"c{i}": "INT64" for i in range(1, 7)})

        advisor = ClusterAdvisor(manifest, catalog, top_n=4)
        res = advisor.recommend("target")
        assert len(res.recommendations) == 4

    def test_unknown_target_returns_empty_with_warning(self):
        advisor = ClusterAdvisor({"nodes": {}, "child_map": {}})
        res = advisor.recommend("nonexistent")
        assert res.recommendations == []
        assert any("not found" in w for w in res.warnings)

    def test_no_downstream_no_recommendations(self):
        target = _model("target", sql="SELECT 1", alias="t")
        manifest = _build_manifest(target, [])
        advisor = ClusterAdvisor(manifest, _catalog(target[0], {"x": "INT64"}))
        res = advisor.recommend("target")
        assert res.recommendations == []


class TestClusterScoring:
    def test_eq_outweighs_join_when_same_count(self):
        # WHERE eq weight 3.0 > JOIN weight 2.0
        target = _model("target", sql="SELECT 1", alias="t")
        ds1 = _model("ds1", sql="SELECT * FROM proj.ds.t t WHERE t.c_filter = 1")
        ds2 = _model("ds2", sql="SELECT 1 FROM proj.ds.other o JOIN proj.ds.t t ON t.c_join = o.id")
        manifest = _build_manifest(target, [ds1, ds2])
        catalog = _catalog(target[0], {"c_filter": "INT64", "c_join": "INT64"})

        advisor = ClusterAdvisor(manifest, catalog)
        res = advisor.recommend("target")

        cols = [r.column for r in res.recommendations]
        assert cols.index("c_filter") < cols.index("c_join")

    def test_function_wrapped_columns_score_low(self):
        target = _model("target", sql="SELECT 1", alias="t")
        ds_fn = _model("ds1", sql="SELECT * FROM proj.ds.t t WHERE UPPER(t.fnwrap) = 'X'")
        ds_eq = _model("ds2", sql="SELECT * FROM proj.ds.t t WHERE t.plain = 1")
        manifest = _build_manifest(target, [ds_fn, ds_eq])
        catalog = _catalog(target[0], {"fnwrap": "STRING", "plain": "INT64"})

        advisor = ClusterAdvisor(manifest, catalog)
        res = advisor.recommend("target")
        cols = [r.column for r in res.recommendations]
        assert cols.index("plain") < cols.index("fnwrap")

    def test_diagnoses_empty_compiled_code(self):
        # Regression mirror of the partition advisor case: a dev manifest
        # from ``dbt parse`` lacks compiled_code, so 0 events come out
        # despite a populated child_map.
        target = _model("target", sql="SELECT 1", alias="t")
        downstreams = [
            (
                f"model.pkg.ds{i}",
                {
                    "name": f"ds{i}",
                    "alias": f"ds{i}",
                    "schema": "ds",
                    "database": "proj",
                    "compiled_code": "",
                    "config": {},
                    "package_name": "pkg",
                    "resource_type": "model",
                    "depends_on": {"nodes": []},
                },
            )
            for i in range(4)
        ]
        manifest = _build_manifest(target, downstreams)
        advisor = ClusterAdvisor(manifest, _catalog(target[0], {"x": "INT64"}))
        res = advisor.recommend("target")
        assert res.direct_downstream_count == 4
        assert res.analysed_downstream_count == 0
        assert res.recommendations == []
        assert any("compiled_code" in w for w in res.warnings)

    def test_unknown_operators_do_not_contribute_silent_score(self):
        # Regression: WHERE neq/like/is_null/none used to fall through
        # ``dict.get(..., 0.5)`` and silently inflate scores without showing
        # any matching counter in ``reasoning`` (score did not match the
        # visible breakdown).
        target = _model("target", sql="SELECT 1", alias="t")
        downstream = _model(
            "ds1",
            sql=(
                "SELECT * FROM proj.ds.t t "
                "WHERE t.col_neq != 5 "
                "AND t.col_like LIKE 'foo%' "
                "AND t.col_isnull IS NULL "
                "AND t.col_eq = 1"
            ),
        )
        manifest = _build_manifest(target, [downstream])
        catalog = _catalog(
            target[0],
            {
                "col_neq": "INT64",
                "col_like": "STRING",
                "col_isnull": "INT64",
                "col_eq": "INT64",
            },
        )
        advisor = ClusterAdvisor(manifest, catalog)
        res = advisor.recommend("target")
        cols = [r.column for r in res.recommendations]
        # Only col_eq has a scoring operator; the others must be dropped.
        assert cols == ["col_eq"]
        # And the visible reasoning must fully account for the score.
        rec = res.recommendations[0]
        # WHERE eq weight 3.0 × log2(1+1)=1.0 → 3.0
        assert rec.score == pytest.approx(3.0)
        assert any("equality" in r.lower() for r in rec.reasoning)
