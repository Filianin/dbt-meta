"""Tests for LineageBuilder (manifest+catalog → LineageGraph)."""

from typing import Any
from unittest.mock import patch

import pytest

from dbt_meta.lineage import LineageBuilder
from dbt_meta.lineage.builder import _ModelTimeout, _per_model_timeout


def _make_manifest(model_name: str, schema: str, alias: str, sql: str, depends_on=None, database="testdb") -> dict[str, Any]:
    """Construct a minimal manifest with one model + optional sources."""
    unique_id = f"model.test_pkg.{model_name}"
    nodes: dict[str, Any] = {
        unique_id: {
            "name": model_name,
            "schema": schema,
            "alias": alias,
            "database": database,
            "package_name": "test_pkg",
            "compiled_code": sql,
            "depends_on": {"nodes": depends_on or []},
            "resource_type": "model",
        }
    }
    return {"nodes": nodes, "sources": {}}


def _make_catalog(catalog_id: str, schema: str, table: str, columns: list[str], database="testdb") -> dict[str, Any]:
    """Construct a minimal catalog entry for SQLGlot schema discovery."""
    return {
        "nodes": {
            catalog_id: {
                "metadata": {
                    "database": database,
                    "schema": schema,
                    "name": table,
                },
                "columns": {
                    col.lower(): {"name": col, "type": "STRING"} for col in columns
                },
            }
        },
        "sources": {},
    }


class TestSimpleBuild:
    def test_renamed_column_passthrough(self):
        sql = """
        SELECT id AS client_id, name AS client_name
        FROM testdb.raw_schema.clients
        """
        manifest = _make_manifest(
            "stg_clients",
            "staging",
            "stg_clients",
            sql,
            depends_on=["source.test_pkg.raw_schema.clients"],
        )
        # Add the source node so it shows up in the manifest's sources lookup
        manifest["sources"] = {
            "source.test_pkg.raw_schema.clients": {
                "schema": "raw_schema",
                "identifier": "clients",
                "name": "clients",
                "database": "testdb",
            }
        }
        catalog = _make_catalog(
            "source.test_pkg.raw_schema.clients",
            "raw_schema",
            "clients",
            ["id", "name"],
        )
        catalog["sources"] = catalog.pop("nodes")  # source goes under "sources"

        builder = LineageBuilder(manifest, catalog)
        graph, stats = builder.build()

        assert stats.models_parsed == 1
        # Output columns of stg_clients are present
        assert graph.has_node("stg_clients.client_id")
        assert graph.has_node("stg_clients.client_name")
        # Edges from source: id → client_id, name → client_name
        client_id_parents = graph.parents("stg_clients.client_id")
        assert any("clients" in p and p.endswith(".id") for p in client_id_parents)


class TestMissingCompiledCode:
    def test_skipped_with_warning(self):
        manifest = {
            "nodes": {
                "model.test_pkg.empty_model": {
                    "name": "empty_model",
                    "schema": "x",
                    "alias": "empty_model",
                    "database": "testdb",
                    "package_name": "test_pkg",
                    "compiled_code": "",  # missing!
                    "depends_on": {"nodes": []},
                    "resource_type": "model",
                }
            },
            "sources": {},
        }
        builder = LineageBuilder(manifest, {})
        _, stats = builder.build()
        assert stats.models_parsed == 0
        assert stats.models_skipped_no_sql == 1
        assert any("empty_model" in w for w in stats.warnings)


class TestParseError:
    def test_invalid_sql_skipped(self):
        manifest = _make_manifest(
            "broken",
            "x",
            "broken",
            "THIS IS NOT VALID SQL @#$%^",
        )
        builder = LineageBuilder(manifest, {})
        _, stats = builder.build()
        assert stats.models_parsed == 0
        assert stats.models_skipped_parse_error >= 1


class TestStatsCount:
    def test_models_total_counts_models_only(self):
        manifest = _make_manifest("m1", "x", "m1", "SELECT 1 AS a")
        # Add a non-model node which should be ignored
        manifest["nodes"]["test.test_pkg.dummy"] = {"name": "dummy", "compiled_code": "x"}
        builder = LineageBuilder(manifest, {})
        _, stats = builder.build()
        assert stats.models_total == 1


class TestSchemaConsistency:
    def test_uniform_3_level_schema(self):
        """Ensure SQLGlot schema is always 3-level (db.schema.table) to avoid SchemaError."""
        catalog = _make_catalog("source.x.foo.bar", "foo", "bar", ["a"], database="db1")
        # Add a second entry with a different database to test placeholder logic
        catalog["nodes"]["source.x.foo.baz"] = {
            "metadata": {"database": "", "schema": "foo", "name": "baz"},
            "columns": {"q": {"name": "q", "type": "STRING"}},
        }

        builder = LineageBuilder({"nodes": {}, "sources": {}}, catalog)
        builder._build_indices()

        schema = builder._sqlglot_schema
        # All keys at top level are databases (or placeholder)
        assert all(isinstance(v, dict) for v in schema.values())
        # All values 2 levels deep are tables (dicts of cols)
        for db_dict in schema.values():
            for sch_dict in db_dict.values():
                for table_value in sch_dict.values():
                    assert isinstance(table_value, dict)
                    # Each col maps to a string type
                    assert all(isinstance(t, str) for t in table_value.values())

    def test_default_db_placeholder_for_missing_database(self):
        catalog = {
            "nodes": {
                "model.x.foo": {
                    "metadata": {"database": "", "schema": "ds", "name": "foo"},
                    "columns": {"a": {"name": "a", "type": "STRING"}},
                }
            },
            "sources": {},
        }
        builder = LineageBuilder({"nodes": {}, "sources": {}}, catalog)
        builder._build_indices()
        assert LineageBuilder.DEFAULT_DB_PLACEHOLDER in builder._sqlglot_schema


# -----------------------------------------------------------------------------
# Real-edge generation tests — drive the builder end-to-end so leaf resolution,
# transform classification, and edge insertion are all exercised.
# -----------------------------------------------------------------------------


def _full_manifest(*, models: list[dict[str, Any]], sources: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """Build a manifest with multiple models + sources for end-to-end runs."""
    nodes: dict[str, Any] = {}
    src_nodes: dict[str, Any] = {}
    for m in models:
        uid = m["unique_id"]
        nodes[uid] = m["data"]
    for s in sources or []:
        src_nodes[s["unique_id"]] = s["data"]
    return {"nodes": nodes, "sources": src_nodes}


def _model_full(short, *, sql, alias=None, database="testdb", schema="ds"):
    return {
        "unique_id": f"model.test_pkg.{short}",
        "data": {
            "name": short,
            "alias": alias or short,
            "schema": schema,
            "database": database,
            "package_name": "test_pkg",
            "compiled_code": sql,
            "depends_on": {"nodes": []},
            "resource_type": "model",
        },
    }


class TestEdgeGeneration:
    def test_passthrough_edge_between_models(self):
        # downstream selects from upstream; lineage should produce
        # upstream.col -> downstream.col edge
        upstream = _model_full(
            "stg_upstream",
            sql="SELECT id, name FROM testdb.raw_ds.raw_clients",
            alias="stg_upstream",
            schema="ds",
        )
        downstream = _model_full(
            "core_downstream",
            sql="SELECT id AS client_id, name FROM testdb.ds.stg_upstream",
            alias="core_downstream",
        )
        manifest = _full_manifest(models=[upstream, downstream])
        catalog = {
            "nodes": {
                upstream["unique_id"]: {
                    "metadata": {"database": "testdb", "schema": "ds", "name": "stg_upstream"},
                    "columns": {
                        "id": {"name": "id", "type": "INT64"},
                        "name": {"name": "name", "type": "STRING"},
                    },
                }
            },
            "sources": {},
        }
        builder = LineageBuilder(manifest, catalog)
        graph, stats = builder.build()

        assert stats.models_parsed == 2
        # downstream's client_id should descend from upstream's id
        ancestors = graph.ancestors("core_downstream.client_id")
        assert "stg_upstream.id" in ancestors

    def test_derived_transform_classification(self):
        upstream = _model_full(
            "stg_amounts",
            sql="SELECT amount FROM testdb.raw_ds.raw_amounts",
            alias="stg_amounts",
        )
        downstream = _model_full(
            "agg_amounts",
            sql="SELECT amount * 2 AS doubled FROM testdb.ds.stg_amounts",
        )
        manifest = _full_manifest(models=[upstream, downstream])
        catalog = {
            "nodes": {
                upstream["unique_id"]: {
                    "metadata": {"database": "testdb", "schema": "ds", "name": "stg_amounts"},
                    "columns": {"amount": {"name": "amount", "type": "INT64"}},
                }
            },
            "sources": {},
        }
        builder = LineageBuilder(manifest, catalog)
        graph, stats = builder.build()
        assert stats.models_parsed == 2
        # The doubled column should have an upstream edge from stg_amounts.amount
        anc = graph.ancestors("agg_amounts.doubled")
        assert "stg_amounts.amount" in anc

    def test_source_node_resolves_to_synthetic_short_name(self):
        # A model that pulls directly from a manifest-declared source
        downstream = _model_full(
            "stg_orders",
            sql="SELECT id, status FROM testdb.raw_orders.orders",
            alias="stg_orders",
        )
        source = {
            "unique_id": "source.pkg.raw_orders.orders",
            "data": {
                "schema": "raw_orders",
                "identifier": "orders",
                "name": "orders",
                "database": "testdb",
            },
        }
        manifest = _full_manifest(models=[downstream], sources=[source])
        # Provide catalog so SELECT * style isn't needed
        catalog = {
            "nodes": {},
            "sources": {
                "source.pkg.raw_orders.orders": {
                    "metadata": {"database": "testdb", "schema": "raw_orders", "name": "orders"},
                    "columns": {
                        "id": {"name": "id", "type": "INT64"},
                        "status": {"name": "status", "type": "STRING"},
                    },
                }
            },
        }
        builder = LineageBuilder(manifest, catalog)
        graph, stats = builder.build()
        assert stats.models_parsed == 1
        # Source short name is "source.<schema>.<identifier>"
        assert "source.raw_orders.orders.id" in graph.ancestors("stg_orders.id")


class TestProgressAndSlowReporting:
    def test_progress_callback_fires_per_model(self):
        m1 = _model_full("m1", sql="SELECT 1 AS a")
        m2 = _model_full("m2", sql="SELECT 1 AS a")
        manifest = _full_manifest(models=[m1, m2])
        calls = []

        def cb(idx, total, name, elapsed):
            calls.append((idx, total, name))

        builder = LineageBuilder(manifest, {}, progress_callback=cb)
        builder.build()
        assert [c[0] for c in calls] == [1, 2]
        assert all(c[1] == 2 for c in calls)
        assert {c[2] for c in calls} == {"m1", "m2"}

    def test_slow_models_recorded(self):
        m = _model_full("slow", sql="SELECT 1 AS a")
        manifest = _full_manifest(models=[m])
        # Threshold below any real elapsed time — every model qualifies
        builder = LineageBuilder(manifest, {}, slow_threshold_seconds=0.0)
        _, stats = builder.build()
        assert any(name == "slow" for name, _ in stats.slow_models)


class TestAmbiguousAndIndexEdgeCases:
    def test_ambiguous_bare_name_dropped_from_index(self):
        # Two models with same alias "shared" in different schemas/dbs.
        # The bare-name index entry must collapse to "" (ambiguous).
        m1 = _model_full("m1", sql="SELECT 1 AS a", alias="shared", schema="s1")
        m2 = _model_full("m2", sql="SELECT 1 AS a", alias="shared", schema="s2")
        manifest = _full_manifest(models=[m1, m2])
        builder = LineageBuilder(manifest, {})
        builder._build_indices()
        assert builder._table_index.get("shared") == ""
        # But schema-qualified entries still resolve unambiguously
        assert builder._table_index["s1.shared"] == "m1"
        assert builder._table_index["s2.shared"] == "m2"

    def test_register_table_skips_empty_table_name(self):
        builder = LineageBuilder({"nodes": {}, "sources": {}}, {})
        builder._register_table("db", "ds", "", "target")
        assert builder._table_index == {}

    def test_lookup_table_returns_none_for_unknown(self):
        builder = LineageBuilder({"nodes": {}, "sources": {}}, {})
        builder._build_indices()
        assert builder._lookup_table("db", "ds", "nope") is None

    def test_lookup_table_returns_none_when_ambiguous(self):
        # Set up ambiguous bare name explicitly
        builder = LineageBuilder({"nodes": {}, "sources": {}}, {})
        builder._table_index["ambiguous"] = ""
        assert builder._lookup_table("", "", "ambiguous") is None


class TestColumnTypesAndCatalog:
    def test_column_types_for_model_returns_empty_when_no_catalog(self):
        builder = LineageBuilder({"nodes": {}, "sources": {}}, None)
        assert builder._column_types_for_model("model.x.foo") == {}

    def test_column_types_for_model_returns_empty_for_unknown(self):
        builder = LineageBuilder({"nodes": {}, "sources": {}}, {"nodes": {}, "sources": {}})
        assert builder._column_types_for_model("model.x.foo") == {}

    def test_column_types_for_model_lower_cases_keys_upper_cases_types(self):
        catalog = {
            "nodes": {
                "model.x.foo": {
                    "metadata": {"schema": "ds", "name": "foo"},
                    "columns": {
                        "Id": {"name": "Id", "type": "int64"},
                        "Name": {"name": "Name", "type": "string"},
                    },
                }
            },
            "sources": {},
        }
        builder = LineageBuilder({"nodes": {}, "sources": {}}, catalog)
        types = builder._column_types_for_model("model.x.foo")
        assert types == {"id": "INT64", "name": "STRING"}

    def test_catalog_table_name_uses_manifest_alias(self):
        manifest = _full_manifest(models=[_model_full("m", sql="SELECT 1", alias="m_alias")])
        catalog_meta = {"name": "m_alias", "schema": "ds", "database": "db"}
        builder = LineageBuilder(manifest, {})
        # Should prefer manifest's alias over catalog meta name
        assert builder._catalog_table_name("model.test_pkg.m", catalog_meta) == "m_alias"

    def test_catalog_table_name_falls_back_to_meta_name(self):
        builder = LineageBuilder({"nodes": {}, "sources": {}}, {})
        assert builder._catalog_table_name(
            "unknown.id", {"name": "fallback_name"}
        ) == "fallback_name"


class TestErrorBranches:
    def test_parse_error_records_warning_with_class_name(self):
        # SQL that ParseError chokes on → goes through the SqlglotError branch
        m = _model_full("broken", sql="THIS IS NOT @#$%^ VALID")
        manifest = _full_manifest(models=[m])
        builder = LineageBuilder(manifest, {})
        _, stats = builder.build()
        assert stats.models_skipped_parse_error == 1
        assert any("broken: parse error:" in w for w in stats.warnings)

    def test_results_not_dict_returns_silently(self):
        # Force lineage() to return a non-dict to exercise the guard branch
        m = _model_full("ok", sql="SELECT 1 AS a")
        manifest = _full_manifest(models=[m])
        builder = LineageBuilder(manifest, {})
        with patch("dbt_meta.lineage.builder.lineage", return_value="not-a-dict"):
            graph, stats = builder.build()
        assert stats.models_parsed == 0
        assert graph.node_count == 0


class TestPerModelTimeout:
    def test_zero_timeout_is_noop(self):
        # Should not raise even if alarm is zero
        with _per_model_timeout(0):
            pass

    def test_timeout_raises(self):
        # Use a tiny timeout (1s) and a busy loop that exceeds it
        import time as _time

        with pytest.raises(_ModelTimeout):
            with _per_model_timeout(1):
                # Burn ~2s of CPU so SIGALRM fires
                start = _time.time()
                while _time.time() - start < 2.5:
                    pass

    def test_builder_records_timeout_skip(self):
        # Patch lineage to sleep longer than the per-model budget
        import time as _time

        def slow_lineage(*a, **kw):
            _time.sleep(2.0)
            return {}

        m = _model_full("hang", sql="SELECT 1 AS a")
        manifest = _full_manifest(models=[m])
        builder = LineageBuilder(manifest, {}, per_model_timeout=1)
        with patch("dbt_meta.lineage.builder.lineage", side_effect=slow_lineage):
            _, stats = builder.build()
        assert stats.models_skipped_timeout == 1
        assert any("timeout after 1s" in w for w in stats.warnings)


class TestClassifyTransform:
    """Indirectly via _classify_transform — but we can call it on real Nodes."""

    def test_classify_passthrough_via_real_lineage(self):
        # SELECT id AS x FROM upstream → x's leaf is a bare Column → passthrough
        upstream = _model_full("up", sql="SELECT id FROM testdb.raw.t", alias="up")
        downstream = _model_full("down", sql="SELECT id FROM testdb.ds.up")
        manifest = _full_manifest(models=[upstream, downstream])
        catalog = {
            "nodes": {
                upstream["unique_id"]: {
                    "metadata": {"database": "testdb", "schema": "ds", "name": "up"},
                    "columns": {"id": {"name": "id", "type": "INT64"}},
                }
            },
            "sources": {},
        }
        builder = LineageBuilder(manifest, catalog)
        graph, _ = builder.build()
        # Confirm the edge exists — classify_transform was called
        assert "up.id" in graph.ancestors("down.id")
