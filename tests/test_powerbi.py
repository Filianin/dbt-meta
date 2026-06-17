"""Behavior tests for the Power BI metadata pipeline.

Organized by module — M-expression parsing, SQL analysis, cross-query
resolution, dbt classification, index build, artifact persistence, querying,
the Scanner API client, and the command orchestration layer. All tests exercise
public interfaces only.
"""

import orjson
import pytest

import dbt_meta.command_impl.powerbi as cmd
import dbt_meta.powerbi.scanner as scanner
from dbt_meta.config import Config
from dbt_meta.errors import DbtMetaError
from dbt_meta.powerbi.artifact import (
    artifact_age_hours,
    find_powerbi_artifact,
    load_index,
    save_index,
)
from dbt_meta.powerbi.index import (
    PowerBiIndex,
    ReportEntry,
    SqlAnalysisEntry,
    TableRef,
    build_index,
)
from dbt_meta.powerbi.m_parser import parse_m_expression
from dbt_meta.powerbi.mapper import DbtTableIndex
from dbt_meta.powerbi.query import find, show
from dbt_meta.powerbi.resolver import QueryNode, resolve_query_tables
from dbt_meta.powerbi.sql_analyzer import analyze_sql
from tests import powerbi_fixtures as fx

# ============================================================================
# M-expression parser
# ============================================================================


class TestMParserNavigation:
    def test_navigation_yields_fully_qualified_table(self):
        result = parse_m_expression(fx.NAV_TABLE)

        assert result.kind == "navigation"
        assert result.tables == ("my-project.core_utils.d_calendar",)
        assert result.parse_status == "ok"

    def test_navigation_to_view_yields_table(self):
        result = parse_m_expression(fx.NAV_VIEW)

        assert result.kind == "navigation"
        assert result.tables == ("my-project.report_x.v_sales",)


class TestMParserNativeSql:
    def test_single_table_extracts_decoded_sql(self):
        result = parse_m_expression(fx.NATIVE_SINGLE)

        assert result.kind == "native_sql"
        # #(lf) decoded to real newlines, ready for SQLGlot
        assert "#(lf)" not in result.native_sql
        assert "\n" in result.native_sql
        assert "`my-project.core_client.client_info`" in result.native_sql
        assert result.parse_status == "ok"

    def test_join_keeps_full_sql(self):
        result = parse_m_expression(fx.NATIVE_JOIN)

        assert result.kind == "native_sql"
        assert "LEFT JOIN" in result.native_sql
        assert result.cross_query_refs == ()

    def test_nested_join_records_cross_query_ref(self):
        result = parse_m_expression(fx.NATIVE_NESTED_JOIN)

        assert result.kind == "native_sql"
        assert "client_model_countries" in result.cross_query_refs

    def test_quoted_cross_query_ref_is_unquoted(self):
        expr = (
            'let\n    Source = Value.NativeQuery(GoogleBigQuery.Database()'
            '{[Name="my-project"]}[Data], "select id from my-project.core.t",'
            ' null, [EnableFolding=true]),\n'
            '    #"Merged" = Table.NestedJoin(Source, {"id"}, '
            '#"bi am_lookup", {"id"}, "bi am_lookup", JoinKind.LeftOuter)\n'
            'in\n    #"Merged"'
        )
        result = parse_m_expression(expr)

        assert "bi am_lookup" in result.cross_query_refs


class TestMParserNonBigQuery:
    def test_inline_table(self):
        assert parse_m_expression(fx.INLINE_FROM_ROWS).kind == "inline"

    def test_dax_calculated_table(self):
        assert parse_m_expression(fx.DAX_CALCULATED).kind == "dax"


# ============================================================================
# Native-SQL analyzer
# ============================================================================


class TestSqlAnalyzerTableExtraction:
    def test_single_table(self):
        result = analyze_sql(
            "SELECT client_id FROM `my-project.core_client.client_info`"
        )

        assert result.tables == ("my-project.core_client.client_info",)
        assert result.parse_status == "ok"

    def test_unquoted_hyphenated_project(self):
        result = analyze_sql(
            "SELECT * FROM my-project.staging_amas.profiles_legal_owners"
        )

        assert result.tables == ("my-project.staging_amas.profiles_legal_owners",)

    def test_join_collects_both_tables(self):
        sql = (
            "SELECT urp.tr_client_id, ci.current_country\n"
            "FROM `my-project.core_client.client_registration_utm_params` AS urp\n"
            "LEFT JOIN `my-project.core_client.client_info` AS ci\n"
            "  ON urp.tr_client_id = ci.client_id\n"
            "WHERE urp.registration_date >= '2025-01-01'"
        )
        result = analyze_sql(sql)

        assert set(result.tables) == {
            "my-project.core_client.client_registration_utm_params",
            "my-project.core_client.client_info",
        }

    def test_cte_reference_is_not_a_table(self):
        sql = (
            "WITH clients AS (SELECT * FROM `p.s.client_info`)\n"
            "SELECT * FROM clients"
        )
        result = analyze_sql(sql)

        assert result.tables == ("p.s.client_info",)


class TestSqlAnalyzerClauseExtraction:
    def test_where_filters_and_group_by(self):
        sql = (
            "SELECT country, count(*) FROM `p.s.t`\n"
            "WHERE status = 'active'\n"
            "GROUP BY country"
        )
        result = analyze_sql(sql)

        assert "status" in result.filters
        assert "country" in result.group_by

    def test_join_columns_captured(self):
        sql = (
            "SELECT a.x FROM `p.s.a` AS a\n"
            "JOIN `p.s.b` AS b ON a.id = b.id"
        )
        result = analyze_sql(sql)

        assert "id" in result.joins


class TestSqlAnalyzerFallback:
    def test_unparseable_sql_marks_partial_but_keeps_tables(self):
        # Deliberately broken SQL that SQLGlot cannot fully parse.
        result = analyze_sql("SELECT FROM WHERE FROM `my-project.core.t` GROUP")

        assert result.parse_status == "partial"
        assert "my-project.core.t" in result.tables


# ============================================================================
# Intra-dataset cross-query resolution
# ============================================================================


class TestResolver:
    def test_direct_tables_returned(self):
        nodes = {"A": QueryNode(tables=("p.s.t1",), cross_query_refs=())}

        assert resolve_query_tables("A", nodes) == ("p.s.t1",)

    def test_cross_query_ref_resolved_to_leaf(self):
        nodes = {
            "Main": QueryNode(tables=("p.s.main",), cross_query_refs=("Lookup",)),
            "Lookup": QueryNode(tables=("p.s.countries",), cross_query_refs=()),
        }

        assert set(resolve_query_tables("Main", nodes)) == {
            "p.s.main",
            "p.s.countries",
        }

    def test_transitive_chain(self):
        nodes = {
            "A": QueryNode(tables=(), cross_query_refs=("B",)),
            "B": QueryNode(tables=(), cross_query_refs=("C",)),
            "C": QueryNode(tables=("p.s.leaf",), cross_query_refs=()),
        }

        assert resolve_query_tables("A", nodes) == ("p.s.leaf",)

    def test_cycle_is_safe(self):
        nodes = {
            "A": QueryNode(tables=("p.s.a",), cross_query_refs=("B",)),
            "B": QueryNode(tables=("p.s.b",), cross_query_refs=("A",)),
        }

        assert set(resolve_query_tables("A", nodes)) == {"p.s.a", "p.s.b"}

    def test_missing_ref_ignored(self):
        nodes = {"A": QueryNode(tables=("p.s.a",), cross_query_refs=("Ghost",))}

        assert resolve_query_tables("A", nodes) == ("p.s.a",)


# ============================================================================
# Physical-table -> dbt classification
# ============================================================================


MAPPER_MANIFEST = {
    "nodes": {
        "model.proj.client_info": {
            "resource_type": "model",
            "name": "client_info",
            "database": "admirals-bi-dwh",
            "schema": "core_client",
            "config": {"alias": "client_info"},
        },
        "model.proj.fct_orders": {
            "resource_type": "model",
            "name": "fct_orders",
            "database": "admirals-bi-dwh",
            "schema": "marts",
            "config": {"alias": "orders"},  # aliased physical name
        },
        "test.proj.some_test": {"resource_type": "test", "name": "t"},
    },
    "sources": {
        "source.proj.raw.events": {
            "resource_type": "source",
            "name": "events",
            "database": "admirals-bi-dwh",
            "schema": "raw_amas",
            "identifier": "events_raw",
        },
    },
}


class TestMapper:
    def test_model_resolved_by_alias(self):
        idx = DbtTableIndex(MAPPER_MANIFEST)

        m = idx.lookup("admirals-bi-dwh.marts.orders")
        assert m.status == "model"
        assert m.dbt_name == "fct_orders"

    def test_model_resolved_by_name(self):
        idx = DbtTableIndex(MAPPER_MANIFEST)

        m = idx.lookup("admirals-bi-dwh.core_client.client_info")
        assert m.status == "model"
        assert m.dbt_name == "client_info"

    def test_source_resolved_by_identifier(self):
        idx = DbtTableIndex(MAPPER_MANIFEST)

        m = idx.lookup("admirals-bi-dwh.raw_amas.events_raw")
        assert m.status == "source"
        assert m.dbt_name == "events"

    def test_unknown_table_is_external(self):
        idx = DbtTableIndex(MAPPER_MANIFEST)

        m = idx.lookup("other-project.staging_amas.profiles")
        assert m.status == "external"
        assert m.dbt_name is None

    def test_lookup_is_case_insensitive(self):
        idx = DbtTableIndex(MAPPER_MANIFEST)

        assert idx.lookup("admirals-bi-dwh.MARTS.Orders").status == "model"

    def test_two_part_name_resolves_to_model(self):
        # Native SQL often omits the (default) project id.
        idx = DbtTableIndex(MAPPER_MANIFEST)

        m = idx.lookup("core_client.client_info")
        assert m.status == "model"
        assert m.dbt_name == "client_info"

    def test_two_part_name_resolves_source_by_identifier(self):
        idx = DbtTableIndex(MAPPER_MANIFEST)

        m = idx.lookup("raw_amas.events_raw")
        assert m.status == "source"
        assert m.dbt_name == "events"

    def test_ambiguous_two_part_name_stays_external(self):
        # Same schema.table in two projects — cannot disambiguate, so external.
        manifest = {
            "nodes": {
                "model.proj.a": {
                    "resource_type": "model",
                    "name": "shared",
                    "database": "proj-a",
                    "schema": "core",
                    "config": {},
                },
                "model.proj.b": {
                    "resource_type": "model",
                    "name": "shared",
                    "database": "proj-b",
                    "schema": "core",
                    "config": {},
                },
            },
            "sources": {},
        }
        idx = DbtTableIndex(manifest)

        assert idx.lookup("core.shared").status == "external"
        # Fully-qualified still resolves.
        assert idx.lookup("proj-a.core.shared").status == "model"


# ============================================================================
# Index build from a scanResult
# ============================================================================


INDEX_MANIFEST = {
    "nodes": {
        "model.proj.d_calendar": {
            "resource_type": "model",
            "name": "d_calendar",
            "database": "my-project",
            "schema": "core_utils",
            "config": {"alias": "d_calendar"},
        },
        "model.proj.client_info": {
            "resource_type": "model",
            "name": "client_info",
            "database": "my-project",
            "schema": "core_client",
            "config": {"alias": "client_info"},
        },
    },
    "sources": {},
}


def _index_scan(tables, reports, dataset_name="DS", dataset_id="ds1", ws="BI Marketing"):
    return {
        "workspaces": [
            {
                "name": ws,
                "datasets": [
                    {"id": dataset_id, "name": dataset_name, "tables": tables}
                ],
                "reports": reports,
            }
        ]
    }


def _index_table(name, expression, measures=None):
    return {
        "name": name,
        "source": [{"expression": expression}],
        "measures": measures or [],
        "columns": [],
    }


class TestIndexReportMapping:
    def test_navigation_table_classified_as_model(self):
        scan = _index_scan(
            tables=[_index_table("d_calendar", fx.NAV_TABLE)],
            reports=[{"name": "Sales", "datasetId": "ds1"}],
        )
        idx = build_index(scan, INDEX_MANIFEST)

        report = idx.reports[0]
        assert report.report == "Sales"
        assert report.workspace == "BI Marketing"
        assert report.dataset == "DS"
        bqs = {t.bq: t for t in report.tables}
        assert "my-project.core_utils.d_calendar" in bqs
        assert bqs["my-project.core_utils.d_calendar"].status == "model"
        assert bqs["my-project.core_utils.d_calendar"].dbt_model == "d_calendar"

    def test_native_sql_table_listed_with_analysis(self):
        scan = _index_scan(
            tables=[_index_table("clients", fx.NATIVE_SINGLE)],
            reports=[{"name": "Clients", "datasetId": "ds1"}],
        )
        idx = build_index(scan, INDEX_MANIFEST)

        report = idx.reports[0]
        assert any(
            t.bq == "my-project.core_client.client_info" for t in report.tables
        )
        assert report.sql_analysis  # native SQL produced an analysis entry

    def test_report_without_dataset_has_no_tables(self):
        scan = _index_scan(
            tables=[_index_table("d_calendar", fx.NAV_TABLE)],
            reports=[{"name": "Orphan", "datasetId": "missing"}],
        )
        idx = build_index(scan, INDEX_MANIFEST)

        assert idx.reports[0].tables == []


class TestIndexCrossQueryResolution:
    def test_nested_join_pulls_sibling_table(self):
        # Main native query references sibling query 'client_model_countries'
        sibling = _index_table("client_model_countries", fx.NAV_TABLE)  # -> d_calendar
        main = _index_table("profiles", fx.NATIVE_NESTED_JOIN)
        scan = _index_scan(
            tables=[main, sibling],
            reports=[{"name": "Profiles", "datasetId": "ds1"}],
        )
        idx = build_index(scan, INDEX_MANIFEST)

        bqs = {t.bq for t in idx.reports[0].tables}
        assert "my-project.core_amas.client_profiles_real" in bqs
        assert "my-project.core_utils.d_calendar" in bqs  # via cross-query ref


class TestIndexMetricIndex:
    def test_measure_maps_to_dataset_tables(self):
        tbl = _index_table(
            "clients",
            fx.NATIVE_SINGLE,
            measures=[{"name": "Total Clients", "expression": "COUNT(1)"}],
        )
        scan = _index_scan(
            tables=[tbl],
            reports=[{"name": "Clients", "datasetId": "ds1"}],
        )
        idx = build_index(scan, INDEX_MANIFEST)

        assert (
            "my-project.core_client.client_info"
            in idx.metric_index["Total Clients"]
        )


# ============================================================================
# Artifact persistence
# ============================================================================


def _sample_index():
    return PowerBiIndex(
        reports=[
            ReportEntry(
                workspace="BI Marketing",
                report="Sales",
                dataset="DS",
                tables=[TableRef(bq="p.s.t", status="model", dbt_model="t")],
                sql_analysis=[
                    SqlAnalysisEntry(
                        query="q",
                        tables=("p.s.t",),
                        filters=("status",),
                        joins=(),
                        group_by=("country",),
                        parse_status="ok",
                    )
                ],
            )
        ],
        metric_index={"Total": ["p.s.t"]},
        generated_at="2026-06-16T00:00:00+00:00",
    )


class TestArtifact:
    def test_round_trip_preserves_data(self, tmp_path):
        path = tmp_path / "powerbi_index.json"
        save_index(_sample_index(), str(path))

        loaded = load_index(str(path))

        assert loaded.reports[0].report == "Sales"
        assert loaded.reports[0].tables[0].bq == "p.s.t"
        assert loaded.reports[0].tables[0].status == "model"
        assert loaded.reports[0].sql_analysis[0].group_by == ("country",)
        assert loaded.metric_index["Total"] == ["p.s.t"]
        assert loaded.schema_version == "1.0"

    def test_age_reflects_recent_write(self, tmp_path):
        path = tmp_path / "powerbi_index.json"
        save_index(_sample_index(), str(path))

        assert artifact_age_hours(str(path)) < 1.0

    def test_age_none_when_missing(self, tmp_path):
        assert artifact_age_hours(str(tmp_path / "nope.json")) is None

    def test_find_uses_explicit_path(self, tmp_path):
        path = tmp_path / "powerbi_index.json"
        save_index(_sample_index(), str(path))

        assert find_powerbi_artifact(explicit_path=str(path)) == str(path.absolute())

    def test_find_uses_env_path(self, tmp_path, monkeypatch):
        path = tmp_path / "powerbi_index.json"
        save_index(_sample_index(), str(path))
        monkeypatch.setenv("DBT_PROD_POWERBI_PATH", str(path))

        assert find_powerbi_artifact() == str(path.absolute())

    def test_find_raises_when_missing(self, tmp_path, monkeypatch):
        monkeypatch.delenv("DBT_PROD_POWERBI_PATH", raising=False)
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(
            "dbt_meta.powerbi.artifact.Path.home", lambda: tmp_path / "nohome"
        )

        with pytest.raises(FileNotFoundError):
            find_powerbi_artifact()


# ============================================================================
# Querying the index (find / show)
# ============================================================================


def _query_index():
    return PowerBiIndex(
        reports=[
            ReportEntry(
                workspace="BI Marketing",
                report="Organic Leads",
                dataset="Leads DS",
                tables=[
                    TableRef(
                        bq="p.core_client.client_info",
                        status="model",
                        dbt_model="client_info",
                    ),
                    TableRef(bq="p.raw_amas.events", status="external"),
                ],
            ),
            ReportEntry(
                workspace="BI Trading",
                report="Trading Volume",
                dataset="Volume DS",
                tables=[
                    TableRef(
                        bq="p.marts.volume", status="model", dbt_model="volume"
                    )
                ],
            ),
        ],
        metric_index={"Total Leads": ["p.core_client.client_info"]},
    )


class TestQueryFind:
    def test_matches_report_name_case_insensitive(self):
        results = find(_query_index(), "organic")

        assert [r.report for r in results.reports] == ["Organic Leads"]

    def test_matches_table_name(self):
        results = find(_query_index(), "volume")

        # report 'Trading Volume' and table 'p.marts.volume' both match
        assert any(r.report == "Trading Volume" for r in results.reports)

    def test_matches_metric(self):
        results = find(_query_index(), "leads")

        assert "Total Leads" in results.metrics
        assert results.metrics["Total Leads"] == ["p.core_client.client_info"]

    def test_no_match_is_empty(self):
        results = find(_query_index(), "zzz")

        assert results.reports == []
        assert results.metrics == {}


class TestQueryShow:
    def test_returns_full_report_breakdown(self):
        report = show(_query_index(), "Organic Leads")

        assert report is not None
        assert report.dataset == "Leads DS"
        assert len(report.tables) == 2

    def test_unknown_report_returns_none(self):
        assert show(_query_index(), "Nonexistent") is None

    def test_partial_name_resolves(self):
        report = show(_query_index(), "organic")

        assert report is not None
        assert report.report == "Organic Leads"


# ============================================================================
# Scanner API client (HTTP boundary mocked)
# ============================================================================


class _FakeApi:
    """Stand-in for the Power BI HTTP boundary, scripted per endpoint."""

    def __init__(self, statuses, scan_result=None):
        self._statuses = list(statuses)
        self._scan_result = scan_result or {
            "workspaces": [{"name": "BI Marketing", "datasets": []}]
        }
        self.calls = []

    def __call__(self, token, endpoint, method="GET", data=None, timeout=30):
        self.calls.append((endpoint, method, data))
        if endpoint.startswith("/admin/workspaces/getInfo"):
            return {"id": "scan-123"}
        if endpoint.startswith("/admin/workspaces/scanStatus"):
            return {"status": self._statuses.pop(0)}
        if endpoint.startswith("/admin/workspaces/scanResult"):
            return self._scan_result
        return None


class TestScanner:
    def test_scan_workspaces_returns_full_result(self, monkeypatch):
        fake = _FakeApi(statuses=["Running", "Succeeded"])
        monkeypatch.setattr(scanner, "_call_powerbi_api", fake)

        result = scanner.scan_workspaces("tok", ["ws1", "ws2"], poll_interval=0)

        assert result["workspaces"][0]["name"] == "BI Marketing"

    def test_scan_sends_all_workspaces_in_one_batch(self, monkeypatch):
        fake = _FakeApi(statuses=["Succeeded"])
        monkeypatch.setattr(scanner, "_call_powerbi_api", fake)

        scanner.scan_workspaces("tok", ["ws1", "ws2", "ws3"], poll_interval=0)

        get_info = next(
            c for c in fake.calls if c[0].startswith("/admin/workspaces/getInfo")
        )
        assert get_info[2] == {"workspaces": ["ws1", "ws2", "ws3"]}

    def test_enrichment_flags_in_getinfo(self, monkeypatch):
        fake = _FakeApi(statuses=["Succeeded"])
        monkeypatch.setattr(scanner, "_call_powerbi_api", fake)

        scanner.scan_workspaces("tok", ["ws1"], poll_interval=0)

        get_info = next(
            c for c in fake.calls if c[0].startswith("/admin/workspaces/getInfo")
        )
        for flag in ("datasetSchema=true", "datasetExpressions=true", "lineage=true"):
            assert flag in get_info[0]

    def test_failed_scan_returns_none(self, monkeypatch):
        fake = _FakeApi(statuses=["Failed"])
        monkeypatch.setattr(scanner, "_call_powerbi_api", fake)

        assert scanner.scan_workspaces("tok", ["ws1"], poll_interval=0) is None

    def test_scan_strips_user_emails_keeps_names(self, monkeypatch):
        scan_result = {
            "workspaces": [
                {
                    "name": "BI Marketing",
                    "users": [
                        {
                            "displayName": "Denis Goryunov",
                            "emailAddress": "denis.goryunov@example.com",
                            "identifier": "denis.goryunov@example.com",
                            "graphId": "ca4a8945-293d-4bd4-bfd2-0d11e958c082",
                            "principalType": "User",
                        }
                    ],
                    "reports": [
                        {
                            "name": "Org Leads",
                            "users": [
                                {
                                    "displayName": "Jane Roe",
                                    "emailAddress": "jane.roe@example.com",
                                    "identifier": "jane.roe@example.com",
                                }
                            ],
                        }
                    ],
                }
            ]
        }
        fake = _FakeApi(statuses=["Succeeded"], scan_result=scan_result)
        monkeypatch.setattr(scanner, "_call_powerbi_api", fake)

        result = scanner.scan_workspaces("tok", ["ws1"], poll_interval=0)

        ws_user = result["workspaces"][0]["users"][0]
        assert ws_user["displayName"] == "Denis Goryunov"
        assert ws_user["graphId"] == "ca4a8945-293d-4bd4-bfd2-0d11e958c082"
        assert "emailAddress" not in ws_user
        assert "identifier" not in ws_user
        # nested users (under reports/dashboards/datasets) are scrubbed too
        report_user = result["workspaces"][0]["reports"][0]["users"][0]
        assert report_user["displayName"] == "Jane Roe"
        assert "emailAddress" not in report_user
        assert "identifier" not in report_user


# ============================================================================
# Command orchestration layer
# ============================================================================


CMD_MANIFEST = {
    "nodes": {
        "model.proj.client_info": {
            "resource_type": "model",
            "name": "client_info",
            "database": "my-project",
            "schema": "core_client",
            "config": {"alias": "client_info"},
        }
    },
    "sources": {},
}


def _cmd_write(path, obj):
    path.write_bytes(orjson.dumps(obj))
    return str(path)


def _cmd_scan_result():
    return {
        "workspaces": [
            {
                "name": "BI Marketing",
                "datasets": [
                    {
                        "id": "ds1",
                        "name": "Clients DS",
                        "tables": [
                            {
                                "name": "clients",
                                "source": [{"expression": fx.NATIVE_SINGLE}],
                                "measures": [{"name": "Total", "expression": "x"}],
                                "columns": [],
                            }
                        ],
                    }
                ],
                "reports": [{"name": "Clients Report", "datasetId": "ds1"}],
            }
        ]
    }


class TestCommandBuild:
    def test_build_writes_index_and_reports_counts(self, tmp_path):
        raw = _cmd_write(tmp_path / "raw.json", _cmd_scan_result())
        man = _cmd_write(tmp_path / "manifest.json", CMD_MANIFEST)
        out = tmp_path / "index.json"

        result = cmd.build_index_artifact(raw, man, str(out))

        assert out.exists()
        assert result["reports"] == 1
        loaded = orjson.loads(out.read_bytes())
        assert loaded["reports"][0]["report"] == "Clients Report"


class TestCommandFind:
    def test_find_returns_matching_report_and_tables(self, tmp_path):
        raw = _cmd_write(tmp_path / "raw.json", _cmd_scan_result())
        man = _cmd_write(tmp_path / "manifest.json", CMD_MANIFEST)
        out = tmp_path / "index.json"
        cmd.build_index_artifact(raw, man, str(out))

        result = cmd.find_in_index(str(out), "clients")

        assert any(r["report"] == "Clients Report" for r in result["reports"])
        assert any(
            t["bq"] == "my-project.core_client.client_info"
            for r in result["reports"]
            for t in r["tables"]
        )

    def test_find_metric(self, tmp_path):
        raw = _cmd_write(tmp_path / "raw.json", _cmd_scan_result())
        man = _cmd_write(tmp_path / "manifest.json", CMD_MANIFEST)
        out = tmp_path / "index.json"
        cmd.build_index_artifact(raw, man, str(out))

        result = cmd.find_in_index(str(out), "total")

        assert "Total" in result["metrics"]


class TestCommandShow:
    def test_show_returns_breakdown(self, tmp_path):
        raw = _cmd_write(tmp_path / "raw.json", _cmd_scan_result())
        man = _cmd_write(tmp_path / "manifest.json", CMD_MANIFEST)
        out = tmp_path / "index.json"
        cmd.build_index_artifact(raw, man, str(out))

        result = cmd.show_report(str(out), "Clients Report")

        assert result["report"] == "Clients Report"
        assert result["dataset"] == "Clients DS"

    def test_show_unknown_raises(self, tmp_path):
        raw = _cmd_write(tmp_path / "raw.json", _cmd_scan_result())
        man = _cmd_write(tmp_path / "manifest.json", CMD_MANIFEST)
        out = tmp_path / "index.json"
        cmd.build_index_artifact(raw, man, str(out))

        with pytest.raises(DbtMetaError):
            cmd.show_report(str(out), "Nonexistent")


class TestCommandScan:
    def test_scan_writes_raw_and_summary(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cmd, "get_powerbi_token", lambda *a, **k: "tok")
        monkeypatch.setattr(cmd, "scan_workspaces", lambda *a, **k: _cmd_scan_result())
        out = tmp_path / "raw.json"
        config = Config()
        config.powerbi_tenant_id = "t"
        config.powerbi_client_id = "c"
        config.powerbi_client_secret = "s"
        config.powerbi_workspaces = ["ws1"]

        result = cmd.scan_command(config, str(out))

        assert out.exists()
        assert result["workspaces"] == 1
        assert result["datasets"] == 1

    def test_scan_without_credentials_raises(self, tmp_path):
        config = Config()
        config.powerbi_workspaces = ["ws1"]

        with pytest.raises(DbtMetaError):
            cmd.scan_command(config, str(tmp_path / "raw.json"))

    def test_scan_api_failure_raises(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cmd, "get_powerbi_token", lambda *a, **k: "tok")
        monkeypatch.setattr(cmd, "scan_workspaces", lambda *a, **k: None)
        config = Config()
        config.powerbi_tenant_id = "t"
        config.powerbi_client_id = "c"
        config.powerbi_client_secret = "s"
        config.powerbi_workspaces = ["ws1"]

        with pytest.raises(DbtMetaError):
            cmd.scan_command(config, str(tmp_path / "raw.json"))
