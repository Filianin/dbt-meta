"""Behavior tests for the Power BI metadata pipeline.

Organized by module — M-expression parsing, SQL analysis, cross-query
resolution, dbt classification, index build, artifact persistence, querying,
the Scanner API client, and the command orchestration layer. All tests exercise
public interfaces only.
"""

import json

import orjson
import pytest

import dbt_meta.command_impl.powerbi as cmd
import dbt_meta.powerbi.scanner as scanner
from dbt_meta.config import Config
from dbt_meta.errors import DbtMetaError
from dbt_meta.powerbi.artifact import (
    artifact_age_hours,
    find_powerbi_artifact,
    find_powerbi_raw,
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
        assert loaded.schema_version == "1.2"

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

    def test_empty_workspaces_returns_none(self):
        assert scanner.scan_workspaces("tok", [], poll_interval=0) is None

    def test_getinfo_without_id_returns_none(self, monkeypatch):
        def fake(token, endpoint, method="GET", data=None, timeout=30):
            if endpoint.startswith("/admin/workspaces/getInfo"):
                return {}  # no "id"
            return None

        monkeypatch.setattr(scanner, "_call_powerbi_api", fake)

        assert scanner.scan_workspaces("tok", ["ws1"], poll_interval=0) is None

    def test_status_poll_none_returns_none(self, monkeypatch):
        def fake(token, endpoint, method="GET", data=None, timeout=30):
            if endpoint.startswith("/admin/workspaces/getInfo"):
                return {"id": "scan-123"}
            if endpoint.startswith("/admin/workspaces/scanStatus"):
                return None  # status call fails mid-poll
            return None

        monkeypatch.setattr(scanner, "_call_powerbi_api", fake)

        assert scanner.scan_workspaces("tok", ["ws1"], poll_interval=0) is None

    def test_poll_exhausted_returns_none(self, monkeypatch):
        slept = []
        monkeypatch.setattr(scanner.time, "sleep", lambda s: slept.append(s))
        fake = _FakeApi(statuses=["Running", "Running"])
        monkeypatch.setattr(scanner, "_call_powerbi_api", fake)

        result = scanner.scan_workspaces(
            "tok", ["ws1"], poll_interval=1, max_polls=2
        )

        assert result is None
        assert slept == [1, 1]  # sleep happened on each non-terminal poll

    def test_result_without_workspaces_returns_none(self, monkeypatch):
        fake = _FakeApi(statuses=["Succeeded"], scan_result={"no": "workspaces"})
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
                            "modifiedBy": "bob@example.com",
                            "createdBy": "carol@example.com",
                            "configuredBy": "dave@example.com",
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
        # scalar UPN/email fields on the artifact are dropped entirely
        report = result["workspaces"][0]["reports"][0]
        assert "modifiedBy" not in report
        assert "createdBy" not in report
        assert "configuredBy" not in report


# ============================================================================
# API transport — secrets must never reach process argv
# ============================================================================


class _CapturedRun:
    """Stand-in for subprocess.run that records argv + stdin and returns stdout."""

    def __init__(self, stdout):
        self.stdout = stdout
        self.cmd = None
        self.input = None

    def __call__(self, cmd, *, input=None, capture_output=None, text=None, timeout=None):
        self.cmd = cmd
        self.input = input

        class _Result:
            returncode = 0

        r = _Result()
        r.stdout = self.stdout
        return r


class TestApiTransport:
    def test_token_secret_not_in_argv(self, monkeypatch):
        from dbt_meta.utils import powerbi as pbi_utils

        run = _CapturedRun('{"access_token": "abc"}')
        monkeypatch.setattr(pbi_utils.subprocess, "run", run)
        monkeypatch.setattr(pbi_utils.shutil, "which", lambda _: "/usr/bin/curl")

        token = pbi_utils.get_powerbi_token("tenant", "client", "s3cret-value")

        assert token == "abc"
        assert "s3cret-value" not in " ".join(run.cmd)
        assert "s3cret-value" in run.input
        assert "@-" in run.cmd

    def test_bearer_token_not_in_argv(self, monkeypatch):
        from dbt_meta.utils import powerbi as pbi_utils

        run = _CapturedRun('{"ok": true}')
        monkeypatch.setattr(pbi_utils.subprocess, "run", run)
        monkeypatch.setattr(pbi_utils.shutil, "which", lambda _: "/usr/bin/curl")

        result = pbi_utils._call_powerbi_api("jwt-token-xyz", "/admin/x")

        assert result == {"ok": True}
        assert "jwt-token-xyz" not in " ".join(run.cmd)
        assert "jwt-token-xyz" in run.input
        assert run.cmd[run.cmd.index("-K") + 1] == "-"

    def test_token_none_when_curl_missing(self, monkeypatch):
        from dbt_meta.utils import powerbi as pbi_utils

        monkeypatch.setattr(pbi_utils.shutil, "which", lambda _: None)

        assert pbi_utils.get_powerbi_token("t", "c", "s") is None

    def test_token_none_on_nonzero_exit(self, monkeypatch):
        from dbt_meta.utils import powerbi as pbi_utils

        def run(cmd, **kw):
            class _R:
                returncode = 1
                stdout = ""
            return _R()

        monkeypatch.setattr(pbi_utils.shutil, "which", lambda _: "/usr/bin/curl")
        monkeypatch.setattr(pbi_utils.subprocess, "run", run)

        assert pbi_utils.get_powerbi_token("t", "c", "s") is None

    def test_token_none_on_timeout(self, monkeypatch):
        import subprocess

        from dbt_meta.utils import powerbi as pbi_utils

        def run(cmd, **kw):
            raise subprocess.TimeoutExpired(cmd, 30)

        monkeypatch.setattr(pbi_utils.shutil, "which", lambda _: "/usr/bin/curl")
        monkeypatch.setattr(pbi_utils.subprocess, "run", run)

        assert pbi_utils.get_powerbi_token("t", "c", "s") is None

    def test_api_none_when_curl_missing(self, monkeypatch):
        from dbt_meta.utils import powerbi as pbi_utils

        monkeypatch.setattr(pbi_utils.shutil, "which", lambda _: None)

        assert pbi_utils._call_powerbi_api("tok", "/admin/x") is None

    def test_api_none_on_nonzero_exit(self, monkeypatch):
        from dbt_meta.utils import powerbi as pbi_utils

        def run(cmd, **kw):
            class _R:
                returncode = 1
                stdout = ""
            return _R()

        monkeypatch.setattr(pbi_utils.shutil, "which", lambda _: "/usr/bin/curl")
        monkeypatch.setattr(pbi_utils.subprocess, "run", run)

        assert pbi_utils._call_powerbi_api("tok", "/admin/x") is None

    def test_api_empty_stdout_returns_empty_dict(self, monkeypatch):
        from dbt_meta.utils import powerbi as pbi_utils

        run = _CapturedRun("   ")
        monkeypatch.setattr(pbi_utils.shutil, "which", lambda _: "/usr/bin/curl")
        monkeypatch.setattr(pbi_utils.subprocess, "run", run)

        assert pbi_utils._call_powerbi_api("tok", "/admin/x") == {}

    def test_api_none_on_malformed_json(self, monkeypatch):
        from dbt_meta.utils import powerbi as pbi_utils

        run = _CapturedRun("not json{")
        monkeypatch.setattr(pbi_utils.shutil, "which", lambda _: "/usr/bin/curl")
        monkeypatch.setattr(pbi_utils.subprocess, "run", run)

        assert pbi_utils._call_powerbi_api("tok", "/admin/x") is None

    def test_api_post_data_passed_as_argument(self, monkeypatch):
        from dbt_meta.utils import powerbi as pbi_utils

        run = _CapturedRun('{"ok": true}')
        monkeypatch.setattr(pbi_utils.shutil, "which", lambda _: "/usr/bin/curl")
        monkeypatch.setattr(pbi_utils.subprocess, "run", run)

        pbi_utils._call_powerbi_api("tok", "/admin/x", method="POST", data={"a": 1})

        assert "-d" in run.cmd
        assert '{"a": 1}' in run.cmd


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
                "id": "ws1",
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
                "reports": [
                    {"id": "rpt1", "name": "Clients Report", "datasetId": "ds1"}
                ],
            }
        ]
    }


class TestCommandArtifacts:
    def test_artifacts_writes_both_files_and_returns_summary(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cmd, "get_powerbi_token", lambda *a, **k: "tok")
        monkeypatch.setattr(cmd, "scan_workspaces", lambda *a, **k: _cmd_scan_result())
        man = _cmd_write(tmp_path / "manifest.json", CMD_MANIFEST)
        raw_out = tmp_path / "powerbi_raw.json"
        idx_out = tmp_path / "powerbi_index.json"
        config = Config()
        config.powerbi_tenant_id = "t"
        config.powerbi_client_id = "c"
        config.powerbi_client_secret = "s"
        config.powerbi_workspaces = ["ws1"]

        result = cmd.artifacts_cmd(
            config, man, str(raw_out), str(idx_out), with_layouts=False
        )

        assert raw_out.exists()
        assert idx_out.exists()
        assert result["reports"] == 1
        assert result["raw_path"] == str(raw_out)
        assert result["index_path"] == str(idx_out)
        assert result["layouts"] == {}

    def test_artifacts_with_layouts_attaches_pages(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cmd, "get_powerbi_token", lambda *a, **k: "tok")
        monkeypatch.setattr(cmd, "get_fabric_token", lambda *a, **k: "fabtok")
        monkeypatch.setattr(cmd, "scan_workspaces", lambda *a, **k: _cmd_scan_result())
        monkeypatch.setattr(
            cmd, "get_report_definition", lambda *a, **k: fx.PBIR_LEGACY_REPORT
        )
        man = _cmd_write(tmp_path / "manifest.json", CMD_MANIFEST)
        raw_out = tmp_path / "powerbi_raw.json"
        idx_out = tmp_path / "powerbi_index.json"
        config = Config()
        config.powerbi_tenant_id = "t"
        config.powerbi_client_id = "c"
        config.powerbi_client_secret = "s"
        config.powerbi_workspaces = ["ws1"]

        result = cmd.artifacts_cmd(config, man, str(raw_out), str(idx_out))

        assert result["layouts"] == {"with_layout": 1, "total": 1}
        saved = load_index(str(idx_out))
        assert saved.reports[0].pages[0].name == "PPC Reg Cohorts"

    def test_artifacts_layouts_skipped_when_no_fabric_token(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cmd, "get_powerbi_token", lambda *a, **k: "tok")
        monkeypatch.setattr(cmd, "get_fabric_token", lambda *a, **k: None)
        monkeypatch.setattr(cmd, "scan_workspaces", lambda *a, **k: _cmd_scan_result())
        man = _cmd_write(tmp_path / "manifest.json", CMD_MANIFEST)
        config = Config()
        config.powerbi_tenant_id = "t"
        config.powerbi_client_id = "c"
        config.powerbi_client_secret = "s"
        config.powerbi_workspaces = ["ws1"]

        result = cmd.artifacts_cmd(
            config, man,
            str(tmp_path / "raw.json"), str(tmp_path / "idx.json"),
        )

        assert result["layouts"] == {}

    def test_artifacts_scan_failure_raises(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cmd, "get_powerbi_token", lambda *a, **k: "tok")
        monkeypatch.setattr(cmd, "scan_workspaces", lambda *a, **k: None)
        man = _cmd_write(tmp_path / "manifest.json", CMD_MANIFEST)
        config = Config()
        config.powerbi_tenant_id = "t"
        config.powerbi_client_id = "c"
        config.powerbi_client_secret = "s"
        config.powerbi_workspaces = ["ws1"]

        with pytest.raises(DbtMetaError):
            cmd.artifacts_cmd(
                config, man,
                str(tmp_path / "raw.json"),
                str(tmp_path / "idx.json"),
            )


class TestDatasetMeasures:
    def test_measure_without_name_is_skipped(self):
        scan = {
            "workspaces": [
                {
                    "id": "ws1",
                    "datasets": [
                        {
                            "id": "ds1",
                            "tables": [
                                {
                                    "name": "t",
                                    "measures": [
                                        {"expression": "x"},  # no name → skipped
                                        {"name": "Revenue"},
                                    ],
                                }
                            ],
                        }
                    ],
                }
            ]
        }

        assert cmd._dataset_measures(scan) == {"ds1": {"Revenue"}}


class TestEnrichWithLayouts:
    """Failure isolation of the layout pass — one bad report never aborts it."""

    def test_none_definition_leaves_pages_empty(self, monkeypatch):
        scan = _cmd_scan_result()
        index = build_index(scan, CMD_MANIFEST)
        monkeypatch.setattr(cmd, "get_report_definition", lambda *a, **k: None)

        got, total = cmd.enrich_with_layouts(index, scan, "fabtok")

        assert (got, total) == (0, 1)
        assert index.reports[0].pages == []

    def test_empty_report_id_is_skipped(self, monkeypatch):
        scan = _cmd_scan_result()
        scan["workspaces"][0]["reports"][0]["id"] = ""
        index = build_index(scan, CMD_MANIFEST)
        called = []
        monkeypatch.setattr(
            cmd,
            "get_report_definition",
            lambda *a, **k: called.append(a) or fx.PBIR_LEGACY_REPORT,
        )

        got, total = cmd.enrich_with_layouts(index, scan, "fabtok")

        assert (got, total) == (0, 1)
        assert called == []

    def test_definition_with_no_pages_is_not_counted(self, monkeypatch):
        scan = _cmd_scan_result()
        index = build_index(scan, CMD_MANIFEST)
        monkeypatch.setattr(
            cmd, "get_report_definition", lambda *a, **k: {"sections": []}
        )

        got, total = cmd.enrich_with_layouts(index, scan, "fabtok")

        assert (got, total) == (0, 1)
        assert index.reports[0].pages == []

    def test_successful_definition_attaches_and_counts(self, monkeypatch):
        scan = _cmd_scan_result()
        index = build_index(scan, CMD_MANIFEST)
        monkeypatch.setattr(
            cmd, "get_report_definition", lambda *a, **k: fx.PBIR_LEGACY_REPORT
        )

        got, total = cmd.enrich_with_layouts(index, scan, "fabtok")

        assert (got, total) == (1, 1)
        assert index.reports[0].pages[0].name == "PPC Reg Cohorts"


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


class TestCommandList:
    def test_lists_all_reports_sorted(self, tmp_path):
        index = PowerBiIndex(
            reports=[
                ReportEntry(
                    workspace="BI Sales",
                    report="Zebra",
                    dataset="DS2",
                    tables=[TableRef(bq="p.s.t2", status="model", dbt_model="t2")],
                ),
                ReportEntry(
                    workspace="BI Marketing",
                    report="Alpha",
                    dataset="DS1",
                    tables=[TableRef(bq="p.s.t1", status="model", dbt_model="t1")],
                ),
            ]
        )
        path = tmp_path / "index.json"
        save_index(index, str(path))

        result = cmd.list_cmd(str(path))

        assert result["count"] == 2
        assert [(r["workspace"], r["report"]) for r in result["reports"]] == [
            ("BI Marketing", "Alpha"),
            ("BI Sales", "Zebra"),
        ]
        assert result["reports"][0]["tables"][0]["bq"] == "p.s.t1"

    def test_empty_index_returns_zero(self, tmp_path):
        path = tmp_path / "index.json"
        save_index(PowerBiIndex(reports=[]), str(path))

        result = cmd.list_cmd(str(path))

        assert result["count"] == 0
        assert result["reports"] == []


class TestSqlInIndex:
    def test_show_sql_analysis_contains_sql_text(self, tmp_path):
        raw = _cmd_write(tmp_path / "raw.json", _cmd_scan_result())
        man = _cmd_write(tmp_path / "manifest.json", CMD_MANIFEST)
        out = tmp_path / "index.json"
        cmd.build_index_artifact(raw, man, str(out))

        result = cmd.show_report(str(out), "Clients Report")

        sql_entries = result.get("sql_analysis", [])
        assert len(sql_entries) == 1
        assert "SELECT" in sql_entries[0]["sql"]
        assert "client_info" in sql_entries[0]["sql"]


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

    def test_scan_missing_workspaces_raises(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cmd, "get_powerbi_token", lambda *a, **k: "tok")
        config = Config()
        config.powerbi_tenant_id = "t"
        config.powerbi_client_id = "c"
        config.powerbi_client_secret = "s"
        config.powerbi_workspaces = []

        with pytest.raises(DbtMetaError, match="workspaces"):
            cmd.scan_command(config, str(tmp_path / "raw.json"))

    def test_scan_token_none_raises(self, tmp_path, monkeypatch):
        monkeypatch.setattr(cmd, "get_powerbi_token", lambda *a, **k: None)
        config = Config()
        config.powerbi_tenant_id = "t"
        config.powerbi_client_id = "c"
        config.powerbi_client_secret = "s"
        config.powerbi_workspaces = ["ws1"]

        with pytest.raises(DbtMetaError, match="token"):
            cmd.scan_command(config, str(tmp_path / "raw.json"))


# ============================================================================
# TestCommandBuild — _load_json error branches
# ============================================================================


class TestCommandBuildErrors:
    def test_build_file_not_found_raises(self, tmp_path):
        with pytest.raises(DbtMetaError, match="not found"):
            cmd.build_index_artifact(
                str(tmp_path / "nonexistent.json"),
                str(tmp_path / "man.json"),
                str(tmp_path / "out.json"),
            )

    def test_build_bad_json_raises(self, tmp_path):
        bad = tmp_path / "bad.json"
        bad.write_bytes(b"bad")
        man = tmp_path / "manifest.json"
        man.write_bytes(b"{}")

        with pytest.raises(DbtMetaError, match="Invalid JSON"):
            cmd.build_index_artifact(str(bad), str(man), str(tmp_path / "out.json"))


# ============================================================================
# TestRawReader — _load_raw and _find_report error branches
# ============================================================================


def _raw_with_reports(*reports, dataset_id="ds1"):
    """Build a minimal raw artifact dict with the given report dicts."""
    return {
        "workspaces": [
            {
                "name": "BI WS",
                "datasets": [
                    {"id": dataset_id, "name": "DS", "tables": []}
                ],
                "reports": list(reports),
            }
        ]
    }


class TestRawReader:
    def test_load_raw_file_not_found_raises(self, tmp_path):
        with pytest.raises(DbtMetaError, match="not found"):
            cmd.measures_cmd(str(tmp_path / "nonexistent.json"), "Any")

    def test_load_raw_bad_json_raises(self, tmp_path):
        bad = tmp_path / "bad.json"
        bad.write_bytes(b"not-json")

        with pytest.raises(DbtMetaError, match="Invalid JSON"):
            cmd.measures_cmd(str(bad), "Any")

    def test_exact_ambiguity_raises(self, tmp_path):
        raw_data = _raw_with_reports(
            {"name": "Exact Name", "datasetId": "ds1"},
            {"name": "Exact Name", "datasetId": "ds1"},
        )
        raw_path = _cmd_write(tmp_path / "raw.json", raw_data)

        with pytest.raises(DbtMetaError, match="Ambiguous"):
            cmd.owners_cmd(raw_path, "Exact Name")

    def test_partial_ambiguity_raises(self, tmp_path):
        raw_data = _raw_with_reports(
            {"name": "Alpha partial report", "datasetId": "ds1"},
            {"name": "Beta partial report", "datasetId": "ds1"},
        )
        raw_path = _cmd_write(tmp_path / "raw.json", raw_data)

        with pytest.raises(DbtMetaError, match="Ambiguous"):
            cmd.owners_cmd(raw_path, "partial")

    def test_dataset_not_found_returns_empty_measures(self, tmp_path):
        raw_data = _raw_with_reports(
            {"name": "My Report", "datasetId": "nonexistent"},
        )
        raw_path = _cmd_write(tmp_path / "raw.json", raw_data)

        result = cmd.measures_cmd(raw_path, "My Report")

        assert result["report"] == "My Report"
        assert result["measures"] == []

    def test_single_partial_match_returns_report(self, tmp_path):
        raw_data = _raw_with_reports({"name": "Alpha unique report", "datasetId": "ds1"})
        raw_path = _cmd_write(tmp_path / "raw.json", raw_data)

        result = cmd.measures_cmd(raw_path, "unique")

        assert result["report"] == "Alpha unique report"

    def test_report_not_found_raises(self, tmp_path):
        raw_data = _raw_with_reports({"name": "Some Report", "datasetId": "ds1"})
        raw_path = _cmd_write(tmp_path / "raw.json", raw_data)

        with pytest.raises(DbtMetaError, match="not found"):
            cmd.measures_cmd(raw_path, "nonexistent report xyz")

    def test_measures_with_dataset_and_tables(self, tmp_path):
        raw_data = {
            "workspaces": [
                {
                    "name": "BI WS",
                    "datasets": [
                        {
                            "id": "ds1",
                            "name": "DS",
                            "tables": [
                                {
                                    "name": "sales",
                                    "measures": [
                                        {
                                            "name": "Revenue",
                                            "expression": "SUM(sales[amount])",
                                            "isHidden": False,
                                        }
                                    ],
                                    "source": [],
                                    "columns": [],
                                }
                            ],
                        }
                    ],
                    "reports": [{"name": "Sales Report", "datasetId": "ds1"}],
                }
            ]
        }
        raw_path = _cmd_write(tmp_path / "raw.json", raw_data)

        result = cmd.measures_cmd(raw_path, "Sales Report")

        assert result["report"] == "Sales Report"
        assert len(result["measures"]) == 1
        assert result["measures"][0]["name"] == "Revenue"
        assert result["measures"][0]["table"] == "sales"

    def test_source_cmd_returns_expressions(self, tmp_path):
        native_expr = (
            "let\n    Source = GoogleBigQuery.Database(),\n"
            '    p = Source{[Name="p"]}[Data],\n'
            '    s = p{[Name="s",Kind="Schema"]}[Data],\n'
            '    t = s{[Name="tbl",Kind="Table"]}[Data]\nin\n    t'
        )
        raw_data = {
            "workspaces": [
                {
                    "name": "BI WS",
                    "datasets": [
                        {
                            "id": "ds1",
                            "name": "DS",
                            "tables": [
                                {
                                    "name": "tbl",
                                    "source": [{"expression": native_expr}],
                                    "measures": [],
                                    "columns": [],
                                }
                            ],
                        }
                    ],
                    "reports": [{"name": "Source Report", "datasetId": "ds1"}],
                }
            ]
        }
        raw_path = _cmd_write(tmp_path / "raw.json", raw_data)

        result = cmd.source_cmd(raw_path, "Source Report")

        assert result["report"] == "Source Report"
        assert len(result["sources"]) == 1
        assert result["sources"][0]["table"] == "tbl"

    def test_owners_cmd_returns_owners(self, tmp_path):
        raw_data = {
            "workspaces": [
                {
                    "name": "BI WS",
                    "datasets": [],
                    "reports": [
                        {
                            "name": "Owners Report",
                            "datasetId": "ds1",
                            "modifiedBy": "alice@example.com",
                            "modifiedDateTime": "2026-01-01T00:00:00",
                            "users": [
                                {
                                    "displayName": "Alice",
                                    "reportUserAccessRight": "Owner",
                                },
                                {
                                    "displayName": "Bob",
                                    "reportUserAccessRight": "Read",
                                },
                            ],
                        }
                    ],
                }
            ]
        }
        raw_path = _cmd_write(tmp_path / "raw.json", raw_data)

        result = cmd.owners_cmd(raw_path, "Owners Report")

        assert result["report"] == "Owners Report"
        assert result["owners"] == ["Alice"]
        assert result["modified_by"] == "alice@example.com"


# ============================================================================
# TestArtifact — find_powerbi_raw and find_powerbi_artifact missing branches
# ============================================================================


class TestArtifactMissingBranches:
    def test_find_raw_explicit_path_not_exists(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            find_powerbi_raw(explicit_path=str(tmp_path / "nonexistent.json"))

    def test_find_raw_no_path_anywhere_raises(self, tmp_path, monkeypatch):
        monkeypatch.delenv("DBT_PROD_POWERBI_RAW_PATH", raising=False)
        monkeypatch.setattr(
            "dbt_meta.powerbi.artifact.Path.home", lambda: tmp_path / "nohome"
        )

        with pytest.raises(FileNotFoundError):
            find_powerbi_raw()

    def test_find_raw_env_set_but_file_missing(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DBT_PROD_POWERBI_RAW_PATH", str(tmp_path / "missing.json"))

        with pytest.raises(FileNotFoundError):
            find_powerbi_raw()

    def test_find_raw_default_home_path_exists(self, tmp_path, monkeypatch):
        fake_home = tmp_path / "home"
        fake_home.mkdir()
        dbt_state = fake_home / "dbt-state"
        dbt_state.mkdir()
        raw_file = dbt_state / "powerbi_raw.json"
        raw_file.write_bytes(b"{}")
        monkeypatch.delenv("DBT_PROD_POWERBI_RAW_PATH", raising=False)
        monkeypatch.setattr("dbt_meta.powerbi.artifact.Path.home", lambda: fake_home)

        result = find_powerbi_raw()

        assert result == str(raw_file.absolute())

    def test_find_artifact_explicit_path_not_exists(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            find_powerbi_artifact(explicit_path=str(tmp_path / "nonexistent.json"))

    def test_find_artifact_env_set_but_file_missing(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DBT_PROD_POWERBI_PATH", str(tmp_path / "missing.json"))

        with pytest.raises(FileNotFoundError):
            find_powerbi_artifact()

    def test_find_artifact_default_home_path_exists(self, tmp_path, monkeypatch):
        fake_home = tmp_path / "home"
        fake_home.mkdir()
        dbt_state = fake_home / "dbt-state"
        dbt_state.mkdir()
        index_file = dbt_state / "powerbi_index.json"
        save_index(_sample_index(), str(index_file))
        monkeypatch.delenv("DBT_PROD_POWERBI_PATH", raising=False)
        monkeypatch.setattr("dbt_meta.powerbi.artifact.Path.home", lambda: fake_home)

        result = find_powerbi_artifact()

        assert result == str(index_file.absolute())


# ============================================================================
# TestQueryFind — match by dataset name
# ============================================================================


class TestQueryFindDataset:
    def test_matches_dataset_name(self):
        # "leads ds" is in dataset="Leads DS" but NOT in report="Organic Leads"
        # and NOT in bq tables ("p.core_client.client_info", "p.raw_amas.events")
        results = find(_query_index(), "leads ds")

        assert len(results.reports) == 1
        assert results.reports[0].report == "Organic Leads"


# ============================================================================
# TestQueryReportsForModel — reports_for_model coverage
# ============================================================================


class TestQueryReportsForModel:
    def test_finds_reports_by_dbt_model(self):
        from dbt_meta.powerbi.query import reports_for_model

        results = reports_for_model(_query_index(), "client_info")

        assert len(results) == 1
        report, matched = results[0]
        assert report.report == "Organic Leads"
        assert "p.core_client.client_info" in matched

    def test_no_match_returns_empty(self):
        from dbt_meta.powerbi.query import reports_for_model

        assert reports_for_model(_query_index(), "nonexistent_xyz") == []

    def test_case_insensitive(self):
        from dbt_meta.powerbi.query import reports_for_model

        results = reports_for_model(_query_index(), "CLIENT_INFO")

        assert len(results) == 1
        assert results[0][0].report == "Organic Leads"

    def test_external_tables_not_matched(self):
        from dbt_meta.powerbi.query import reports_for_model

        # "p.raw_amas.events" is external (no dbt_model), should not match "events"
        results = reports_for_model(_query_index(), "events")

        assert results == []


# ============================================================================
# TestFindRawExisting — find_powerbi_raw success branches
# ============================================================================


class TestFindRawExisting:
    def test_find_raw_explicit_path_exists(self, tmp_path):
        raw = tmp_path / "powerbi_raw.json"
        raw.write_bytes(b"{}")

        assert find_powerbi_raw(explicit_path=str(raw)) == str(raw.absolute())

    def test_find_raw_env_path_exists(self, tmp_path, monkeypatch):
        raw = tmp_path / "powerbi_raw.json"
        raw.write_bytes(b"{}")
        monkeypatch.setenv("DBT_PROD_POWERBI_RAW_PATH", str(raw))
        monkeypatch.delenv("DBT_PROD_POWERBI_PATH", raising=False)

        assert find_powerbi_raw() == str(raw.absolute())


# ============================================================================
# TestCommandReports — reports_for_model_cmd coverage
# ============================================================================

CMD_MANIFEST_MULTI = {
    "nodes": {
        "model.proj.core_alpha": {
            "resource_type": "model",
            "name": "core_alpha",
            "database": "my-project",
            "schema": "s",
            "config": {"alias": "core_alpha"},
        },
        "model.proj.core_beta": {
            "resource_type": "model",
            "name": "core_beta",
            "database": "my-project",
            "schema": "s",
            "config": {"alias": "core_beta"},
        },
    },
    "sources": {},
}

_NATIVE_CORE_ALPHA = (
    'let\n    Source = Value.NativeQuery(GoogleBigQuery.Database()'
    '{[Name="my-project"]}[Data], "SELECT id FROM `my-project.s.core_alpha`",'
    " null, [EnableFolding=true])\nin\n    Source"
)

_NATIVE_CORE_BETA = (
    'let\n    Source = Value.NativeQuery(GoogleBigQuery.Database()'
    '{[Name="my-project"]}[Data], "SELECT id FROM `my-project.s.core_beta`",'
    " null, [EnableFolding=true])\nin\n    Source"
)


def _cmd_scan_result_ambiguous():
    return {
        "workspaces": [
            {
                "name": "BI WS",
                "datasets": [
                    {
                        "id": "ds1",
                        "name": "Alpha DS",
                        "tables": [
                            {
                                "name": "alpha",
                                "source": [{"expression": _NATIVE_CORE_ALPHA}],
                                "measures": [],
                                "columns": [],
                            }
                        ],
                    },
                    {
                        "id": "ds2",
                        "name": "Beta DS",
                        "tables": [
                            {
                                "name": "beta",
                                "source": [{"expression": _NATIVE_CORE_BETA}],
                                "measures": [],
                                "columns": [],
                            }
                        ],
                    },
                ],
                "reports": [
                    {"name": "Alpha Report", "datasetId": "ds1"},
                    {"name": "Beta Report", "datasetId": "ds2"},
                ],
            }
        ]
    }


class TestCommandReports:
    def test_returns_reports_using_model(self, tmp_path):
        raw = _cmd_write(tmp_path / "raw.json", _cmd_scan_result())
        man = _cmd_write(tmp_path / "manifest.json", CMD_MANIFEST)
        out = tmp_path / "index.json"
        cmd.build_index_artifact(raw, man, str(out))

        result = cmd.reports_for_model_cmd(str(out), "client_info")

        assert result["model"] == "client_info"
        assert len(result["reports"]) == 1
        assert result["reports"][0]["report"] == "Clients Report"
        assert "my-project.core_client.client_info" in result["reports"][0]["matched_tables"]

    def test_no_match_returns_empty_list(self, tmp_path):
        raw = _cmd_write(tmp_path / "raw.json", _cmd_scan_result())
        man = _cmd_write(tmp_path / "manifest.json", CMD_MANIFEST)
        out = tmp_path / "index.json"
        cmd.build_index_artifact(raw, man, str(out))

        result = cmd.reports_for_model_cmd(str(out), "nonexistent_xyz")

        assert result["reports"] == []

    def test_ambiguous_model_raises(self, tmp_path):
        raw = _cmd_write(tmp_path / "raw.json", _cmd_scan_result_ambiguous())
        man = _cmd_write(tmp_path / "manifest.json", CMD_MANIFEST_MULTI)
        out = tmp_path / "index.json"
        cmd.build_index_artifact(raw, man, str(out))

        with pytest.raises(DbtMetaError, match="core_alpha"):
            cmd.reports_for_model_cmd(str(out), "core")


# ============================================================================
# TestCommandLineage — lineage_cmd
# ============================================================================

def _make_lineage_graph_with(node_ids: list[str], edges: dict[str, list[str]]):
    """Build a minimal LineageGraph stub for lineage_cmd tests."""
    from dbt_meta.lineage.graph import LineageGraph

    g = LineageGraph()
    for nid in node_ids:
        g.add_node(nid)
    for src, targets in edges.items():
        for tgt in targets:
            g.add_edge(src, tgt)
    return g


def _lineage_scan_result():
    """Scan result where the table has a native SQL with a WHERE filter on registration_time."""
    return {
        "workspaces": [
            {
                "name": "BI WS",
                "datasets": [
                    {
                        "id": "ds1",
                        "name": "Clients DS",
                        "tables": [
                            {
                                "name": "clients",
                                "source": [
                                    {
                                        "expression": (
                                            'let\n  Source = Value.NativeQuery('
                                            'GoogleBigQuery.Database(){[Name="my-project"]}[Data],'
                                            ' "SELECT registration_time FROM'
                                            ' `my-project.core_client.client_info`'
                                            " WHERE registration_time >= '2023-01-01'\","
                                            " null, [EnableFolding=true])\nin\n  Source"
                                        )
                                    }
                                ],
                                "measures": [],
                                "columns": [],
                            }
                        ],
                    }
                ],
                "reports": [{"name": "Clients Report", "datasetId": "ds1"}],
            }
        ]
    }


class TestCommandLineage:
    def test_returns_upstream_for_filter_columns(self, tmp_path, monkeypatch):
        raw = _cmd_write(tmp_path / "raw.json", _lineage_scan_result())
        man = _cmd_write(tmp_path / "manifest.json", CMD_MANIFEST)
        idx = tmp_path / "index.json"
        cmd.build_index_artifact(raw, man, str(idx))

        graph = _make_lineage_graph_with(
            node_ids=[
                "client_info.registration_time",
                "raw_registrations.registration_time",
            ],
            edges={
                "raw_registrations.registration_time": ["client_info.registration_time"]
            },
        )
        monkeypatch.setattr(cmd, "_load_lineage_graph", lambda path: graph)

        result = cmd.lineage_cmd(str(idx), "/fake/lineage.json", "Clients Report")

        assert result["report"] == "Clients Report"
        cols = result["columns"]
        assert any(c["bq_column"] == "registration_time" for c in cols)
        match = next(c for c in cols if c["bq_column"] == "registration_time")
        assert "raw_registrations.registration_time" in match["ancestors"]

    def test_unknown_column_skipped_gracefully(self, tmp_path, monkeypatch):
        raw = _cmd_write(tmp_path / "raw.json", _lineage_scan_result())
        man = _cmd_write(tmp_path / "manifest.json", CMD_MANIFEST)
        idx = tmp_path / "index.json"
        cmd.build_index_artifact(raw, man, str(idx))

        graph = _make_lineage_graph_with(node_ids=[], edges={})
        monkeypatch.setattr(cmd, "_load_lineage_graph", lambda path: graph)

        result = cmd.lineage_cmd(str(idx), "/fake/lineage.json", "Clients Report")

        assert result["columns"] == []

    def test_missing_lineage_artifact_raises(self, tmp_path):
        raw = _cmd_write(tmp_path / "raw.json", _lineage_scan_result())
        man = _cmd_write(tmp_path / "manifest.json", CMD_MANIFEST)
        idx = tmp_path / "index.json"
        cmd.build_index_artifact(raw, man, str(idx))

        with pytest.raises(DbtMetaError, match="lineage"):
            cmd.lineage_cmd(str(idx), str(tmp_path / "nonexistent.json"), "Clients Report")

    def test_unknown_report_raises(self, tmp_path):
        raw = _cmd_write(tmp_path / "raw.json", _lineage_scan_result())
        man = _cmd_write(tmp_path / "manifest.json", CMD_MANIFEST)
        idx = tmp_path / "index.json"
        cmd.build_index_artifact(raw, man, str(idx))

        with pytest.raises(DbtMetaError, match="Report not found"):
            cmd.lineage_cmd(str(idx), "/fake/lineage.json", "Nonexistent Report")

    def test_duplicate_column_resolved_once(self, tmp_path, monkeypatch):
        index = PowerBiIndex(
            reports=[
                ReportEntry(
                    workspace="W",
                    report="Dup Report",
                    dataset="D",
                    tables=[TableRef(bq="p.s.t", status="model", dbt_model="m")],
                    sql_analysis=[
                        SqlAnalysisEntry(
                            query="q1",
                            tables=("p.s.t",),
                            filters=("col",),
                            joins=(),
                            group_by=(),
                            parse_status="ok",
                        ),
                        SqlAnalysisEntry(
                            query="q2",
                            tables=("p.s.t",),
                            filters=("col",),
                            joins=(),
                            group_by=(),
                            parse_status="ok",
                        ),
                    ],
                )
            ]
        )
        idx = tmp_path / "index.json"
        save_index(index, str(idx))

        graph = _make_lineage_graph_with(
            node_ids=["m.col", "up.col"], edges={"up.col": ["m.col"]}
        )
        monkeypatch.setattr(cmd, "_load_lineage_graph", lambda path: graph)

        result = cmd.lineage_cmd(str(idx), "/fake/lineage.json", "Dup Report")

        # 'col' appears in two sql_analysis entries → resolved only once
        assert [c["bq_column"] for c in result["columns"]] == ["col"]

    def test_load_lineage_graph_reads_real_artifact(self, tmp_path):
        from dbt_meta.lineage.artifact import save_artifact

        graph = _make_lineage_graph_with(
            node_ids=["m.col", "up.col"], edges={"up.col": ["m.col"]}
        )
        path = tmp_path / "lineage.json"
        save_artifact(graph, str(path))

        loaded = cmd._load_lineage_graph(str(path))

        assert loaded.has_node("m.col")
        assert "up.col" in loaded.ancestors("m.col")


# ============================================================================
# TestCommandCost — cost_cmd
# ============================================================================


class TestCommandCost:
    def test_returns_cost_for_dbt_model_tables(self, tmp_path, monkeypatch):
        raw = _cmd_write(tmp_path / "raw.json", _cmd_scan_result())
        man = _cmd_write(tmp_path / "manifest.json", CMD_MANIFEST)
        out = tmp_path / "index.json"
        cmd.build_index_artifact(raw, man, str(out))
        monkeypatch.setattr(
            cmd, "fetch_model_query_costs",
            lambda **kw: [
                {
                    "dbt_model_name": "client_info",
                    "query_cost_usd": 1.23,
                    "query_count": 42,
                    "bytes_processed": 500_000_000,
                    "cache_hit_ratio": 0.6,
                }
            ],
        )

        result = cmd.cost_cmd(str(out), "Clients Report")

        assert result["report"] == "Clients Report"
        tables = result["tables"]
        assert len(tables) == 1
        assert tables[0]["bq"] == "my-project.core_client.client_info"
        assert tables[0]["query_cost_usd"] == 1.23

    def test_external_tables_have_null_cost(self, tmp_path, monkeypatch):
        raw = _cmd_write(tmp_path / "raw.json", _cmd_scan_result())
        man = _cmd_write(tmp_path / "manifest.json", CMD_MANIFEST)
        out = tmp_path / "index.json"
        cmd.build_index_artifact(raw, man, str(out))
        monkeypatch.setattr(cmd, "fetch_model_query_costs", lambda **kw: [])

        result = cmd.cost_cmd(str(out), "Clients Report")

        assert result["tables"][0]["query_cost_usd"] is None

    def test_unknown_report_raises(self, tmp_path, monkeypatch):
        raw = _cmd_write(tmp_path / "raw.json", _cmd_scan_result())
        man = _cmd_write(tmp_path / "manifest.json", CMD_MANIFEST)
        out = tmp_path / "index.json"
        cmd.build_index_artifact(raw, man, str(out))
        monkeypatch.setattr(cmd, "fetch_model_query_costs", lambda **kw: [])

        with pytest.raises(DbtMetaError):
            cmd.cost_cmd(str(out), "Nonexistent Report")


# ============================================================================
# TestDaxParser — parse_dax_refs
# ============================================================================


class TestDaxParser:
    def test_quoted_table_column(self):
        from dbt_meta.powerbi.dax import parse_dax_refs

        refs = parse_dax_refs("CALCULATE(SUM('Sales Data'[Revenue]))")

        assert {"table": "Sales Data", "column": "Revenue"} in refs

    def test_bare_table_column(self):
        from dbt_meta.powerbi.dax import parse_dax_refs

        refs = parse_dax_refs("CALCULATE(SUM(Sales[Revenue]), FILTER(Sales, Sales[Date] > 0))")

        tables = {r["table"] for r in refs}
        columns = {r["column"] for r in refs}
        assert tables == {"Sales"}
        assert "Revenue" in columns
        assert "Date" in columns

    def test_deduplicates_refs(self):
        from dbt_meta.powerbi.dax import parse_dax_refs

        refs = parse_dax_refs("Sales[Revenue] + Sales[Revenue]")

        assert refs.count({"table": "Sales", "column": "Revenue"}) == 1

    def test_empty_expression_returns_empty(self):
        from dbt_meta.powerbi.dax import parse_dax_refs

        assert parse_dax_refs("") == []

    def test_measures_cmd_includes_dax_refs(self, tmp_path):
        raw_data = _raw_with_reports(
            {
                "name": "Sales Report",
                "datasetId": "ds1",
            },
            dataset_id="ds1",
        )
        raw_data["workspaces"][0]["datasets"][0]["tables"] = [
            {
                "name": "Sales",
                "measures": [
                    {"name": "Total Revenue", "expression": "SUM(Sales[Revenue])"}
                ],
                "columns": [],
                "source": [],
            }
        ]
        raw_path = _cmd_write(tmp_path / "raw.json", raw_data)

        result = cmd.measures_cmd(raw_path, "Sales Report")

        measure = result["measures"][0]
        assert measure["name"] == "Total Revenue"
        assert {"table": "Sales", "column": "Revenue"} in measure["dax_refs"]


# ============================================================================
# TestPbirParser — parse_pbir_legacy
# ============================================================================


class TestPbirParser:
    def test_yields_one_page_per_section_with_display_name(self):
        from dbt_meta.powerbi.pbir_parser import parse_pbir_legacy

        pages = parse_pbir_legacy(fx.PBIR_LEGACY_REPORT)

        assert [p.name for p in pages] == ["PPC Reg Cohorts", "Detail"]

    def test_visual_type_and_role_grouped_fields(self):
        from dbt_meta.powerbi.pbir_parser import parse_pbir_legacy

        pages = parse_pbir_legacy(fx.PBIR_LEGACY_REPORT)
        funnel = pages[0].visuals[0]

        assert funnel.type == "funnel"
        assert set(funnel.fields) == {"values", "axis"}
        assert funnel.fields["axis"][0].column == "stage"

    def test_canonical_roles_unify_across_visual_types(self):
        from dbt_meta.powerbi.pbir_parser import parse_pbir_legacy

        pages = parse_pbir_legacy(fx.PBIR_LEGACY_REPORT)
        area = pages[0].visuals[2]

        # Y -> values, Category -> axis, Series -> legend, Tooltips -> tooltip
        assert set(area.fields) == {"values", "axis", "legend", "tooltip"}
        assert area.fields["legend"][0].table == "Device"

    def test_aggregation_wrapper_is_a_measure(self):
        from dbt_meta.powerbi.pbir_parser import parse_pbir_legacy

        pages = parse_pbir_legacy(fx.PBIR_LEGACY_REPORT)
        funnel_value = pages[0].visuals[0].fields["values"][0]

        assert funnel_value.table == "events"
        assert funnel_value.column == "stage_count"
        assert funnel_value.kind == "measure"

    def test_bare_ref_is_a_column_by_default(self):
        from dbt_meta.powerbi.pbir_parser import parse_pbir_legacy

        pages = parse_pbir_legacy(fx.PBIR_LEGACY_REPORT)
        slicer_field = pages[0].visuals[1].fields["values"][0]

        assert slicer_field.kind == "column"

    def test_bare_ref_named_as_dataset_measure_is_a_measure(self):
        from dbt_meta.powerbi.pbir_parser import parse_pbir_legacy

        pages = parse_pbir_legacy(
            fx.PBIR_LEGACY_REPORT, measures=["total_revenue"]
        )
        card_field = pages[1].visuals[0].fields["Custom Role"][0]

        assert card_field.column == "total_revenue"
        assert card_field.kind == "measure"

    def test_unknown_role_falls_back_to_raw_key(self):
        from dbt_meta.powerbi.pbir_parser import parse_pbir_legacy

        pages = parse_pbir_legacy(fx.PBIR_LEGACY_REPORT)

        assert "Custom Role" in pages[1].visuals[0].fields

    def test_malformed_containers_are_skipped_not_raised(self):
        from dbt_meta.powerbi.pbir_parser import parse_pbir_legacy

        pages = parse_pbir_legacy(fx.PBIR_LEGACY_MESSY)

        # bad json + missing singleVisual + empty container dropped, one good kept
        assert [v.type for v in pages[0].visuals] == ["card"]
        # empty section preserved with zero visuals
        assert pages[1].name == "Empty"
        assert pages[1].visuals == []

    def test_empty_report_yields_no_pages(self):
        from dbt_meta.powerbi.pbir_parser import parse_pbir_legacy

        assert parse_pbir_legacy({}) == []

    def test_bare_query_ref_without_dot_keeps_empty_table(self):
        from dbt_meta.powerbi.pbir_parser import parse_pbir_legacy

        report = {
            "sections": [
                {
                    "displayName": "P",
                    "visualContainers": [
                        {
                            "config": fx._config(
                                "card", {"Values": [{"queryRef": "barecolumn"}]}
                            )
                        }
                    ],
                }
            ]
        }
        field = parse_pbir_legacy(report)[0].visuals[0].fields["values"][0]

        assert field.table == ""
        assert field.column == "barecolumn"

    def test_empty_query_ref_is_dropped(self):
        from dbt_meta.powerbi.pbir_parser import parse_pbir_legacy

        report = {
            "sections": [
                {
                    "displayName": "P",
                    "visualContainers": [
                        {
                            "config": fx._config(
                                "card",
                                {"Values": [{"queryRef": "  "}, {"queryRef": "t.c"}]},
                            )
                        }
                    ],
                }
            ]
        }
        fields = parse_pbir_legacy(report)[0].visuals[0].fields

        # only the valid ref survives; the role still appears once
        assert [f.column for f in fields["values"]] == ["c"]

    def test_role_with_only_empty_refs_is_omitted(self):
        from dbt_meta.powerbi.pbir_parser import parse_pbir_legacy

        # First role yields no refs (looping on to the next role), second is valid.
        report = {
            "sections": [
                {
                    "displayName": "P",
                    "visualContainers": [
                        {
                            "config": fx._config(
                                "card",
                                {
                                    "Tooltips": [{"queryRef": ""}],
                                    "Values": [{"queryRef": "t.c"}],
                                },
                            )
                        }
                    ],
                }
            ]
        }
        fields = parse_pbir_legacy(report)[0].visuals[0].fields

        assert "tooltip" not in fields
        assert [f.column for f in fields["values"]] == ["c"]

    def test_projections_not_a_dict_yields_no_fields(self):
        from dbt_meta.powerbi.pbir_parser import parse_pbir_legacy

        config = json.dumps(
            {"singleVisual": {"visualType": "card", "projections": ["bad"]}}
        )
        report = {
            "sections": [
                {"displayName": "P", "visualContainers": [{"config": config}]}
            ]
        }
        visual = parse_pbir_legacy(report)[0].visuals[0]

        assert visual.type == "card"
        assert visual.fields == {}

    def test_role_whose_items_are_not_a_list_are_skipped(self):
        from dbt_meta.powerbi.pbir_parser import parse_pbir_legacy

        config = json.dumps(
            {
                "singleVisual": {
                    "visualType": "card",
                    "projections": {
                        "Bad": "notalist",
                        "Values": [{"queryRef": "t.c"}],
                    },
                }
            }
        )
        report = {
            "sections": [
                {"displayName": "P", "visualContainers": [{"config": config}]}
            ]
        }
        fields = parse_pbir_legacy(report)[0].visuals[0].fields

        assert set(fields) == {"values"}

    def test_missing_visual_type_defaults_to_unknown(self):
        from dbt_meta.powerbi.pbir_parser import parse_pbir_legacy

        config = json.dumps({"singleVisual": {"projections": {}}})
        report = {
            "sections": [
                {"displayName": "P", "visualContainers": [{"config": config}]}
            ]
        }

        assert parse_pbir_legacy(report)[0].visuals[0].type == "unknown"

    def test_non_dict_section_is_skipped(self):
        from dbt_meta.powerbi.pbir_parser import parse_pbir_legacy

        report = {"sections": ["not a dict", {"displayName": "Real"}]}
        pages = parse_pbir_legacy(report)

        assert [p.name for p in pages] == ["Real"]

    def test_section_name_falls_back_to_name_then_empty(self):
        from dbt_meta.powerbi.pbir_parser import parse_pbir_legacy

        report = {"sections": [{"name": "S_internal"}, {}]}
        pages = parse_pbir_legacy(report)

        assert [p.name for p in pages] == ["S_internal", ""]


# ============================================================================
# TestLayoutPersistence — pages survive a save/load roundtrip, sparse emission
# ============================================================================


class TestLayoutPersistence:
    def _index_with_pages(self):
        from dbt_meta.powerbi.index import (
            FieldRef,
            PageEntry,
            VisualEntry,
        )

        report = ReportEntry(workspace="W", report="R", dataset="D")
        report.pages = [
            PageEntry(
                name="P1",
                visuals=[
                    VisualEntry(
                        type="funnel",
                        fields={
                            "values": [
                                FieldRef(table="events", column="cnt", kind="measure")
                            ],
                            "axis": [
                                FieldRef(table="events", column="stage", kind="column")
                            ],
                        },
                    )
                ],
            )
        ]
        return PowerBiIndex(reports=[report])

    def test_pages_roundtrip_through_save_load(self, tmp_path):
        out = tmp_path / "idx.json"
        save_index(self._index_with_pages(), str(out))

        loaded = load_index(str(out))
        page = loaded.reports[0].pages[0]
        assert page.name == "P1"
        visual = page.visuals[0]
        assert visual.type == "funnel"
        assert visual.fields["values"][0].kind == "measure"
        assert visual.fields["axis"][0].column == "stage"

    def test_pages_omitted_when_empty(self, tmp_path):
        out = tmp_path / "idx.json"
        save_index(PowerBiIndex(reports=[ReportEntry("W", "R", "D")]), str(out))

        data = orjson.loads(out.read_bytes())
        assert "pages" not in data["reports"][0]
        assert data["schema_version"] == "1.2"


# ============================================================================
# TestFabricDefinition — get_report_definition LRO + decode
# ============================================================================


class TestFabricDefinition:
    def _report_json_part(self):
        import base64

        payload = base64.b64encode(orjson.dumps(fx.PBIR_LEGACY_REPORT)).decode()
        return {"definition": {"parts": [{"path": "report.json", "payload": payload}]}}

    def test_inline_200_decodes_report_json(self, monkeypatch):
        from dbt_meta.utils import powerbi as pb

        body = orjson.dumps(self._report_json_part()).decode()
        monkeypatch.setattr(pb, "_fabric_call", lambda *a, **k: (200, {}, body))

        result = pb.get_report_definition("tok", "ws", "rpt")

        assert result is not None
        assert result["sections"][0]["displayName"] == "PPC Reg Cohorts"

    def test_lro_202_polls_to_succeeded(self, monkeypatch):
        from dbt_meta.utils import powerbi as pb

        result_body = orjson.dumps(self._report_json_part()).decode()
        calls = {"n": 0}

        def fake_call(token, url, method="GET", body=None, timeout=90):
            calls["n"] += 1
            if calls["n"] == 1:
                return 202, {"location": "https://poll", "retry-after": "0"}, ""
            if url.endswith("/result"):
                return 200, {}, result_body
            return 200, {}, orjson.dumps({"status": "Succeeded"}).decode()

        monkeypatch.setattr(pb, "_fabric_call", fake_call)

        result = pb.get_report_definition("tok", "ws", "rpt", poll_interval=0)

        assert result is not None
        assert result["sections"][0]["name"] == "ReportSection1"

    def test_lro_failed_returns_none(self, monkeypatch):
        from dbt_meta.utils import powerbi as pb

        def fake_call(token, url, method="GET", body=None, timeout=90):
            if method == "POST":
                return 202, {"location": "https://poll", "retry-after": "0"}, ""
            return 200, {}, orjson.dumps({"status": "Failed"}).decode()

        monkeypatch.setattr(pb, "_fabric_call", fake_call)

        assert pb.get_report_definition("tok", "ws", "rpt", poll_interval=0) is None

    def test_http_error_returns_none(self, monkeypatch):
        from dbt_meta.utils import powerbi as pb

        monkeypatch.setattr(pb, "_fabric_call", lambda *a, **k: (403, {}, "denied"))

        assert pb.get_report_definition("tok", "ws", "rpt") is None

    def test_missing_report_json_part_returns_none(self, monkeypatch):
        from dbt_meta.utils import powerbi as pb

        body = orjson.dumps({"definition": {"parts": []}}).decode()
        monkeypatch.setattr(pb, "_fabric_call", lambda *a, **k: (200, {}, body))

        assert pb.get_report_definition("tok", "ws", "rpt") is None

    def test_200_with_malformed_json_returns_none(self, monkeypatch):
        from dbt_meta.utils import powerbi as pb

        monkeypatch.setattr(pb, "_fabric_call", lambda *a, **k: (200, {}, "{bad"))

        assert pb.get_report_definition("tok", "ws", "rpt") is None

    def test_200_without_definition_returns_none(self, monkeypatch):
        from dbt_meta.utils import powerbi as pb

        monkeypatch.setattr(pb, "_fabric_call", lambda *a, **k: (200, {}, "{}"))

        assert pb.get_report_definition("tok", "ws", "rpt") is None

    def test_202_without_location_returns_none(self, monkeypatch):
        from dbt_meta.utils import powerbi as pb

        monkeypatch.setattr(pb, "_fabric_call", lambda *a, **k: (202, {}, ""))

        assert pb.get_report_definition("tok", "ws", "rpt", poll_interval=0) is None

    def test_202_invalid_retry_after_is_tolerated(self, monkeypatch):
        from dbt_meta.utils import powerbi as pb

        result_body = orjson.dumps(self._report_json_part()).decode()

        def fake_call(token, url, method="GET", body=None, timeout=90):
            if method == "POST":
                return 202, {"location": "https://poll", "retry-after": "soon"}, ""
            if url.endswith("/result"):
                return 200, {}, result_body
            return 200, {}, orjson.dumps({"status": "Succeeded"}).decode()

        monkeypatch.setattr(pb, "_fabric_call", fake_call)

        assert pb.get_report_definition("tok", "ws", "rpt", poll_interval=0) is not None

    def test_poll_body_malformed_returns_none(self, monkeypatch):
        from dbt_meta.utils import powerbi as pb

        def fake_call(token, url, method="GET", body=None, timeout=90):
            if method == "POST":
                return 202, {"location": "https://poll", "retry-after": "0"}, ""
            return 200, {}, "{bad"

        monkeypatch.setattr(pb, "_fabric_call", fake_call)

        assert pb.get_report_definition("tok", "ws", "rpt", poll_interval=0) is None

    def test_result_body_malformed_returns_none(self, monkeypatch):
        from dbt_meta.utils import powerbi as pb

        def fake_call(token, url, method="GET", body=None, timeout=90):
            if method == "POST":
                return 202, {"location": "https://poll", "retry-after": "0"}, ""
            if url.endswith("/result"):
                return 200, {}, "{bad"
            return 200, {}, orjson.dumps({"status": "Succeeded"}).decode()

        monkeypatch.setattr(pb, "_fabric_call", fake_call)

        assert pb.get_report_definition("tok", "ws", "rpt", poll_interval=0) is None

    def test_lro_running_then_timeout_returns_none(self, monkeypatch):
        from dbt_meta.utils import powerbi as pb

        def fake_call(token, url, method="GET", body=None, timeout=90):
            if method == "POST":
                return 202, {"location": "https://poll", "retry-after": "0"}, ""
            return 200, {}, orjson.dumps({"status": "Running"}).decode()

        monkeypatch.setattr(pb, "_fabric_call", fake_call)

        result = pb.get_report_definition(
            "tok", "ws", "rpt", poll_interval=0, max_polls=2
        )
        assert result is None

    def test_bad_base64_payload_returns_none(self, monkeypatch):
        from dbt_meta.utils import powerbi as pb

        body = orjson.dumps(
            {"definition": {"parts": [{"path": "report.json", "payload": "!!!"}]}}
        ).decode()
        monkeypatch.setattr(pb, "_fabric_call", lambda *a, **k: (200, {}, body))

        assert pb.get_report_definition("tok", "ws", "rpt") is None


# ============================================================================
# TestFabricToken — get_fabric_token uses the Fabric audience, secret off argv
# ============================================================================


class TestFabricToken:
    def test_fabric_token_uses_fabric_scope_and_hides_secret(self, monkeypatch):
        from dbt_meta.utils import powerbi as pb

        run = _CapturedRun('{"access_token": "fab"}')
        monkeypatch.setattr(pb.subprocess, "run", run)
        monkeypatch.setattr(pb.shutil, "which", lambda _: "/usr/bin/curl")

        token = pb.get_fabric_token("tenant", "client", "s3cret")

        assert token == "fab"
        assert "s3cret" not in " ".join(run.cmd)
        assert "api.fabric.microsoft.com%2F.default" in run.input

    def test_fabric_token_none_when_curl_missing(self, monkeypatch):
        from dbt_meta.utils import powerbi as pb

        monkeypatch.setattr(pb.shutil, "which", lambda _: None)

        assert pb.get_fabric_token("t", "c", "s") is None


# ============================================================================
# TestFabricCall — _fabric_call header/status parsing over the curl boundary
# ============================================================================


class TestFabricCall:
    def _run_returning(self, stdout, returncode=0):
        class _R:
            pass

        def run(cmd, *, input=None, capture_output=None, text=None, timeout=None):
            r = _R()
            r.returncode = returncode
            r.stdout = stdout
            r.cmd = cmd
            r.input = input
            run.captured = r
            return r

        return run

    def test_parses_code_headers_and_body(self, monkeypatch):
        from dbt_meta.utils import powerbi as pb

        stdout = (
            "HTTP/1.1 202 Accepted\r\n"
            "Location: https://poll/op\r\n"
            "Retry-After: 5\r\n"
            "\r\n"
            "body-bytes"
            "\n__HTTP_CODE__:202"
        )
        monkeypatch.setattr(pb.shutil, "which", lambda _: "/usr/bin/curl")
        monkeypatch.setattr(pb.subprocess, "run", self._run_returning(stdout))

        code, headers, body = pb._fabric_call("tok", "https://x", method="POST", body={})

        assert code == 202
        assert headers["location"] == "https://poll/op"
        assert headers["retry-after"] == "5"
        assert body == "body-bytes"

    def test_curl_missing_returns_zero(self, monkeypatch):
        from dbt_meta.utils import powerbi as pb

        monkeypatch.setattr(pb.shutil, "which", lambda _: None)

        assert pb._fabric_call("tok", "https://x") == (0, {}, "")

    def test_nonzero_returncode_returns_zero(self, monkeypatch):
        from dbt_meta.utils import powerbi as pb

        monkeypatch.setattr(pb.shutil, "which", lambda _: "/usr/bin/curl")
        monkeypatch.setattr(pb.subprocess, "run", self._run_returning("x", returncode=1))

        assert pb._fabric_call("tok", "https://x") == (0, {}, "")

    def test_timeout_returns_zero(self, monkeypatch):
        from dbt_meta.utils import powerbi as pb

        def run(*a, **k):
            raise pb.subprocess.TimeoutExpired(cmd="curl", timeout=1)

        monkeypatch.setattr(pb.shutil, "which", lambda _: "/usr/bin/curl")
        monkeypatch.setattr(pb.subprocess, "run", run)

        assert pb._fabric_call("tok", "https://x") == (0, {}, "")

    def test_lf_only_header_separator_is_handled(self, monkeypatch):
        from dbt_meta.utils import powerbi as pb

        stdout = "HTTP/1.1 200 OK\nX-Test: v\n\nthebody\n__HTTP_CODE__:200"
        monkeypatch.setattr(pb.shutil, "which", lambda _: "/usr/bin/curl")
        monkeypatch.setattr(pb.subprocess, "run", self._run_returning(stdout))

        code, headers, body = pb._fabric_call("tok", "https://x")

        assert code == 200
        assert headers["x-test"] == "v"
        assert body == "thebody"

    def test_missing_http_marker_yields_zero_code(self, monkeypatch):
        from dbt_meta.utils import powerbi as pb

        # No -w marker in the output (e.g. curl wrote nothing measurable).
        stdout = "HTTP/1.1 200 OK\r\nX-Test: v\r\n\r\nbody"
        monkeypatch.setattr(pb.shutil, "which", lambda _: "/usr/bin/curl")
        monkeypatch.setattr(pb.subprocess, "run", self._run_returning(stdout))

        code, headers, body = pb._fabric_call("tok", "https://x")

        assert code == 0
        assert headers["x-test"] == "v"
        assert body == "body"

    def test_bearer_token_stays_off_argv(self, monkeypatch):
        from dbt_meta.utils import powerbi as pb

        run = self._run_returning("HTTP/1.1 200 OK\r\n\r\nok\n__HTTP_CODE__:200")
        monkeypatch.setattr(pb.shutil, "which", lambda _: "/usr/bin/curl")
        monkeypatch.setattr(pb.subprocess, "run", run)

        pb._fabric_call("jwt-secret", "https://x", method="POST", body={"k": 1})

        captured = run.captured
        assert "jwt-secret" not in " ".join(captured.cmd)
        assert "jwt-secret" in captured.input
        assert "-d" in captured.cmd


# ============================================================================
# TestArtifactsCliContract — the --no-layouts flag wiring through the CLI layer
# ============================================================================


class TestArtifactsCliContract:
    def _runner(self):
        from typer.testing import CliRunner

        from dbt_meta.cli import app

        return CliRunner(), app

    def test_help_exposes_no_layouts(self):
        runner, app = self._runner()
        result = runner.invoke(app, ["powerbi", "artifacts", "--help"])

        assert result.exit_code == 0
        assert "--no-layouts" in result.output

    def test_no_layouts_flag_sets_with_layouts_false(self, tmp_path, monkeypatch):
        import dbt_meta.command_impl.powerbi as pbi

        man = _cmd_write(tmp_path / "manifest.json", CMD_MANIFEST)
        captured = {}

        def fake_artifacts_cmd(config, manifest_path, raw_path, index_path, **kwargs):
            captured["with_layouts"] = kwargs.get("with_layouts")
            return {"reports": 0, "raw_path": raw_path, "index_path": index_path}

        monkeypatch.setattr(pbi, "artifacts_cmd", fake_artifacts_cmd)

        runner, app = self._runner()
        result = runner.invoke(
            app, ["powerbi", "artifacts", "--manifest", man, "--no-layouts", "-j"]
        )

        assert result.exit_code == 0
        assert captured["with_layouts"] is False

    def test_default_keeps_layouts_on(self, tmp_path, monkeypatch):
        import dbt_meta.command_impl.powerbi as pbi

        man = _cmd_write(tmp_path / "manifest.json", CMD_MANIFEST)
        captured = {}

        def fake_artifacts_cmd(config, manifest_path, raw_path, index_path, **kwargs):
            captured["with_layouts"] = kwargs.get("with_layouts")
            return {"reports": 0, "raw_path": raw_path, "index_path": index_path}

        monkeypatch.setattr(pbi, "artifacts_cmd", fake_artifacts_cmd)

        runner, app = self._runner()
        result = runner.invoke(
            app, ["powerbi", "artifacts", "--manifest", man, "-j"]
        )

        assert result.exit_code == 0
        assert captured["with_layouts"] is True


# ============================================================================
# TestPbirFilters — title + filter parsing (visual semantics, v0.3.6)
# ============================================================================


class TestPbirFilters:
    def test_report_level_in_filter(self):
        from dbt_meta.powerbi.pbir_parser import parse_report_filters

        filters = parse_report_filters(fx.PBIR_LEGACY_SEMANTICS)
        device = next(f for f in filters if f.column == "device_type")

        assert device.table == "Device"
        assert device.kind == "column"
        assert device.op == "in"
        assert device.values == ["desktop", "mobile"]
        assert device.summary == "device_type in (desktop, mobile)"

    def test_report_level_advanced_filter(self):
        from dbt_meta.powerbi.pbir_parser import parse_report_filters

        adv = next(
            f for f in parse_report_filters(fx.PBIR_LEGACY_SEMANTICS) if f.op == "advanced"
        )
        assert adv.summary == "status = active and amount > 0"
        assert adv.values == ["active", "0"]

    def test_page_level_relative_date_filter(self):
        from dbt_meta.powerbi.pbir_parser import parse_pbir_legacy

        page = parse_pbir_legacy(fx.PBIR_LEGACY_SEMANTICS)[0]
        rd = page.filters[0]

        assert rd.op == "relative_date"
        assert rd.column == "day_iso"
        assert rd.values == ["30", "days"]
        assert rd.summary == "day_iso — last 30 days"

    def test_visual_title_and_cmp_filter(self):
        from dbt_meta.powerbi.pbir_parser import parse_pbir_legacy

        bar = parse_pbir_legacy(fx.PBIR_LEGACY_SEMANTICS)[0].visuals[0]

        assert bar.title == "Revenue by stage"
        assert len(bar.filters) == 1
        cmp = bar.filters[0]
        assert cmp.op == "cmp"
        assert cmp.values == [">", "100"]
        assert cmp.summary == "amount > 100"

    def test_visual_top_n_filter(self):
        from dbt_meta.powerbi.pbir_parser import parse_pbir_legacy

        table = parse_pbir_legacy(fx.PBIR_LEGACY_SEMANTICS)[0].visuals[1]
        top = table.filters[0]

        assert top.op == "top_n"
        assert top.values == ["10"]
        assert top.summary == "top 10 by Sum(revenue)"

    def test_visual_without_title_or_filter_is_sparse(self):
        from dbt_meta.powerbi.pbir_parser import parse_pbir_legacy

        card = parse_pbir_legacy(fx.PBIR_LEGACY_SEMANTICS)[0].visuals[2]

        assert card.title is None
        assert card.filters == []

    def test_measure_filter_kind_via_measure_set(self):
        from dbt_meta.powerbi.pbir_parser import parse_report_filters

        report = {
            "filters": json.dumps(
                [fx._filter_in("metrics", "total_revenue", ["x"])]
            )
        }
        f = parse_report_filters(report, measures=["total_revenue"])[0]
        assert f.kind == "measure"

    def test_measure_expression_filter(self):
        from dbt_meta.powerbi.pbir_parser import parse_report_filters

        report = {
            "filters": json.dumps(
                [
                    {
                        "expression": {
                            "Measure": {
                                "Expression": {"SourceRef": {"Entity": "metrics"}},
                                "Property": "revenue",
                            }
                        },
                        "type": "Advanced",
                        "filter": {
                            "Where": [
                                {
                                    "Condition": {
                                        "Comparison": {
                                            "ComparisonKind": 2,
                                            "Left": {},
                                            "Right": {"Literal": {"Value": "5D"}},
                                        }
                                    }
                                }
                            ]
                        },
                    }
                ]
            )
        }
        f = parse_report_filters(report)[0]
        assert f.table == "metrics"
        assert f.column == "revenue"
        assert f.kind == "measure"
        assert f.op == "cmp"
        assert f.values == [">=", "5"]

    def test_aggregation_expression_filter_is_measure(self):
        from dbt_meta.powerbi.pbir_parser import parse_report_filters

        report = {
            "filters": json.dumps(
                [
                    {
                        "expression": {
                            "Aggregation": {
                                "Function": 0,
                                "Expression": fx._col_expr("events", "amount"),
                            }
                        },
                        "type": "Advanced",
                        "filter": {"Where": []},
                    }
                ]
            )
        }
        f = parse_report_filters(report)[0]
        assert f.column == "amount"
        assert f.kind == "measure"

    def test_hierarchy_level_expression_filter(self):
        from dbt_meta.powerbi.pbir_parser import parse_report_filters

        report = {
            "filters": json.dumps(
                [
                    {
                        "expression": {
                            "HierarchyLevel": {
                                "Level": "Month",
                                "Expression": {
                                    "Hierarchy": {
                                        "Expression": {
                                            "SourceRef": {"Entity": "d_calendar"}
                                        }
                                    }
                                },
                            }
                        },
                        "type": "Advanced",
                        "filter": {"Where": []},
                    }
                ]
            )
        }
        f = parse_report_filters(report)[0]
        assert f.table == "d_calendar"
        assert f.column == "Month"

    def test_relative_date_without_datespan_degrades(self):
        from dbt_meta.powerbi.pbir_parser import parse_report_filters

        report = {
            "filters": json.dumps(
                [
                    {
                        "expression": fx._col_expr("d_calendar", "day_iso"),
                        "type": "RelativeDate",
                        "filter": {"Where": []},
                    }
                ]
            )
        }
        f = parse_report_filters(report)[0]
        assert f.op == "relative_date"
        assert f.values == []
        assert f.summary == "day_iso — relative date"

    def test_top_n_without_count_or_orderby_degrades(self):
        from dbt_meta.powerbi.pbir_parser import parse_report_filters

        report = {
            "filters": json.dumps(
                [
                    {
                        "expression": fx._col_expr("events", "client_id"),
                        "type": "TopN",
                        "filter": {"Where": []},
                    }
                ]
            )
        }
        f = parse_report_filters(report)[0]
        assert f.op == "top_n"
        assert f.values == []
        assert f.summary == "top N on client_id"

    def test_top_n_count_without_orderby(self):
        from dbt_meta.powerbi.pbir_parser import parse_report_filters

        report = {
            "filters": json.dumps(
                [
                    {
                        "expression": fx._col_expr("events", "client_id"),
                        "type": "TopN",
                        "filter": {
                            "Where": [
                                {
                                    "Condition": {
                                        "Comparison": {
                                            "ComparisonKind": 3,
                                            "Left": {},
                                            "Right": {"Literal": {"Value": "5L"}},
                                        }
                                    }
                                }
                            ]
                        },
                    }
                ]
            )
        }
        f = parse_report_filters(report)[0]
        assert f.values == ["5"]
        assert f.summary == "top 5"

    def test_comparison_without_literal_bound(self):
        from dbt_meta.powerbi.pbir_parser import parse_report_filters

        report = {
            "filters": json.dumps(
                [
                    {
                        "expression": fx._col_expr("events", "amount"),
                        "type": "Advanced",
                        "filter": {
                            "Where": [
                                {
                                    "Condition": {
                                        "Comparison": {
                                            "ComparisonKind": 0,
                                            "Left": {},
                                            "Right": {"Column": {}},
                                        }
                                    }
                                }
                            ]
                        },
                    }
                ]
            )
        }
        f = parse_report_filters(report)[0]
        assert f.op == "cmp"
        assert f.values == ["="]
        assert f.summary == "amount ="

    def test_malformed_filters_are_isolated(self):
        from dbt_meta.powerbi.pbir_parser import parse_report_filters

        report = {
            "filters": json.dumps(
                [
                    "not a dict",
                    {"no_expression": True},
                    {"expression": {"Unknown": {}}, "type": "X"},
                    fx._filter_in("Device", "device_type", ["a"]),
                ]
            )
        }
        filters = parse_report_filters(report)
        assert len(filters) == 1
        assert filters[0].column == "device_type"

    def test_filters_bad_json_string_returns_empty(self):
        from dbt_meta.powerbi.pbir_parser import parse_report_filters

        assert parse_report_filters({"filters": "not json{{"}) == []

    def test_filters_non_string_non_list_returns_empty(self):
        from dbt_meta.powerbi.pbir_parser import parse_report_filters

        assert parse_report_filters({"filters": 42}) == []

    def test_filters_accepts_list_directly(self):
        from dbt_meta.powerbi.pbir_parser import _parse_filters

        out = _parse_filters([fx._filter_in("D", "c", ["x"])], frozenset())
        assert out[0].column == "c"

    def test_no_filters_key(self):
        from dbt_meta.powerbi.pbir_parser import parse_report_filters

        assert parse_report_filters({"sections": []}) == []

    def test_title_missing_variants_return_none(self):
        from dbt_meta.powerbi.pbir_parser import _extract_title

        assert _extract_title({}) is None
        assert _extract_title({"vcObjects": 5}) is None
        assert _extract_title({"vcObjects": {"title": []}}) is None
        assert _extract_title({"vcObjects": {"title": [5]}}) is None
        assert _extract_title({"vcObjects": {"title": [{"properties": {}}]}}) is None

    def test_clean_literal_forms(self):
        from dbt_meta.powerbi.pbir_parser import _clean_literal

        assert _clean_literal("'hello'") == "hello"
        assert _clean_literal("100L") == "100"
        assert _clean_literal("3.14D") == "3.14"
        assert _clean_literal("bare") == "bare"
        assert _clean_literal(True) == "True"


# ============================================================================
# TestLayoutCmd — layout_cmd + artifact round-trip of semantics
# ============================================================================


class TestLayoutCmd:
    def _index_with_semantics(self):
        from dbt_meta.powerbi.pbir_parser import (
            parse_pbir_legacy,
            parse_report_filters,
        )

        report = ReportEntry(workspace="W", report="Sales", dataset="DS")
        report.pages = parse_pbir_legacy(fx.PBIR_LEGACY_SEMANTICS)
        report.filters = parse_report_filters(fx.PBIR_LEGACY_SEMANTICS)
        return PowerBiIndex(reports=[report])

    def test_layout_returns_pages_and_filters(self, tmp_path):
        out = tmp_path / "idx.json"
        save_index(self._index_with_semantics(), str(out))

        result = cmd.layout_cmd(str(out), "Sales")

        assert result["report"] == "Sales"
        assert {f["column"] for f in result["filters"]} == {"device_type", "flags"}
        page = result["pages"][0]
        assert page["name"] == "Overview"
        assert page["filters"][0]["op"] == "relative_date"
        bar = page["visuals"][0]
        assert bar["title"] == "Revenue by stage"
        assert bar["filters"][0]["op"] == "cmp"

    def test_layout_unknown_report_raises(self, tmp_path):
        out = tmp_path / "idx.json"
        save_index(self._index_with_semantics(), str(out))

        with pytest.raises(DbtMetaError):
            cmd.layout_cmd(str(out), "Nonexistent")

    def test_semantics_roundtrip_through_artifact(self, tmp_path):
        out = tmp_path / "idx.json"
        save_index(self._index_with_semantics(), str(out))

        loaded = load_index(str(out))
        report = loaded.reports[0]
        assert len(report.filters) == 2
        bar = report.pages[0].visuals[0]
        assert bar.title == "Revenue by stage"
        assert bar.filters[0].summary == "amount > 100"
        assert report.pages[0].filters[0].values == ["30", "days"]

    def test_enrich_attaches_report_filters(self, monkeypatch):
        from dbt_meta.powerbi.index import PowerBiIndex as Idx

        index = Idx(reports=[ReportEntry("W", "R", "D")])
        scan = {
            "workspaces": [
                {
                    "id": "ws1",
                    "name": "W",
                    "datasets": [],
                    "reports": [{"id": "r1", "name": "R", "datasetId": "d1"}],
                }
            ]
        }
        monkeypatch.setattr(
            cmd, "get_report_definition", lambda *a, **k: fx.PBIR_LEGACY_SEMANTICS
        )

        got, _total = cmd.enrich_with_layouts(index, scan, "fabtok")

        assert got == 1
        assert len(index.reports[0].filters) == 2
        assert index.reports[0].pages[0].name == "Overview"


class TestPbirAdvancedRender:
    def _advanced(self, condition):
        from dbt_meta.powerbi.pbir_parser import parse_report_filters

        report = {
            "filters": json.dumps(
                [
                    {
                        "expression": fx._col_expr("events", "x"),
                        "type": "Advanced",
                        "filter": {"Where": [{"Condition": condition}]},
                    }
                ]
            )
        }
        return parse_report_filters(report)[0]

    def test_advanced_or_with_in_and_comparison(self):
        cond = {
            "Or": {
                "Left": {
                    "In": {
                        "Expressions": [fx._col_expr("events", "country")],
                        "Values": [
                            [{"Literal": {"Value": "'EE'"}}],
                            "not-a-list",
                        ],
                    }
                },
                "Right": {
                    "Comparison": {
                        "ComparisonKind": 1,
                        "Left": fx._col_expr("events", "amount"),
                        "Right": {"Literal": {"Value": "0L"}},
                    }
                },
            }
        }
        f = self._advanced(cond)
        assert f.op == "advanced"
        assert f.summary == "country in (EE) or amount > 0"

    def test_advanced_not_condition(self):
        cond = {
            "Not": {
                "Expression": {
                    "Comparison": {
                        "ComparisonKind": 0,
                        "Left": fx._col_expr("events", "status"),
                        "Right": {"Literal": {"Value": "'x'"}},
                    }
                }
            }
        }
        f = self._advanced(cond)
        assert f.summary == "not status = x"

    def test_advanced_unknown_condition_renders_empty(self):
        f = self._advanced({"Weird": {}})
        # No renderable conditions → fallback summary.
        assert f.summary == "x (advanced)"

    def test_expr_label_aggregation_without_inner(self):
        from dbt_meta.powerbi.pbir_parser import _expr_label

        assert _expr_label({"Aggregation": {"Function": 0}}) == "Sum(?)"
        assert _expr_label({"Aggregation": {"Function": 99, "Expression": {}}}) == "Agg(?)"

    def test_expr_label_none_for_unknown(self):
        from dbt_meta.powerbi.pbir_parser import _expr_label

        assert _expr_label({}) is None
        assert _expr_label("not a dict") is None

    def test_entity_prop_requires_property(self):
        from dbt_meta.powerbi.pbir_parser import _unwrap_column_like

        assert _unwrap_column_like({"Column": {"Expression": {}}}) is None

    def test_unwrap_non_dict(self):
        from dbt_meta.powerbi.pbir_parser import _unwrap_column_like

        assert _unwrap_column_like("x") is None
        assert _unwrap_column_like({"Nope": {}}) is None

    def test_render_condition_non_dict(self):
        from dbt_meta.powerbi.pbir_parser import _render_condition

        assert _render_condition("x") == ""

    def test_find_int_literal_in_list(self):
        from dbt_meta.powerbi.pbir_parser import _find_int_literal

        assert _find_int_literal([{"Literal": {"Value": "7L"}}]) == "7"
        assert _find_int_literal([{"Literal": {"Value": "3.5D"}}]) is None
        assert _find_int_literal("scalar") is None


class TestPbirUnwrapEdges:
    def test_aggregation_without_inner_is_skipped(self):
        from dbt_meta.powerbi.pbir_parser import parse_report_filters

        report = {
            "filters": json.dumps(
                [
                    {
                        "expression": {"Aggregation": {"Function": 0, "Expression": {}}},
                        "type": "Advanced",
                        "filter": {"Where": []},
                    }
                ]
            )
        }
        # Unresolvable aggregation field → filter dropped, never raised.
        assert parse_report_filters(report) == []
