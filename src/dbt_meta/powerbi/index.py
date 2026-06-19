"""Build the compact, agent-ready Power BI index from a raw scanResult.

Ties the pieces together:

``m_parser`` (classify each table's M-expression) → ``sql_analyzer`` (native SQL →
tables + clause columns) → ``resolver`` (follow intra-dataset cross-query refs to
leaves) → ``mapper`` (physical table → dbt model / source / external).

Output is a :class:`PowerBiIndex`: one entry per report (workspace, dataset,
resolved tables, SQL analysis) plus a ``measure → tables`` reverse index so a metric
name lifted from a screenshot resolves to concrete BigQuery tables.

The Scanner API exposes report → dataset → all-tables-of-dataset, but no
page/visual → table binding; that is the finest resolution available.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .m_parser import parse_m_expression
from .mapper import DbtTableIndex, TableMapping
from .resolver import QueryNode, resolve_query_tables
from .sql_analyzer import analyze_sql

SCHEMA_VERSION = "1.0"


@dataclass(frozen=True)
class TableRef:
    """A physical table behind a report, classified against dbt."""

    bq: str
    status: str  # model | source | external
    dbt_model: str | None = None
    parse_status: str = "ok"


@dataclass(frozen=True)
class SqlAnalysisEntry:
    """What one native-SQL query does (logic living outside dbt)."""

    query: str
    tables: tuple[str, ...]
    filters: tuple[str, ...]
    joins: tuple[str, ...]
    group_by: tuple[str, ...]
    parse_status: str
    sql: str | None = None


@dataclass
class ReportEntry:
    workspace: str
    report: str
    dataset: str
    tables: list[TableRef] = field(default_factory=list)
    sql_analysis: list[SqlAnalysisEntry] = field(default_factory=list)


@dataclass
class PowerBiIndex:
    reports: list[ReportEntry] = field(default_factory=list)
    metric_index: dict[str, list[str]] = field(default_factory=dict)
    generated_at: str = ""
    schema_version: str = SCHEMA_VERSION


@dataclass
class _DatasetInfo:
    """Pre-resolved facts about a dataset, keyed by dataset id."""

    name: str
    tables: list[TableRef]
    sql_analysis: list[SqlAnalysisEntry]
    metric_to_tables: dict[str, list[str]]


def _build_dataset(dataset: dict[str, Any], dbt_index: DbtTableIndex) -> _DatasetInfo:
    raw_tables = dataset.get("tables") or []

    # Parse every query (table) once: direct tables + cross-query refs + analysis.
    nodes: dict[str, QueryNode] = {}
    analyses: dict[str, SqlAnalysisEntry] = {}
    for tbl in raw_tables:
        name = tbl.get("name", "")
        expr = _expression(tbl)
        if not expr:
            nodes[name] = QueryNode()
            continue
        src = parse_m_expression(expr)
        if src.kind == "native_sql" and src.native_sql is not None:
            analysis = analyze_sql(src.native_sql)
            nodes[name] = QueryNode(
                tables=analysis.tables, cross_query_refs=src.cross_query_refs
            )
            analyses[name] = SqlAnalysisEntry(
                query=name,
                tables=analysis.tables,
                filters=analysis.filters,
                joins=analysis.joins,
                group_by=analysis.group_by,
                parse_status=analysis.parse_status,
                sql=analysis.sql,
            )
        else:
            nodes[name] = QueryNode(
                tables=src.tables, cross_query_refs=src.cross_query_refs
            )

    # Resolve each query to its full leaf table set (cross-query recursion).
    query_tables: dict[str, tuple[str, ...]] = {
        name: resolve_query_tables(name, nodes) for name in nodes
    }

    # Dataset-level deduped table refs.
    seen: dict[str, TableRef] = {}
    for tables in query_tables.values():
        for bq in tables:
            if bq not in seen:
                m: TableMapping = dbt_index.lookup(bq)
                seen[bq] = TableRef(
                    bq=m.bq, status=m.status, dbt_model=m.dbt_name
                )

    # measure -> the resolved tables of the query that defines it.
    metric_to_tables: dict[str, list[str]] = {}
    for tbl in raw_tables:
        name = tbl.get("name", "")
        for measure in tbl.get("measures") or []:
            mname = measure.get("name")
            if mname:
                metric_to_tables.setdefault(mname, [])
                for bq in query_tables.get(name, ()):
                    if bq not in metric_to_tables[mname]:
                        metric_to_tables[mname].append(bq)

    return _DatasetInfo(
        name=dataset.get("name", ""),
        tables=list(seen.values()),
        sql_analysis=list(analyses.values()),
        metric_to_tables=metric_to_tables,
    )


def _expression(table: dict[str, Any]) -> str:
    for src in table.get("source") or []:
        expr = src.get("expression")
        if expr:
            return str(expr)
    return ""


def build_index(scan_result: dict[str, Any], manifest: dict[str, Any]) -> PowerBiIndex:
    """Build a :class:`PowerBiIndex` from a raw scanResult + dbt manifest."""
    dbt_index = DbtTableIndex(manifest)
    index = PowerBiIndex(generated_at=datetime.now(timezone.utc).isoformat())

    for ws in scan_result.get("workspaces") or []:
        ws_name = ws.get("name", "")
        datasets: dict[str, _DatasetInfo] = {
            ds.get("id", ""): _build_dataset(ds, dbt_index)
            for ds in ws.get("datasets") or []
        }

        # Accumulate the metric index across all datasets.
        for ds_info in datasets.values():
            for metric, tables in ds_info.metric_to_tables.items():
                bucket = index.metric_index.setdefault(metric, [])
                for t in tables:
                    if t not in bucket:
                        bucket.append(t)

        for report in ws.get("reports") or []:
            ds_id = report.get("datasetId", "")
            report_ds = datasets.get(ds_id)
            entry = ReportEntry(
                workspace=ws_name,
                report=report.get("name", ""),
                dataset=report_ds.name if report_ds else "",
            )
            if report_ds:
                entry.tables = list(report_ds.tables)
                entry.sql_analysis = list(report_ds.sql_analysis)
            index.reports.append(entry)

    return index
