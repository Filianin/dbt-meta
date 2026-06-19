"""Power BI command orchestration — scan / build / find / show / reports / measures / source / owners.

Thin glue over the :mod:`dbt_meta.powerbi` core: pull a scanResult from the Admin
Scanner API, build the compact index against the dbt manifest, and query it. Each
function returns a JSON-serializable dict for the CLI; credentials are read from
``Config`` (env-populated) and the token never touches disk.
"""

from __future__ import annotations

from typing import Any

import orjson

from ..config import Config
from ..errors import DbtMetaError
from ..lineage.artifact import load_artifact as _load_lineage_artifact
from ..lineage.graph import LineageGraph, make_node_id
from ..powerbi.artifact import index_to_dict, load_index, save_index
from ..powerbi.index import PowerBiIndex, ReportEntry, build_index
from ..powerbi.query import find, reports_for_model, show
from ..powerbi.raw_reader import measures_for_report, owners_for_report, source_for_report
from ..powerbi.scanner import get_powerbi_token, scan_workspaces
from ..utils.monitoring import fetch_model_query_costs

__all__ = [
    "_load_lineage_graph",
    "artifacts_cmd",
    "build_index_artifact",
    "cost_cmd",
    "find_in_index",
    "lineage_cmd",
    "list_cmd",
    "measures_cmd",
    "owners_cmd",
    "reports_for_model_cmd",
    "scan_command",
    "show_report",
    "source_cmd",
]


def _load_json(path: str) -> dict[str, Any]:
    try:
        with open(path, "rb") as fh:
            data: dict[str, Any] = orjson.loads(fh.read())
            return data
    except FileNotFoundError as e:
        raise DbtMetaError(f"File not found: {path}") from e
    except orjson.JSONDecodeError as e:
        raise DbtMetaError(f"Invalid JSON in {path}: {e}") from e


def _scan_summary(scan_result: dict[str, Any]) -> dict[str, int]:
    workspaces = scan_result.get("workspaces") or []
    datasets = [d for ws in workspaces for d in (ws.get("datasets") or [])]
    reports = [r for ws in workspaces for r in (ws.get("reports") or [])]
    tables = [t for d in datasets for t in (d.get("tables") or [])]
    return {
        "workspaces": len(workspaces),
        "datasets": len(datasets),
        "reports": len(reports),
        "tables": len(tables),
    }


def scan_command(config: Config, out_path: str) -> dict[str, Any]:
    """Scan the configured workspaces and write the raw scanResult to ``out_path``."""
    if not (
        config.powerbi_tenant_id
        and config.powerbi_client_id
        and config.powerbi_client_secret
    ):
        raise DbtMetaError(
            "Power BI credentials not configured.",
            suggestion="Set POWERBI_TENANT_ID / POWERBI_CLIENT_ID / "
            "POWERBI_CLIENT_SECRET (env) or the [powerbi] config section.",
        )
    if not config.powerbi_workspaces:
        raise DbtMetaError(
            "No Power BI workspaces configured.",
            suggestion="Set POWERBI_WORKSPACES (comma-separated ids).",
        )

    token = get_powerbi_token(
        config.powerbi_tenant_id,
        config.powerbi_client_id,
        config.powerbi_client_secret,
    )
    if not token:
        raise DbtMetaError("Failed to obtain Power BI access token.")

    scan_result = scan_workspaces(token, config.powerbi_workspaces)
    if not scan_result:
        raise DbtMetaError(
            "Power BI scan failed or timed out.",
            suggestion="Check workspace ids and service-principal permissions.",
        )

    with open(out_path, "wb") as fh:
        fh.write(orjson.dumps(scan_result, option=orjson.OPT_INDENT_2))

    summary: dict[str, Any] = dict(_scan_summary(scan_result))
    summary["output"] = out_path
    return summary


def artifacts_cmd(
    config: Config,
    manifest_path: str,
    raw_path: str,
    index_path: str,
) -> dict[str, Any]:
    """Scan workspaces and build the compact index in one shot.

    Writes ``raw_path`` (raw scanResult) and ``index_path`` (compact index).
    Both default to ``~/dbt-state/`` in the CLI; passing explicit paths
    overwrites whatever is there — including cron-managed files.
    """
    scan_summary = scan_command(config, raw_path)
    build_summary = build_index_artifact(raw_path, manifest_path, index_path)
    return {
        "workspaces": scan_summary["workspaces"],
        "datasets": scan_summary["datasets"],
        "reports": build_summary["reports"],
        "tables": build_summary["tables"],
        "metrics": build_summary["metrics"],
        "raw_path": raw_path,
        "index_path": index_path,
    }


def build_index_artifact(
    raw_path: str, manifest_path: str, out_path: str
) -> dict[str, Any]:
    """Build the compact index from a raw scanResult + manifest, write to ``out_path``."""
    scan_result = _load_json(raw_path)
    manifest = _load_json(manifest_path)

    index = build_index(scan_result, manifest)
    save_index(index, out_path)

    table_count = len({t.bq for r in index.reports for t in r.tables})
    return {
        "reports": len(index.reports),
        "tables": table_count,
        "metrics": len(index.metric_index),
        "output": out_path,
    }


def _report_to_dict(report: ReportEntry) -> dict[str, Any]:
    full = index_to_dict(PowerBiIndex(reports=[report]))
    report_dict: dict[str, Any] = full["reports"][0]
    return report_dict


def find_in_index(artifact_path: str, query: str) -> dict[str, Any]:
    """Find reports / metrics matching ``query`` in the index artifact."""
    index = load_index(artifact_path)
    result = find(index, query)
    return {
        "query": query,
        "reports": [_report_to_dict(r) for r in result.reports],
        "metrics": result.metrics,
    }


def list_cmd(artifact_path: str) -> dict[str, Any]:
    """List every report in the index, sorted by workspace then report name.

    The flat enumeration answers "what reports exist?" so query commands
    (``show`` / ``cost`` / ``lineage`` / ``owners``) have a name to target.
    """
    index = load_index(artifact_path)
    reports = sorted(
        index.reports, key=lambda r: (r.workspace.lower(), r.report.lower())
    )
    return {
        "count": len(reports),
        "reports": [_report_to_dict(r) for r in reports],
    }


def show_report(artifact_path: str, report_name: str) -> dict[str, Any]:
    """Return the full breakdown of one report from the index artifact."""
    index = load_index(artifact_path)
    report = show(index, report_name)
    if report is None:
        raise DbtMetaError(
            f"Report not found: {report_name!r}",
            suggestion="Use `meta powerbi find <query>` to list matching reports.",
        )
    return _report_to_dict(report)


def cost_cmd(artifact_path: str, report_name: str) -> dict[str, Any]:
    """Return per-table query cost metrics (7-day) for the tables behind a report.

    Tables with ``status="model"`` are matched against the BQ monitoring data
    by dbt model name. External / source tables return ``null`` cost fields.
    """
    index = load_index(artifact_path)
    report = show(index, report_name)
    if report is None:
        raise DbtMetaError(
            f"Report not found: {report_name!r}",
            suggestion="Use `meta powerbi find <query>` to list matching reports.",
        )

    rows = fetch_model_query_costs() or []
    cost_by_model: dict[str, dict[str, Any]] = {
        str(r.get("dbt_model_name", "")): r for r in rows
    }

    tables = []
    for t in report.tables:
        row = cost_by_model.get(t.dbt_model or "") if t.dbt_model else None
        tables.append(
            {
                "bq": t.bq,
                "status": t.status,
                "dbt_model": t.dbt_model,
                "query_cost_usd": float(row["query_cost_usd"]) if row else None,
                "query_count": int(row["query_count"]) if row else None,
                "bytes_processed": int(row["bytes_processed"]) if row else None,
                "cache_hit_ratio": float(row["cache_hit_ratio"]) if row else None,
            }
        )

    return {"report": report.report, "tables": tables}


def _load_lineage_graph(path: str) -> LineageGraph:
    """Load lineage graph from artifact path. Separate function for monkeypatching in tests."""
    try:
        graph, _ = _load_lineage_artifact(path)
        return graph
    except FileNotFoundError as e:
        raise DbtMetaError(
            f"Lineage artifact not found: {path}",
            suggestion="Run `meta lineage build` to build the lineage artifact.",
        ) from e


def lineage_cmd(
    artifact_path: str,
    lineage_path: str,
    report_name: str,
) -> dict[str, Any]:
    """Return column-level upstream paths for filter/join columns in a report's SQL.

    For each column appearing in WHERE or JOIN conditions in the report's native
    SQL queries, looks up ancestors in the lineage graph. Uses the dbt model
    name from the PBI index to form the node id.

    Columns not present in the lineage graph are silently skipped.
    """
    index = load_index(artifact_path)
    report = show(index, report_name)
    if report is None:
        raise DbtMetaError(
            f"Report not found: {report_name!r}",
            suggestion="Use `meta powerbi find <query>` to list matching reports.",
        )

    graph = _load_lineage_graph(lineage_path)

    # Collect (dbt_model, bq_column) pairs from sql_analysis filters + joins.
    candidates: list[tuple[str, str, str]] = []  # (dbt_model, bq_column, bq_table)
    for entry in report.sql_analysis:
        for t in report.tables:
            if t.dbt_model and t.bq in entry.tables:
                for col in (*entry.filters, *entry.joins):
                    candidates.append((t.dbt_model, col, t.bq))

    seen: dict[tuple[str, str], None] = {}
    columns: list[dict[str, Any]] = []
    for dbt_model, bq_column, bq_table in candidates:
        key = (dbt_model, bq_column)
        if key in seen:
            continue
        seen[key] = None
        node_id = make_node_id(dbt_model, bq_column)
        if not graph.has_node(node_id):
            continue
        ancestors = graph.ancestors(node_id)
        columns.append(
            {
                "dbt_model": dbt_model,
                "bq_table": bq_table,
                "bq_column": bq_column,
                "ancestors": ancestors,
            }
        )

    return {"report": report.report, "columns": columns}


def reports_for_model_cmd(artifact_path: str, model_query: str) -> dict[str, Any]:
    """Reverse lookup: find all Power BI reports that use a given dbt model.

    Raises ``DbtMetaError`` when the query matches multiple distinct model names.
    """
    index = load_index(artifact_path)
    matches = reports_for_model(index, model_query)

    # Ambiguity check: if multiple distinct dbt_model values matched, refuse.
    needle = model_query.lower()
    matched_models: set[str] = set()
    for report, _ in matches:
        for t in report.tables:
            if t.dbt_model and needle in t.dbt_model.lower():
                matched_models.add(t.dbt_model)

    if len(matched_models) > 1:
        raise DbtMetaError(
            f"Ambiguous model query '{model_query}' — matches {len(matched_models)} models: "
            + ", ".join(sorted(matched_models))
            + ". Use a more specific query.",
        )

    resolved_model = next(iter(matched_models)) if matched_models else model_query

    return {
        "model": resolved_model,
        "reports": [
            {
                "workspace": report.workspace,
                "report": report.report,
                "dataset": report.dataset,
                "matched_tables": matched_bq,
            }
            for report, matched_bq in matches
        ],
    }


def measures_cmd(raw_path: str, report_name: str) -> dict[str, Any]:
    """Return all DAX measures for the dataset behind a report."""
    return measures_for_report(raw_path, report_name)


def source_cmd(raw_path: str, report_name: str) -> dict[str, Any]:
    """Return Power Query M-expressions for tables with a non-empty source."""
    return source_for_report(raw_path, report_name)


def owners_cmd(raw_path: str, report_name: str) -> dict[str, Any]:
    """Return Owner-level users and last-modified metadata for a report."""
    return owners_for_report(raw_path, report_name)
