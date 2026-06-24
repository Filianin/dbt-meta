"""Persist and load the Power BI index as a JSON artifact.

Mirrors the column-level lineage artifact: ``build`` writes the parsed index once,
queries read it locally in milliseconds. Staleness is judged by file mtime (the
artifact is rebuilt by the Airflow DAG alongside the manifest).
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

import orjson

from .index import (
    FieldRef,
    FilterRef,
    PageEntry,
    PowerBiIndex,
    ReportEntry,
    SqlAnalysisEntry,
    TableRef,
    VisualEntry,
)


def _table_ref_to_dict(t: TableRef) -> dict[str, Any]:
    return {
        "bq": t.bq,
        "status": t.status,
        "dbt_model": t.dbt_model,
        "parse_status": t.parse_status,
    }


def _filter_ref_to_dict(f: FilterRef) -> dict[str, Any]:
    return {
        "table": f.table,
        "column": f.column,
        "kind": f.kind,
        "op": f.op,
        "values": list(f.values),
        "summary": f.summary,
    }


def _filter_ref_from_dict(f: dict[str, Any]) -> FilterRef:
    return FilterRef(
        table=f.get("table", ""),
        column=f.get("column", ""),
        kind=f.get("kind", "column"),
        op=f.get("op", "advanced"),
        values=list(f.get("values") or []),
        summary=f.get("summary", ""),
    )


def _visual_to_dict(v: VisualEntry) -> dict[str, Any]:
    out: dict[str, Any] = {
        "type": v.type,
        "fields": {
            role: [{"table": f.table, "column": f.column, "kind": f.kind} for f in refs]
            for role, refs in v.fields.items()
        },
    }
    # Sparse: emit title/filters only when present.
    if v.title is not None:
        out["title"] = v.title
    if v.filters:
        out["filters"] = [_filter_ref_to_dict(f) for f in v.filters]
    return out


def _visual_from_dict(v: dict[str, Any]) -> VisualEntry:
    return VisualEntry(
        type=v.get("type", "unknown"),
        fields={
            role: [
                FieldRef(
                    table=f.get("table", ""),
                    column=f.get("column", ""),
                    kind=f.get("kind", "column"),
                )
                for f in refs
            ]
            for role, refs in (v.get("fields") or {}).items()
        },
        title=v.get("title"),
        filters=[_filter_ref_from_dict(f) for f in (v.get("filters") or [])],
    )


def _page_to_dict(p: PageEntry) -> dict[str, Any]:
    out: dict[str, Any] = {
        "name": p.name,
        "visuals": [_visual_to_dict(v) for v in p.visuals],
    }
    if p.filters:
        out["filters"] = [_filter_ref_to_dict(f) for f in p.filters]
    return out


def _page_from_dict(p: dict[str, Any]) -> PageEntry:
    return PageEntry(
        name=p.get("name", ""),
        visuals=[_visual_from_dict(v) for v in p.get("visuals", [])],
        filters=[_filter_ref_from_dict(f) for f in (p.get("filters") or [])],
    )


def _sql_entry_to_dict(s: SqlAnalysisEntry) -> dict[str, Any]:
    return {
        "query": s.query,
        "tables": list(s.tables),
        "filters": list(s.filters),
        "joins": list(s.joins),
        "group_by": list(s.group_by),
        "parse_status": s.parse_status,
        "sql": s.sql,
    }


def index_to_dict(index: PowerBiIndex) -> dict[str, Any]:
    return {
        "schema_version": index.schema_version,
        "generated_at": index.generated_at,
        "reports": [
            {
                "workspace": r.workspace,
                "report": r.report,
                "dataset": r.dataset,
                "tables": [_table_ref_to_dict(t) for t in r.tables],
                "sql_analysis": [_sql_entry_to_dict(s) for s in r.sql_analysis],
                # Sparse: emit `pages` only when layout was retrieved (index
                # style — absence means "no PBIR layout", not "no pages").
                **(
                    {"pages": [_page_to_dict(p) for p in r.pages]}
                    if r.pages
                    else {}
                ),
                # Sparse: report-level filters only when present.
                **(
                    {"filters": [_filter_ref_to_dict(f) for f in r.filters]}
                    if r.filters
                    else {}
                ),
            }
            for r in index.reports
        ],
        "metric_index": index.metric_index,
    }


def index_from_dict(data: dict[str, Any]) -> PowerBiIndex:
    reports = [
        ReportEntry(
            workspace=r.get("workspace", ""),
            report=r.get("report", ""),
            dataset=r.get("dataset", ""),
            tables=[
                TableRef(
                    bq=t.get("bq", ""),
                    status=t.get("status", "external"),
                    dbt_model=t.get("dbt_model"),
                    parse_status=t.get("parse_status", "ok"),
                )
                for t in r.get("tables", [])
            ],
            sql_analysis=[
                SqlAnalysisEntry(
                    query=s.get("query", ""),
                    tables=tuple(s.get("tables", [])),
                    filters=tuple(s.get("filters", [])),
                    joins=tuple(s.get("joins", [])),
                    group_by=tuple(s.get("group_by", [])),
                    parse_status=s.get("parse_status", "ok"),
                    sql=s.get("sql"),
                )
                for s in r.get("sql_analysis", [])
            ],
            pages=[_page_from_dict(p) for p in r.get("pages", [])],
            filters=[_filter_ref_from_dict(f) for f in (r.get("filters") or [])],
        )
        for r in data.get("reports", [])
    ]
    return PowerBiIndex(
        reports=reports,
        metric_index=data.get("metric_index", {}),
        generated_at=data.get("generated_at", ""),
        schema_version=data.get("schema_version", "1.0"),
    )


def save_index(index: PowerBiIndex, path: str) -> None:
    """Write the index to ``path`` as JSON."""
    payload = orjson.dumps(index_to_dict(index), option=orjson.OPT_INDENT_2)
    with open(path, "wb") as fh:
        fh.write(payload)


def load_index(path: str) -> PowerBiIndex:
    """Load an index from a JSON artifact."""
    with open(path, "rb") as fh:
        data = orjson.loads(fh.read())
    return index_from_dict(data)


def artifact_age_hours(path: str) -> float | None:
    """Hours since the artifact was last modified, or ``None`` if missing."""
    if not os.path.exists(path):
        return None
    return (time.time() - os.path.getmtime(path)) / 3600.0


def find_powerbi_raw(explicit_path: str | None = None) -> str:
    """Locate ``powerbi_raw.json`` (mirrors ``find_powerbi_artifact`` priority).

    Priority: explicit path → ``DBT_PROD_POWERBI_RAW_PATH`` →
    ``~/dbt-state/powerbi_raw.json``.
    """
    if explicit_path:
        path = Path(explicit_path).expanduser()
        if path.exists():
            return str(path.absolute())
        raise FileNotFoundError(f"Power BI raw artifact not found at: {explicit_path}")

    env = os.getenv("DBT_PROD_POWERBI_RAW_PATH")
    if env:
        env_path = Path(env).expanduser()
        if env_path.exists():
            return str(env_path.absolute())
        raise FileNotFoundError(
            f"Power BI raw artifact not found at: {env}\n"
            f"DBT_PROD_POWERBI_RAW_PATH is set but file doesn't exist."
        )

    default_prod = Path.home() / "dbt-state" / "powerbi_raw.json"
    if default_prod.exists():
        return str(default_prod.absolute())

    raise FileNotFoundError(
        "No powerbi_raw.json found. Tried:\n"
        "  1. DBT_PROD_POWERBI_RAW_PATH (env)\n"
        "  2. ~/dbt-state/powerbi_raw.json\n"
        "\n"
        "BUILD ARTIFACT:\n"
        "  meta powerbi scan      # writes ~/dbt-state/powerbi_raw.json\n"
    )


def find_powerbi_artifact(explicit_path: str | None = None) -> str:
    """Locate ``powerbi_index.json`` (mirrors the lineage-artifact priority).

    The Power BI index is a prod-only artifact. Priority: explicit path →
    ``DBT_PROD_POWERBI_PATH`` → ``~/dbt-state/powerbi_index.json``.
    """
    if explicit_path:
        path = Path(explicit_path).expanduser()
        if path.exists():
            return str(path.absolute())
        raise FileNotFoundError(f"Power BI index not found at: {explicit_path}")

    env = os.getenv("DBT_PROD_POWERBI_PATH")
    if env:
        env_path = Path(env).expanduser()
        if env_path.exists():
            return str(env_path.absolute())
        raise FileNotFoundError(
            f"Power BI index not found at: {env}\n"
            f"DBT_PROD_POWERBI_PATH is set but file doesn't exist."
        )

    default_prod = Path.home() / "dbt-state" / "powerbi_index.json"
    if default_prod.exists():
        return str(default_prod.absolute())

    raise FileNotFoundError(
        "No powerbi_index.json found. Tried:\n"
        "  1. DBT_PROD_POWERBI_PATH (env)\n"
        "  2. ~/dbt-state/powerbi_index.json\n"
        "\n"
        "BUILD ARTIFACT:\n"
        "  meta powerbi scan      # writes ~/dbt-state/powerbi_raw.json\n"
        "  meta powerbi build     # writes ~/dbt-state/powerbi_index.json\n"
    )
