"""Power BI command orchestration — scan / build / find / show.

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
from ..powerbi.artifact import index_to_dict, load_index, save_index
from ..powerbi.index import PowerBiIndex, ReportEntry, build_index
from ..powerbi.query import find, show
from ..powerbi.scanner import get_powerbi_token, scan_workspaces

__all__ = [
    "build_index_artifact",
    "find_in_index",
    "scan_command",
    "show_report",
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
