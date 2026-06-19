"""Read rich metadata from powerbi_raw.json — measures, source, owners.

The raw scanResult is the full artifact produced by ``meta powerbi scan``. The
compact index (powerbi_index.json) covers navigation; this module covers depth:
DAX measure expressions, Power Query M-expressions, and report access/ownership.

Report lookup uses the same exact-then-substring strategy as ``query.show``.
Multi-match raises ``DbtMetaError`` so the caller can refine the query.
"""

from __future__ import annotations

from typing import Any

import orjson

from ..errors import DbtMetaError
from .dax import parse_dax_refs


def _load_raw(path: str) -> dict[str, Any]:
    try:
        with open(path, "rb") as fh:
            data: dict[str, Any] = orjson.loads(fh.read())
            return data
    except FileNotFoundError as e:
        raise DbtMetaError(f"powerbi_raw.json not found: {path}") from e
    except orjson.JSONDecodeError as e:
        raise DbtMetaError(f"Invalid JSON in {path}: {e}") from e


def _all_reports(raw: dict[str, Any]) -> list[dict[str, Any]]:
    return [r for ws in (raw.get("workspaces") or []) for r in (ws.get("reports") or [])]


def _find_report(raw: dict[str, Any], report_name: str) -> dict[str, Any]:
    """Return the single report entry matching report_name, or raise."""
    needle = report_name.lower()
    reports = _all_reports(raw)

    exact = [r for r in reports if r.get("name", "").lower() == needle]
    if len(exact) == 1:
        return exact[0]
    if len(exact) > 1:
        names = ", ".join(r.get("name", "") for r in exact)
        raise DbtMetaError(
            f"Ambiguous report name '{report_name}' — {len(exact)} matches: {names}. "
            "Use a more specific query."
        )

    partial = [r for r in reports if needle in r.get("name", "").lower()]
    if len(partial) == 1:
        return partial[0]
    if len(partial) > 1:
        names = ", ".join(r.get("name", "") for r in partial)
        raise DbtMetaError(
            f"Ambiguous report query '{report_name}' — {len(partial)} matches: {names}. "
            "Use a more specific query."
        )

    raise DbtMetaError(
        f"Report '{report_name}' not found in raw artifact.",
        suggestion="Use `meta powerbi find <query>` to list available reports.",
    )


def _find_dataset(raw: dict[str, Any], dataset_id: str) -> dict[str, Any] | None:
    for ws in raw.get("workspaces") or []:
        for ds in ws.get("datasets") or []:
            if ds.get("id") == dataset_id:
                dataset: dict[str, Any] = ds
                return dataset
    return None


def measures_for_report(
    raw_path: str, report_name: str
) -> dict[str, Any]:
    """Return all DAX measures for the dataset behind report_name."""
    raw = _load_raw(raw_path)
    report = _find_report(raw, report_name)
    dataset = _find_dataset(raw, report.get("datasetId", ""))

    measures: list[dict[str, Any]] = []
    if dataset:
        for tbl in dataset.get("tables") or []:
            tbl_name = tbl.get("name", "")
            for m in tbl.get("measures") or []:
                expression = m.get("expression", "")
                measures.append(
                    {
                        "table": tbl_name,
                        "name": m.get("name", ""),
                        "expression": expression,
                        "hidden": bool(m.get("isHidden", False)),
                        "dax_refs": parse_dax_refs(expression),
                    }
                )

    return {"report": report.get("name", report_name), "measures": measures}


def source_for_report(
    raw_path: str, report_name: str
) -> dict[str, Any]:
    """Return M-expressions for tables that have a non-empty source."""
    raw = _load_raw(raw_path)
    report = _find_report(raw, report_name)
    dataset = _find_dataset(raw, report.get("datasetId", ""))

    sources: list[dict[str, Any]] = []
    if dataset:
        for tbl in dataset.get("tables") or []:
            expr = ""
            for src in tbl.get("source") or []:
                if src.get("expression"):
                    expr = src["expression"]
                    break
            if expr:
                sources.append({"table": tbl.get("name", ""), "expression": expr})

    return {"report": report.get("name", report_name), "sources": sources}


def owners_for_report(
    raw_path: str, report_name: str
) -> dict[str, Any]:
    """Return Owner-level users and last-modified metadata for a report."""
    raw = _load_raw(raw_path)
    report = _find_report(raw, report_name)

    owners = [
        u.get("displayName", "")
        for u in (report.get("users") or [])
        if u.get("reportUserAccessRight") == "Owner"
    ]

    return {
        "report": report.get("name", report_name),
        "modified_by": report.get("modifiedBy"),
        "modified_at": report.get("modifiedDateTime"),
        "owners": owners,
    }
