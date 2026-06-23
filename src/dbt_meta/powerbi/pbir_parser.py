"""Parse a PBIR-Legacy ``report.json`` into per-page visual→field structure.

The Fabric ``getDefinition?format=PBIR-Legacy`` endpoint returns the classic
single-file layout: ``sections[]`` (pages) → ``visualContainers[]`` → a
JSON-string ``config`` carrying ``singleVisual.visualType`` and
``singleVisual.projections`` (fields grouped BY ROLE). This module turns that
into :class:`PageEntry` / :class:`VisualEntry` / :class:`FieldRef`.

queryRefs are dotted model names (``Table.Column``) optionally wrapped in an
aggregation (``Sum(Table.Column)``). The parser splits the *table.column* part
and classifies each ref as ``measure`` or ``column`` (decision #9): an
aggregation wrapper is a measure; a bare ref whose name is in the dataset's
measure list is a measure; everything else is a column.

Pure parsing — no I/O, no network. Failure-tolerant: malformed containers are
skipped, never raised, so one bad visual can't sink a report's whole layout.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable
from typing import Any

from .index import FieldRef, PageEntry, VisualEntry

# Chart-internal PBIR roles → a small uniform vocabulary so an agent reads
# fields the same way across visual types (a barChart ``Y`` and a funnel
# ``Values`` are both ``values``). Unknown roles fall through unchanged (#8).
_CANONICAL_ROLES = {
    "Category": "axis",
    "Axis": "axis",
    "X": "axis",
    "Columns": "axis",
    "Rows": "axis",
    "Y": "values",
    "Y2": "values",
    "Value": "values",
    "Values": "values",
    "Series": "legend",
    "Legend": "legend",
    "Tooltips": "tooltip",
    "Tooltip": "tooltip",
    "Size": "size",
    "Details": "details",
}

# An aggregation-wrapped ref: ``Sum(Table.Column)``, ``CountNonNull(...)``, etc.
_AGG_WRAPPER = re.compile(r"^[A-Za-z][A-Za-z0-9]*\((.*)\)$", re.DOTALL)


def _canonical_role(role: str) -> str:
    return _CANONICAL_ROLES.get(role, role)


def _split_table_column(ref: str) -> tuple[str, str]:
    """Split a ``Table.Column`` ref on its last dot. No dot → ('', ref)."""
    idx = ref.rfind(".")
    if idx == -1:
        return "", ref
    return ref[:idx], ref[idx + 1 :]


def _parse_query_ref(query_ref: str, measures: frozenset[str]) -> FieldRef | None:
    """Turn a queryRef string into a :class:`FieldRef`, or ``None`` if empty."""
    ref = (query_ref or "").strip()
    if not ref:
        return None

    wrapped = _AGG_WRAPPER.match(ref)
    if wrapped:
        table, column = _split_table_column(wrapped.group(1).strip())
        return FieldRef(table=table, column=column, kind="measure")

    table, column = _split_table_column(ref)
    kind = "measure" if column in measures else "column"
    return FieldRef(table=table, column=column, kind=kind)


def _parse_visual(
    container: dict[str, Any], measures: frozenset[str]
) -> VisualEntry | None:
    """Parse one ``visualContainer`` → :class:`VisualEntry`, or ``None`` to skip."""
    raw_config = container.get("config")
    if not isinstance(raw_config, str):
        return None
    try:
        config = json.loads(raw_config)
    except (json.JSONDecodeError, ValueError):
        return None

    single = config.get("singleVisual")
    if not isinstance(single, dict):
        return None

    visual_type = single.get("visualType") or "unknown"
    projections = single.get("projections") or {}
    if not isinstance(projections, dict):
        projections = {}

    fields: dict[str, list[FieldRef]] = {}
    for role, items in projections.items():
        if not isinstance(items, list):
            continue
        refs = [
            fr
            for it in items
            if isinstance(it, dict)
            and (fr := _parse_query_ref(it.get("queryRef", ""), measures))
        ]
        if refs:
            fields.setdefault(_canonical_role(role), []).extend(refs)

    return VisualEntry(type=visual_type, fields=fields)


def parse_pbir_legacy(
    report_json: dict[str, Any], measures: Iterable[str] = ()
) -> list[PageEntry]:
    """Parse a decoded PBIR-Legacy ``report.json`` into a list of pages.

    ``measures`` is the dataset's defined measure names (for the bare-measure vs
    bare-column distinction). Pages with no parseable visuals are still kept so
    the page roster stays complete; malformed visuals are dropped.
    """
    measure_set = frozenset(measures)
    pages: list[PageEntry] = []
    for section in report_json.get("sections") or []:
        if not isinstance(section, dict):
            continue
        name = section.get("displayName") or section.get("name") or ""
        visuals = [
            v
            for vc in (section.get("visualContainers") or [])
            if isinstance(vc, dict) and (v := _parse_visual(vc, measure_set))
        ]
        pages.append(PageEntry(name=name, visuals=visuals))
    return pages
