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

from .index import FieldRef, FilterRef, PageEntry, VisualEntry

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

# PBI ComparisonKind enum → operator symbol.
_CMP_SYMBOLS = {0: "=", 1: ">", 2: ">=", 3: "<", 4: "<="}

# PBI Aggregation Function enum → label used in TopN "by" / advanced summaries.
_AGG_FUNCS = {
    0: "Sum",
    1: "Avg",
    2: "Count",
    3: "Min",
    4: "Max",
    5: "CountNonNull",
    6: "Median",
    7: "StdDev",
    8: "Var",
}

# PBI relative-date TimeUnit enum → human unit (best-effort subset).
_TIME_UNITS = {0: "days", 1: "weeks", 3: "months", 5: "years"}

# A numeric literal with PBI's type suffix: ``100L`` (int), ``3.14D`` (double).
_NUM_LITERAL = re.compile(r"^(-?\d+(?:\.\d+)?)[LDMF]?$")


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


def _clean_literal(raw: Any) -> str:
    """Strip PBI's literal packaging: ``'text'`` → ``text``, ``100L`` → ``100``."""
    s = str(raw)
    if len(s) >= 2 and s[0] == "'" and s[-1] == "'":
        return s[1:-1]
    m = _NUM_LITERAL.match(s)
    if m:
        return m.group(1)
    return s


def _entity_prop(col: dict[str, Any], kind: str) -> tuple[str, str, str] | None:
    """Extract ``(entity, property, kind)`` from a Column/Measure node."""
    prop = col.get("Property")
    if not prop:
        return None
    expr = col.get("Expression")
    sref = expr.get("SourceRef", {}) if isinstance(expr, dict) else {}
    entity = sref.get("Entity", "") or sref.get("Source", "")
    return str(entity), str(prop), kind


def _unwrap_column_like(node: Any) -> tuple[str, str, str] | None:
    """Resolve a Column / Measure / Aggregation / HierarchyLevel field expression."""
    if not isinstance(node, dict):
        return None
    if isinstance(node.get("Column"), dict):
        return _entity_prop(node["Column"], "column")
    if isinstance(node.get("Measure"), dict):
        return _entity_prop(node["Measure"], "measure")
    if isinstance(node.get("Aggregation"), dict):
        inner = _unwrap_column_like(node["Aggregation"].get("Expression"))
        if inner:
            return inner[0], inner[1], "measure"
        return None
    if isinstance(node.get("HierarchyLevel"), dict):
        hl = node["HierarchyLevel"]
        level = hl.get("Level") or ""
        hier = hl.get("Expression", {}).get("Hierarchy", {})
        sref = hier.get("Expression", {}).get("SourceRef", {}) if isinstance(hier, dict) else {}
        entity = sref.get("Entity", "") or sref.get("Source", "") if isinstance(sref, dict) else ""
        return str(entity), str(level), "column"
    return None


def _expr_label(expr: Any) -> str | None:
    """A short label for a measure/column expression: ``Sum(revenue)`` / ``amount``."""
    if not isinstance(expr, dict):
        return None
    if isinstance(expr.get("Aggregation"), dict):
        agg = expr["Aggregation"]
        func = _AGG_FUNCS.get(agg.get("Function"), "Agg")
        inner = _unwrap_column_like(agg.get("Expression"))
        return f"{func}({inner[1]})" if inner else f"{func}(?)"
    field_ = _unwrap_column_like(expr)
    return field_[1] if field_ else None


def _literal_values_of_in(in_node: dict[str, Any]) -> list[str]:
    vals: list[str] = []
    for grp in in_node.get("Values") or []:
        if not isinstance(grp, list):
            continue
        for item in grp:
            if isinstance(item, dict) and isinstance(item.get("Literal"), dict):
                v = item["Literal"].get("Value")
                if v is not None:
                    vals.append(_clean_literal(v))
    return vals


def _render_condition(cond: Any) -> str:
    """Best-effort human rendering of a Where condition (recursive on And/Or/Not)."""
    if not isinstance(cond, dict):
        return ""
    if isinstance(cond.get("And"), dict):
        node = cond["And"]
        return f"{_render_condition(node.get('Left'))} and {_render_condition(node.get('Right'))}".strip()
    if isinstance(cond.get("Or"), dict):
        node = cond["Or"]
        return f"{_render_condition(node.get('Left'))} or {_render_condition(node.get('Right'))}".strip()
    if isinstance(cond.get("Not"), dict):
        return f"not {_render_condition(cond['Not'].get('Expression'))}".strip()
    if isinstance(cond.get("Comparison"), dict):
        comp = cond["Comparison"]
        left = _expr_label(comp.get("Left")) or "?"
        symbol = _CMP_SYMBOLS.get(comp.get("ComparisonKind"), "?")
        right = comp.get("Right")
        bound = (
            _clean_literal(right["Literal"].get("Value", ""))
            if isinstance(right, dict) and isinstance(right.get("Literal"), dict)
            else "?"
        )
        return f"{left} {symbol} {bound}"
    if isinstance(cond.get("In"), dict):
        in_node = cond["In"]
        exprs = in_node.get("Expressions") or []
        label = _expr_label(exprs[0]) if exprs else None
        vals = _literal_values_of_in(in_node)
        return f"{label or '?'} in ({', '.join(vals)})"
    return ""


def _find_datespan(node: Any) -> tuple[str, str] | None:
    """Recursively find a DateSpan → ``(count, unit)`` for relative-date filters."""
    if isinstance(node, dict):
        ds = node.get("DateSpan")
        if isinstance(ds, dict):
            expr = ds.get("Expression")
            lit = expr.get("Literal") if isinstance(expr, dict) else None
            val = lit.get("Value") if isinstance(lit, dict) else None
            if val is not None:
                code = ds.get("TimeUnit")
                unit = (
                    _TIME_UNITS.get(code, f"unit_{code}")
                    if isinstance(code, int)
                    else f"unit_{code}"
                )
                return _clean_literal(val), unit
        for v in node.values():
            found = _find_datespan(v)
            if found:
                return found
    elif isinstance(node, list):
        for v in node:
            found = _find_datespan(v)
            if found:
                return found
    return None


def _find_int_literal(node: Any) -> str | None:
    """Recursively find the first integer literal value (for TopN item count)."""
    if isinstance(node, dict):
        lit = node.get("Literal")
        if isinstance(lit, dict):
            raw = str(lit.get("Value", ""))
            m = _NUM_LITERAL.match(raw)
            if m and "." not in m.group(1):
                return m.group(1)
        for v in node.values():
            found = _find_int_literal(v)
            if found:
                return found
    elif isinstance(node, list):
        for v in node:
            found = _find_int_literal(v)
            if found:
                return found
    return None


def _find_orderby_label(filt: dict[str, Any]) -> str | None:
    for ob in filt.get("OrderBy") or []:
        if isinstance(ob, dict):
            label = _expr_label(ob.get("Expression"))
            if label:
                return label
    return None


def _flatten_literals(node: Any) -> list[str]:
    """Recursively collect every literal value (cleaned) under ``node``."""
    out: list[str] = []
    if isinstance(node, dict):
        lit = node.get("Literal")
        if isinstance(lit, dict) and lit.get("Value") is not None:
            out.append(_clean_literal(lit["Value"]))
        for v in node.values():
            out.extend(_flatten_literals(v))
    elif isinstance(node, list):
        for v in node:
            out.extend(_flatten_literals(v))
    return out


def _conditions_of(filt: dict[str, Any]) -> list[dict[str, Any]]:
    conditions: list[dict[str, Any]] = []
    for w in filt.get("Where") or []:
        if isinstance(w, dict) and isinstance(w.get("Condition"), dict):
            conditions.append(w["Condition"])
    return conditions


def _classify_filter(
    ftype: str, filt: dict[str, Any], column: str
) -> tuple[str, list[str], str]:
    """Map a PBI filter's ``type`` + ``Where`` → ``(op, values, summary)`` (decision #7)."""
    if ftype in ("RelativeDate", "RelativeTime"):
        ds = _find_datespan(filt)
        if ds:
            count, unit = ds
            return "relative_date", [count, unit], f"{column} — last {count} {unit}"
        return "relative_date", [], f"{column} — relative date"

    if ftype == "TopN":
        n = _find_int_literal(filt.get("Where"))
        by = _find_orderby_label(filt)
        values = [n] if n else []
        if n and by:
            summary = f"top {n} by {by}"
        elif n:
            summary = f"top {n}"
        else:
            summary = f"top N on {column}"
        return "top_n", values, summary

    conditions = _conditions_of(filt)

    # Single In condition → categorical membership.
    in_nodes = [c["In"] for c in conditions if isinstance(c.get("In"), dict)]
    if in_nodes and len(conditions) == 1:
        vals = _literal_values_of_in(in_nodes[0])
        return "in", vals, f"{column} in ({', '.join(vals)})"

    # Single Comparison condition → simple comparison.
    comps = [c["Comparison"] for c in conditions if isinstance(c.get("Comparison"), dict)]
    if len(comps) == 1 and len(conditions) == 1:
        comp = comps[0]
        symbol = _CMP_SYMBOLS.get(comp.get("ComparisonKind"), "?")
        right = comp.get("Right")
        bound = (
            _clean_literal(right["Literal"].get("Value", ""))
            if isinstance(right, dict) and isinstance(right.get("Literal"), dict)
            else ""
        )
        values = [symbol, bound] if bound else [symbol]
        return "cmp", values, f"{column} {symbol} {bound}".strip()

    # Anything else (And/Or trees, mixed conditions) → advanced, best-effort.
    rendered = " and ".join(s for s in (_render_condition(c) for c in conditions) if s)
    return "advanced", _flatten_literals(filt.get("Where")), rendered or f"{column} (advanced)"


def _parse_one_filter(f: dict[str, Any], measures: frozenset[str]) -> FilterRef | None:
    """Parse one PBI filter object → :class:`FilterRef`, or ``None`` to skip."""
    field_ = _unwrap_column_like(f.get("expression"))
    if not field_:
        return None
    table, column, kind = field_
    if kind == "column" and column in measures:
        kind = "measure"
    ftype = str(f.get("type") or "")
    filt = f.get("filter")
    op, values, summary = _classify_filter(
        ftype, filt if isinstance(filt, dict) else {}, column
    )
    return FilterRef(
        table=table, column=column, kind=kind, op=op, values=values, summary=summary
    )


def _parse_filters(raw: Any, measures: frozenset[str]) -> list[FilterRef]:
    """Parse a PBIR ``filters`` field (JSON-string or list) → ``[FilterRef]``.

    Failure-isolated: a malformed filter degrades (is skipped) and never raises,
    so one bad filter can't sink a report's whole layout (decision #6).
    """
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            return []
    elif isinstance(raw, list):
        data = raw
    else:
        return []
    out: list[FilterRef] = []
    for f in data:
        if not isinstance(f, dict):
            continue
        try:
            ref = _parse_one_filter(f, measures)
        except (KeyError, TypeError, ValueError, AttributeError, IndexError):
            ref = None
        if ref:
            out.append(ref)
    return out


def _extract_title(single: dict[str, Any]) -> str | None:
    """Explicit visual title from ``vcObjects.title[0]…Literal.Value``, else ``None``."""
    vc = single.get("vcObjects")
    if not isinstance(vc, dict):
        return None
    titles = vc.get("title")
    if not isinstance(titles, list) or not titles:
        return None
    first = titles[0]
    if not isinstance(first, dict):
        return None
    props = first.get("properties")
    text = props.get("text") if isinstance(props, dict) else None
    expr = text.get("expr") if isinstance(text, dict) else None
    lit = expr.get("Literal") if isinstance(expr, dict) else None
    val = lit.get("Value") if isinstance(lit, dict) else None
    if val is None:
        return None
    return _clean_literal(val)


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

    title = _extract_title(single)
    filters = _parse_filters(container.get("filters"), measures)
    return VisualEntry(type=visual_type, fields=fields, title=title, filters=filters)


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
        page_filters = _parse_filters(section.get("filters"), measure_set)
        pages.append(PageEntry(name=name, visuals=visuals, filters=page_filters))
    return pages


def parse_report_filters(
    report_json: dict[str, Any], measures: Iterable[str] = ()
) -> list[FilterRef]:
    """Parse report-level filters (top-level ``filters``) → ``[FilterRef]``.

    Separate from :func:`parse_pbir_legacy` (which returns pages) because
    report-scope filters attach to the :class:`~dbt_meta.powerbi.index.ReportEntry`,
    not to any page. Failure-isolated like every other parse here.
    """
    return _parse_filters(report_json.get("filters"), frozenset(measures))
