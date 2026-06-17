"""Analyze a Power BI native BigQuery SQL string.

Operates on physical ``project.schema.table`` identifiers (not dbt ``unique_id``),
since Power BI native queries hit BigQuery directly. Extracts the table set plus a
coarse picture of what the query does — WHERE filter columns, JOIN-ON columns and
GROUP BY columns — so the agent understands the logic living outside dbt.

SQLGlot (BigQuery dialect) handles hyphenated project ids natively, so no backtick
preprocessing is needed. On a parse error we fall back to a regex table sweep and
mark ``parse_status="partial"`` — tables are never lost silently.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from sqlglot import exp, parse_one
from sqlglot.errors import SqlglotError


@dataclass(frozen=True)
class SqlAnalysis:
    """Coarse analysis of one native SQL query."""

    tables: tuple[str, ...] = ()
    filters: tuple[str, ...] = ()  # columns referenced in WHERE
    joins: tuple[str, ...] = ()  # columns referenced in JOIN ... ON
    group_by: tuple[str, ...] = ()  # columns in GROUP BY
    parse_status: str = "ok"  # ok | partial


# Backtick-quoted or bare 3-part BigQuery name, project id may contain hyphens.
_TABLE_RE = re.compile(
    r"`?([A-Za-z0-9_-]+)`?\.`?([A-Za-z0-9_]+)`?\.`?([A-Za-z0-9_]+)`?"
)


def _table_name(table: exp.Table) -> str:
    parts = [
        p.name
        for p in (table.args.get("catalog"), table.args.get("db"), table.this)
        if p is not None
    ]
    return ".".join(parts)


def _column_names(node: exp.Expression | None) -> list[str]:
    if node is None:
        return []
    return [c.name for c in node.find_all(exp.Column)]


def _regex_tables(sql: str) -> tuple[str, ...]:
    seen: dict[str, None] = {}
    for m in _TABLE_RE.finditer(sql):
        seen[".".join(m.groups())] = None
    return tuple(seen)


def analyze_sql(sql: str) -> SqlAnalysis:
    """Parse a native BigQuery SQL string into a :class:`SqlAnalysis`."""
    try:
        tree = parse_one(sql, dialect="bigquery")
    except (SqlglotError, RecursionError):
        return SqlAnalysis(tables=_regex_tables(sql), parse_status="partial")

    # Keep only qualified names (>= schema.table); bare single-part names are CTE
    # references / derived-table aliases, not physical BigQuery tables.
    tables = tuple(
        dict.fromkeys(
            name
            for t in tree.find_all(exp.Table)
            if (name := _table_name(t)).count(".") >= 1
        )
    )

    filters: list[str] = []
    for where in tree.find_all(exp.Where):
        filters.extend(_column_names(where.this))

    joins: list[str] = []
    for join in tree.find_all(exp.Join):
        joins.extend(_column_names(join.args.get("on")))

    group_by: list[str] = []
    for group in tree.find_all(exp.Group):
        for e in group.expressions:
            group_by.extend(_column_names(e))

    return SqlAnalysis(
        tables=tables,
        filters=tuple(dict.fromkeys(filters)),
        joins=tuple(dict.fromkeys(joins)),
        group_by=tuple(dict.fromkeys(group_by)),
        parse_status="ok",
    )
