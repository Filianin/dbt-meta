"""Parse Power BI Power Query (M) expressions into a classified source description.

An M-expression behind a Power BI table is one of a few shapes:

- **navigation** — a 1:1 import that walks ``GoogleBigQuery.Database()`` through
  ``[Name=...,Kind="Schema"]`` then ``[Name=...,Kind="Table"/"View"]`` steps. The
  fully-qualified ``project.schema.table`` is reconstructed from those steps.
- **native_sql** — ``Value.NativeQuery(GoogleBigQuery.Database(){...}, "SELECT ...")``;
  the SQL string is decoded (M escapes) and handed to the SQL analyzer downstream.
- **inline** — ``Table.FromRows`` constant, no BigQuery source.
- **dax** — a DAX calculated table (no M ``let`` source).

Only navigation is handled so far (tracer bullet).
"""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class MSource:
    """Classified result of parsing one M-expression."""

    kind: str  # navigation | native_sql | inline | dax | unknown
    tables: tuple[str, ...] = ()
    native_sql: str | None = None
    cross_query_refs: tuple[str, ...] = ()
    parse_status: str = "ok"  # ok | partial


# A navigation step: {[Name="x",Kind="Schema"]} or {[Name="x"]} (project id step).
_NAV_STEP = re.compile(r'\{\[Name="(?P<name>[^"]+)"(?:,Kind="(?P<kind>[^"]+)")?\]\}')

# The SQL string literal that is the 2nd positional arg of Value.NativeQuery.
_NATIVE_SQL = re.compile(
    r'Value\.NativeQuery\(\s*GoogleBigQuery\.Database\(\).*?\[Data\]\s*,\s*"(?P<sql>(?:[^"\\]|\\.)*)"',
    re.DOTALL,
)

# Table.NestedJoin(left, keys, <other_query>, keys, name, kind) — 3rd arg is a
# reference to another in-dataset M-query (not a BigQuery table). The reference is
# either a bare identifier or a #"quoted name" (names with spaces).
_REF = r'(?:#"[^"]*"|[A-Za-z_]\w*)'
_NESTED_JOIN = re.compile(
    rf'Table\.NestedJoin\(\s*{_REF}\s*,\s*\{{[^}}]*\}}\s*,\s*(?P<ref>{_REF})\s*,'
)


def _ref_name(ref: str) -> str:
    """Strip the #"..." quoting from an M-query reference, if present."""
    if ref.startswith('#"') and ref.endswith('"'):
        return ref[2:-1]
    return ref


def _decode_m_string(s: str) -> str:
    """Decode M string escapes into a plain SQL string."""
    return (
        s.replace("#(lf)", "\n")
        .replace("#(tab)", "\t")
        .replace('\\"', '"')
    )


def parse_m_expression(expr: str) -> MSource:
    """Classify an M-expression and extract its BigQuery source(s)."""
    native = _NATIVE_SQL.search(expr)
    if native:
        sql = _decode_m_string(native.group("sql"))
        refs = tuple(dict.fromkeys(_ref_name(r) for r in _NESTED_JOIN.findall(expr)))
        return MSource(kind="native_sql", native_sql=sql, cross_query_refs=refs)

    if "GoogleBigQuery.Database" in expr:
        steps = _NAV_STEP.findall(expr)
        names = [name for name, _ in steps]
        if len(names) >= 3:
            table = ".".join(names[:3])
            return MSource(kind="navigation", tables=(table,))

    if "Table.FromRows" in expr:
        return MSource(kind="inline")

    # M expressions start with `let`; anything else is a DAX calculated table.
    if not expr.lstrip().startswith("let"):
        return MSource(kind="dax")

    return MSource(kind="unknown", parse_status="partial")
