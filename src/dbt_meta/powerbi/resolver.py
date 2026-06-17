"""Resolve intra-dataset cross-query references down to BigQuery leaves.

A Power BI M-query can reference another query in the same dataset by name
(``Table.NestedJoin(Source, ..., other_query, ...)``). Those references are not
BigQuery tables — they must be followed recursively until only physical
``project.schema.table`` leaves remain, so the table set reported per query is
complete. Cycles and dangling references are tolerated.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class QueryNode:
    """One M-query's direct tables and its references to sibling queries."""

    tables: tuple[str, ...] = ()
    cross_query_refs: tuple[str, ...] = ()


def resolve_query_tables(name: str, nodes: dict[str, QueryNode]) -> tuple[str, ...]:
    """Return the full transitive table set for query ``name``."""
    seen_queries: set[str] = set()
    tables: dict[str, None] = {}

    def visit(query: str) -> None:
        if query in seen_queries:
            return
        seen_queries.add(query)
        node = nodes.get(query)
        if node is None:
            return
        for t in node.tables:
            tables[t] = None
        for ref in node.cross_query_refs:
            visit(ref)

    visit(name)
    return tuple(tables)
