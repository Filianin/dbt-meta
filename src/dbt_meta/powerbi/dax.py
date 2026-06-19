"""Extract table/column references from DAX expressions.

DAX references follow two patterns:
- Quoted table: ``'Table Name'[ColumnName]``
- Bare table:   ``TableName[ColumnName]``

Returns a deduplicated list of ``{table, column}`` dicts.
"""

from __future__ import annotations

import re

_QUOTED = re.compile(r"'([^']+)'\[([^\]]+)\]")
_BARE = re.compile(r"([A-Za-z_]\w*)\[([^\]]+)\]")


def parse_dax_refs(expression: str) -> list[dict[str, str]]:
    """Return deduplicated table/column refs extracted from a DAX expression."""
    seen: dict[tuple[str, str], None] = {}
    for table, column in _QUOTED.findall(expression):
        seen[(table, column)] = None
    remaining = _QUOTED.sub("", expression)
    for table, column in _BARE.findall(remaining):
        seen[(table, column)] = None
    return [{"table": t, "column": c} for t, c in seen]
