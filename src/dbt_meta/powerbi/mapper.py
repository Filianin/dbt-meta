"""Classify a physical ``project.schema.table`` against the dbt manifest.

Reverse-lookup each BigQuery table referenced by Power BI against the manifest's
**models and sources**. The physical name of a model is
``database.schema.(alias or name)``; for a source it is
``database.schema.(identifier or name)``. Anything not found is ``external`` —
raw / staging / personal layers Power BI pulls from directly, i.e. logic living
outside the dbt project.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class TableMapping:
    """Result of classifying one physical table."""

    bq: str
    status: str  # model | source | external
    dbt_name: str | None = None
    unique_id: str | None = None


class DbtTableIndex:
    """Pre-built physical-name -> dbt-node index for O(1) classification."""

    def __init__(self, manifest: dict[str, Any]) -> None:
        self._index: dict[str, tuple[str, str, str]] = {}  # key -> (status, name, uid)
        # Secondary index on schema.table for native SQL that omits the
        # (default) project id. A schema.table seen under more than one project
        # is ambiguous and dropped so lookup falls back to ``external``.
        short_hits: dict[str, tuple[str, str, str]] = {}
        ambiguous: set[str] = set()

        def add(key: str, value: tuple[str, str, str]) -> None:
            self._index[key] = value
            short = self._short_key(key)
            if short in short_hits and short_hits[short] != value:
                ambiguous.add(short)
            else:
                short_hits[short] = value

        for uid, node in manifest.get("nodes", {}).items():
            if node.get("resource_type") != "model":
                continue
            key = self._model_key(node)
            if key:
                add(key, ("model", node.get("name", ""), uid))

        for uid, node in manifest.get("sources", {}).items():
            if node.get("resource_type") != "source":
                continue
            key = self._source_key(node)
            if key:
                add(key, ("source", node.get("name", ""), uid))

        self._short_index = {
            short: value
            for short, value in short_hits.items()
            if short not in ambiguous
        }

    @staticmethod
    def _short_key(key: str) -> str:
        """``db.schema.table`` -> ``schema.table`` (last two segments)."""
        return ".".join(key.split(".")[-2:])

    @staticmethod
    def _model_key(node: dict[str, Any]) -> str:
        config = node.get("config", {})
        database = node.get("database", "")
        schema = node.get("schema", "")
        table = config.get("alias") or node.get("name", "")
        if not (database and schema and table):
            return ""
        return f"{database}.{schema}.{table}".lower()

    @staticmethod
    def _source_key(node: dict[str, Any]) -> str:
        database = node.get("database", "")
        schema = node.get("schema", "")
        table = node.get("identifier") or node.get("name", "")
        if not (database and schema and table):
            return ""
        return f"{database}.{schema}.{table}".lower()

    def lookup(self, bq_table: str) -> TableMapping:
        """Classify a ``project.schema.table`` (project id optional)."""
        key = bq_table.lower()
        hit = self._index.get(key)
        if hit is None and key.count(".") == 1:
            # Native SQL dropped the project id — try the schema.table index.
            hit = self._short_index.get(key)
        if hit is None:
            return TableMapping(bq=bq_table, status="external")
        status, name, uid = hit
        return TableMapping(bq=bq_table, status=status, dbt_name=name, unique_id=uid)
