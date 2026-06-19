"""JSON serialization for the lineage graph artifact.

The artifact format mirrors the dict shape from ``LineageGraph.to_dict``,
wrapped with metadata (schema version, manifest sha, generated_at, stats) so
readers can detect staleness and incompatible upgrades.

Storage layout::

    {
      "schema_version": "1.0",
      "manifest_sha": "abc123...",
      "generated_at": "2026-05-08T12:34:56Z",
      "stats": {"models": 865, "columns": 17234, "edges": 51002},
      "warnings": [...],
      "graph": {"nodes": [...], "edges": [...]}
    }
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import orjson

from dbt_meta.lineage.graph import LineageGraph

SCHEMA_VERSION = "1.0"


def save_artifact(
    graph: LineageGraph,
    path: str,
    *,
    manifest_sha: str | None = None,
    warnings: list[str] | None = None,
) -> str:
    """Write graph to disk as a JSON artifact.

    Args:
        graph: LineageGraph to serialize
        path: Output path (supports ~ expansion). Parent dir is created.
        manifest_sha: Optional checksum of the source manifest for invalidation.
        warnings: Optional list of build-time warnings (failed parses etc.).

    Returns:
        Absolute path of the written file.
    """
    out_path = Path(path).expanduser()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "manifest_sha": manifest_sha or "",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "stats": {
            "nodes": graph.node_count,
            "edges": graph.edge_count,
        },
        "warnings": warnings or [],
        "graph": graph.to_dict(),
    }

    out_path.write_bytes(orjson.dumps(payload, option=orjson.OPT_INDENT_2))
    return str(out_path.absolute())


def load_artifact(path: str) -> tuple[LineageGraph, dict[str, Any]]:
    """Read a graph artifact from disk.

    Args:
        path: Path to lineage.json (supports ~ expansion).

    Returns:
        Tuple of (LineageGraph, metadata-dict) where metadata contains
        ``schema_version``, ``manifest_sha``, ``generated_at``, ``stats``,
        and ``warnings``.

    Raises:
        FileNotFoundError: if path does not exist.
        ValueError: if schema_version is unsupported.
    """
    in_path = Path(path).expanduser()
    if not in_path.exists():
        raise FileNotFoundError(f"Lineage artifact not found: {in_path}")

    payload = orjson.loads(in_path.read_bytes())

    schema_version = payload.get("schema_version", "")
    if schema_version != SCHEMA_VERSION:
        raise ValueError(
            f"Unsupported lineage schema version: {schema_version!r} "
            f"(expected {SCHEMA_VERSION!r}). Rebuild with `meta lineage build`."
        )

    graph = LineageGraph.from_dict(payload.get("graph", {}))
    metadata = {k: v for k, v in payload.items() if k != "graph"}
    return graph, metadata


def get_artifact_age_hours(path: str) -> float | None:
    """Return file mtime age in hours, or None if missing."""
    p = Path(path).expanduser()
    if not p.exists():
        return None
    return (datetime.now().timestamp() - os.path.getmtime(p)) / 3600.0
