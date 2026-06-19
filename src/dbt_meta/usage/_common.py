"""Shared helpers for advisor modules.

Centralises the boilerplate of resolving a target dbt model to:
  - its physical table aliases (for ``ColumnUsageExtractor``)
  - its column types (from catalog.json)
  - its dbt config (partition_by / cluster_by / unique_key)
  - its transitive downstream model unique_ids (from manifest's child_map)
  - compiled SQL of each downstream (manifest → disk fallback)
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable
from typing import Any

from dbt_meta.usage.extractor import ColumnUsageExtractor, UsageEvent


def find_target_node(manifest: dict[str, Any], short_name: str) -> tuple[str, dict[str, Any]] | None:
    """Find ``(unique_id, model_dict)`` for a model by short name."""
    for unique_id, node in manifest.get("nodes", {}).items():
        if not unique_id.startswith("model."):
            continue
        if unique_id.split(".")[-1] == short_name:
            return unique_id, node
    return None


def transitive_downstream(manifest: dict[str, Any], unique_id: str) -> list[str]:
    """BFS over manifest['child_map'] to collect all downstream model ids."""
    child_map = manifest.get("child_map", {})
    nodes = manifest.get("nodes", {})
    visited: set[str] = set()
    out: list[str] = []
    q: deque[str] = deque(child_map.get(unique_id, []))
    while q:
        cur = q.popleft()
        if cur in visited:
            continue
        visited.add(cur)
        # We only care about models — not tests, snapshots, seeds, exposures
        if cur.startswith("model.") and cur in nodes:
            out.append(cur)
        for child in child_map.get(cur, []):
            if child not in visited:
                q.append(child)
    return out


def direct_downstream(manifest: dict[str, Any], unique_id: str) -> list[str]:
    """Immediate model children — those that ``ref()`` the target directly.

    Cluster/partition advisors must use direct children, not transitive
    descendants: grandchildren read the intermediate model's storage, not
    the original table, so their WHERE clauses don't affect the target's
    partition/cluster pruning. Including them would inflate the "X/N
    models reference this table" line with descendants that physically
    never query the target.
    """
    child_map = manifest.get("child_map", {})
    nodes = manifest.get("nodes", {})
    out: list[str] = []
    for child in child_map.get(unique_id, []) or []:
        if child.startswith("model.") and child in nodes:
            out.append(child)
    return out


def upstream_table_aliases(model: dict[str, Any]) -> set[str]:
    """All identifiers that resolve to this model in another SQL.

    Includes bare alias/name, ``schema.alias``, and ``database.schema.alias``.
    """
    db = (model.get("database") or "").lower()
    schema = (model.get("schema") or "").lower()
    alias = (model.get("alias") or model.get("name") or "").lower()
    name = (model.get("name") or "").lower()
    out: set[str] = set()
    for tbl in {alias, name}:
        if not tbl:
            continue
        out.add(tbl)
        if schema:
            out.add(f"{schema}.{tbl}")
        if db and schema:
            out.add(f"{db}.{schema}.{tbl}")
    return out


def select_star_from(sql: str, aliases: set[str]) -> bool:
    """Detect ``SELECT * FROM <upstream>`` (or via subquery / CTE) in ``sql``.

    Walks Select nodes; if any has ``exp.Star`` in its projections AND its
    FROM clause references one of ``aliases``, returns True. Falls back to a
    tolerant substring check if SQLGlot can't parse.
    """
    if not sql.strip() or not aliases:
        return False
    aliases_lower = {a.lower() for a in aliases}
    try:
        from sqlglot import exp, parse_one
        from sqlglot.errors import SqlglotError

        parsed = parse_one(sql, dialect="bigquery")
    except (SqlglotError, RecursionError, AttributeError):
        # Cheap fallback: regex-style check on raw text
        s = sql.lower()
        return "select *" in s and any(a in s for a in aliases_lower)

    for select in parsed.find_all(exp.Select):
        # Does this SELECT have a star projection?
        has_star = any(isinstance(p, exp.Star) for p in select.expressions)
        if not has_star:
            continue
        # Does its FROM clause reference one of our target aliases?
        # SQLGlot stores it as 'from_' (with trailing underscore) on the
        # Select node; some older builds use 'from' — accept either.
        from_clause = select.args.get("from_") or select.args.get("from")
        if from_clause is None:
            continue
        for tbl in from_clause.find_all(exp.Table):
            db_arg = tbl.args.get("catalog")
            sch_arg = tbl.args.get("db")
            db = (db_arg.name if db_arg else "").lower()
            sch = (sch_arg.name if sch_arg else "").lower()
            name = (tbl.name or "").lower()
            cands = {name}
            if sch:
                cands.add(f"{sch}.{name}")
            if db and sch:
                cands.add(f"{db}.{sch}.{name}")
            if cands & aliases_lower:
                return True
    return False


def references_target(sql: str, aliases: set[str]) -> bool:
    """Cheap check: does ``sql`` reference any of ``aliases`` as a table?

    Uses SQLGlot to find ``exp.Table`` nodes, falling back to a tolerant
    substring check on parse failure. Used by RefreshAdvisor to detect
    ``SELECT *`` references (which produce no Column events).
    """
    if not sql.strip() or not aliases:
        return False
    try:
        from sqlglot import exp, parse_one
        from sqlglot.errors import SqlglotError

        parsed = parse_one(sql, dialect="bigquery")
        for tbl in parsed.find_all(exp.Table):
            db_arg = tbl.args.get("catalog")
            sch_arg = tbl.args.get("db")
            db = (db_arg.name if db_arg else "").lower()
            sch = (sch_arg.name if sch_arg else "").lower()
            name = (tbl.name or "").lower()
            candidates = {name}
            if sch:
                candidates.add(f"{sch}.{name}")
            if db and sch:
                candidates.add(f"{db}.{sch}.{name}")
            if candidates & {a.lower() for a in aliases}:
                return True
        return False
    except (SqlglotError, RecursionError, AttributeError):
        # Tolerant fallback — substring on bare names only. AttributeError
        # covers cases where sqlglot returns a None expression for malformed SQL.
        sql_lower = sql.lower()
        return any(a.lower() in sql_lower for a in aliases)


def collect_events(
    manifest: dict[str, Any],
    target_model: dict[str, Any],
    extractor: ColumnUsageExtractor,
    downstream_ids: Iterable[str],
) -> list[UsageEvent]:
    """Run ColumnUsageExtractor across every downstream and concat events."""
    aliases = upstream_table_aliases(target_model)
    nodes = manifest.get("nodes", {})
    events: list[UsageEvent] = []
    for uid in downstream_ids:
        node = nodes.get(uid)
        if node is None:
            continue
        sql = node.get("compiled_code") or ""
        if not sql.strip():
            continue
        downstream_short = uid.split(".")[-1]
        events.extend(extractor.extract(sql, downstream_short, aliases))
    return events


def model_materialization(model: dict[str, Any]) -> str:
    """Return the dbt materialization string (``"incremental"`` / ``"table"`` …).

    Reads ``config.materialized`` with an empty-string default so callers
    can do bare string comparisons without ``None`` guards.
    """
    cfg = model.get("config") or {}
    return str(cfg.get("materialized") or "")


def downstream_short_to_materialized(
    manifest: dict[str, Any],
    downstream_ids: Iterable[str],
) -> dict[str, str]:
    """Map ``downstream_short_name → materialization`` for direct children.

    The partition advisor needs this to flag *incremental* downstream
    that don't actually filter on the partition column (the broken
    case). Non-incremental downstream that do a full scan are normal
    and shouldn't be reported as problems.
    """
    nodes = manifest.get("nodes", {})
    out: dict[str, str] = {}
    for uid in downstream_ids:
        node = nodes.get(uid)
        if node is None:
            continue
        short = uid.split(".")[-1]
        out[short] = model_materialization(node)
    return out


def diagnose_no_extraction(
    manifest: dict[str, Any],
    downstream_ids: Iterable[str],
) -> str | None:
    """Explain why an advisor saw ``downstream_count > 0`` but ``parsed = 0``.

    Both `cluster` and `partition` advisors derive their summary from two
    different sources: ``downstream_count`` from ``manifest.child_map``
    (works on any manifest) and ``parsed_downstream_count`` from
    ``ColumnUsageExtractor`` reading ``compiled_code`` (works only when SQL
    is compiled). When the manifest comes from ``dbt parse`` rather than
    ``dbt compile``, ``compiled_code`` is empty everywhere — and the
    advisor truthfully reports ``0/N`` references, which reads as
    "nothing depends on this model" to a user who doesn't know that
    distinction. Surface the real cause instead.
    """
    nodes = manifest.get("nodes", {})
    empty_count = 0
    total = 0
    for uid in downstream_ids:
        node = nodes.get(uid)
        if node is None:
            continue
        total += 1
        if not (node.get("compiled_code") or "").strip():
            empty_count += 1
    if total == 0:
        return None
    if empty_count == total:
        return (
            f"all {total} downstream models have empty compiled_code — "
            "the active manifest was probably produced by `dbt parse` "
            "(no Jinja rendering). Run `dbt compile` in the project, or "
            "point at the prod manifest via `--manifest "
            "~/dbt-state/manifest.json`."
        )
    if empty_count * 2 > total:
        return (
            f"{empty_count}/{total} downstream models have empty "
            "compiled_code; recommendations are conservative. Run "
            "`dbt compile` in the project to populate them."
        )
    return None


def column_types(catalog: dict[str, Any], unique_id: str) -> dict[str, str]:
    """Return {column_name_lower: data_type_upper} for a model from catalog."""
    if not catalog:
        return {}
    node = catalog.get("nodes", {}).get(unique_id)
    if not node:
        return {}
    return {
        (col.get("name") or "").lower(): (col.get("type") or "").upper()
        for col in node.get("columns", {}).values()
        if col.get("name")
    }


def model_partition_columns(model: dict[str, Any]) -> set[str]:
    """Extract partition column names from ``config.partition_by``."""
    cfg = model.get("config") or {}
    pb = cfg.get("partition_by")
    if not pb:
        return set()
    if isinstance(pb, str):
        return {pb.lower()}
    if isinstance(pb, dict):
        f = pb.get("field") or pb.get("column") or ""
        return {f.lower()} if f else set()
    if isinstance(pb, list):
        return {str(x).lower() for x in pb if x}
    return set()


def model_cluster_columns(model: dict[str, Any]) -> set[str]:
    """Extract clustering column names from ``config.cluster_by``."""
    cfg = model.get("config") or {}
    cb = cfg.get("cluster_by")
    if not cb:
        return set()
    if isinstance(cb, str):
        return {cb.lower()}
    if isinstance(cb, list):
        return {str(x).lower() for x in cb if x}
    return set()
