"""LineageBuilder — assemble a column-level dependency graph from manifest+catalog.

Workflow:
    1. Build table-name → dbt-unique_id index from manifest (models + sources).
    2. Build schema dict for SQLGlot from catalog.json (column types of upstreams).
    3. For each model:
       - get compiled_code (skip with warning if missing)
       - call sqlglot.lineage(None, sql, schema=...) → dict[output_col → Node]
       - walk each Node tree; for every leaf, resolve the leaf's source table
         to a dbt model name; emit edge upstream_model.col → current_model.col
    4. Aggregate into a LineageGraph.

The builder is intentionally tolerant: parse failures are recorded as warnings
and skipped rather than raising, so a single broken model does not fail
artifact generation for the whole project.
"""

from __future__ import annotations

import signal
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import sqlglot
from sqlglot import exp
from sqlglot.lineage import Node, lineage

from dbt_meta.lineage.graph import LineageGraph, make_node_id


class _ModelTimeout(Exception):
    """Raised when a single model exceeds the per-model parse budget."""


@contextmanager
def _per_model_timeout(seconds: int):
    """SIGALRM-based timeout for a single sqlglot.lineage call.

    Only used on POSIX main threads (signal.SIGALRM is unavailable on
    Windows and from non-main threads). Falls back to a no-op silently.
    """
    if seconds <= 0 or sys.platform.startswith("win"):
        yield
        return
    try:
        previous = signal.signal(signal.SIGALRM, _raise_timeout)
    except (ValueError, AttributeError):
        # Not main thread or platform without SIGALRM — just skip
        yield
        return
    try:
        signal.alarm(seconds)
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


def _raise_timeout(signum, frame):  # pragma: no cover — signal handler
    raise _ModelTimeout("sqlglot.lineage exceeded per-model budget")


@dataclass
class BuildStats:
    """Summary statistics for a single LineageBuilder run."""

    models_total: int = 0
    models_parsed: int = 0
    models_skipped_no_sql: int = 0
    models_skipped_parse_error: int = 0
    models_skipped_timeout: int = 0
    columns: int = 0
    edges: int = 0
    warnings: list[str] = field(default_factory=list)
    slow_models: list[tuple[str, float]] = field(default_factory=list)


class LineageBuilder:
    """Build a LineageGraph from a parsed dbt manifest and catalog.

    Args:
        manifest: Parsed manifest.json dict.
        catalog: Optional parsed catalog.json dict (for column types).
        dialect: SQL dialect for SQLGlot. Default 'bigquery'.

    Examples:
        >>> import orjson
        >>> from pathlib import Path
        >>> manifest = orjson.loads((Path.home() / 'dbt-state' / 'manifest.json').read_bytes())
        >>> catalog = orjson.loads((Path.home() / 'dbt-state' / 'catalog.json').read_bytes())
        >>> builder = LineageBuilder(manifest, catalog)
        >>> graph, stats = builder.build()
        >>> print(stats.models_parsed, stats.edges)
    """

    def __init__(
        self,
        manifest: dict[str, Any],
        catalog: Optional[dict[str, Any]] = None,
        dialect: str = "bigquery",
        per_model_timeout: int = 30,
        slow_threshold_seconds: float = 3.0,
        progress_callback: Optional[Callable[[int, int, str, float], None]] = None,
    ) -> None:
        """Initialize builder.

        Args:
            manifest: Parsed manifest.json dict.
            catalog: Optional parsed catalog.json dict.
            dialect: SQL dialect for SQLGlot. Default 'bigquery'.
            per_model_timeout: Seconds budget per model parse. 0 disables.
                Models exceeding the budget are skipped with a warning.
            slow_threshold_seconds: Models slower than this are reported in
                ``BuildStats.slow_models`` for diagnostic purposes.
            progress_callback: ``fn(idx, total, model_name, elapsed)`` invoked
                after each model. Useful for CLI progress reporting.
        """
        self.manifest = manifest
        self.catalog = catalog or {}
        self.dialect = dialect
        self.per_model_timeout = per_model_timeout
        self.slow_threshold_seconds = slow_threshold_seconds
        self.progress_callback = progress_callback

        # Pre-built indices populated lazily on first build()
        self._table_index: dict[str, str] = {}
        self._model_short_name: dict[str, str] = {}
        self._sqlglot_schema: dict[str, Any] = {}

    # ----- public -----

    def build(self) -> tuple[LineageGraph, BuildStats]:
        """Build the column-level lineage graph.

        Returns:
            Tuple of (LineageGraph, BuildStats).
        """
        self._build_indices()
        graph = LineageGraph()
        stats = BuildStats()

        nodes = self.manifest.get("nodes", {})
        models = {uid: n for uid, n in nodes.items() if uid.startswith("model.")}
        stats.models_total = len(models)

        for idx, (unique_id, model) in enumerate(models.items(), start=1):
            t0 = time.perf_counter()
            self._process_model(unique_id, model, graph, stats)
            elapsed = time.perf_counter() - t0

            short = self._model_short_name.get(unique_id) or model.get("name") or unique_id
            if elapsed >= self.slow_threshold_seconds:
                stats.slow_models.append((short, elapsed))
            if self.progress_callback is not None:
                self.progress_callback(idx, stats.models_total, short, elapsed)

        stats.columns = graph.node_count
        stats.edges = graph.edge_count
        return graph, stats

    # ----- per-model -----

    def _process_model(
        self,
        unique_id: str,
        model: dict[str, Any],
        graph: LineageGraph,
        stats: BuildStats,
    ) -> None:
        compiled_sql = model.get("compiled_code") or ""
        model_name = self._model_short_name.get(unique_id) or model.get("name") or ""

        if not compiled_sql.strip():
            stats.models_skipped_no_sql += 1
            stats.warnings.append(f"{model_name}: no compiled_code in manifest")
            return

        try:
            with _per_model_timeout(self.per_model_timeout):
                results = lineage(
                    column=None,
                    sql=compiled_sql,
                    schema=self._sqlglot_schema,
                    dialect=self.dialect,
                )
        except _ModelTimeout:
            stats.models_skipped_timeout += 1
            stats.warnings.append(
                f"{model_name}: timeout after {self.per_model_timeout}s "
                f"(SQL size {len(compiled_sql)} chars)"
            )
            return
        except (sqlglot.errors.SqlglotError, RecursionError, AttributeError) as exc:
            # SqlglotError covers ParseError, OptimizeError, SchemaError, etc.
            # AttributeError defends against malformed nodes returned for
            # edge-case SQL the parser swallows but the optimizer trips on.
            stats.models_skipped_parse_error += 1
            stats.warnings.append(f"{model_name}: parse error: {type(exc).__name__}: {exc!s}"[:200])
            return

        if not isinstance(results, dict):
            return

        col_type_lookup = self._column_types_for_model(unique_id)

        for output_col, node in results.items():
            dst_id = make_node_id(model_name, output_col)
            graph.add_node(
                dst_id,
                {
                    "model": model_name,
                    "column": output_col,
                    "data_type": col_type_lookup.get(output_col, ""),
                },
            )

            for leaf in self._iter_leaves(node):
                src = self._resolve_leaf_to_node_id(leaf)
                if src is None:
                    continue
                graph.add_edge(src, dst_id, {"transform": self._classify_transform(leaf)})

        stats.models_parsed += 1

    # ----- index construction -----

    def _build_indices(self) -> None:
        """Build table-name → unique_id and SQLGlot schema indices."""
        nodes = self.manifest.get("nodes", {})
        sources = self.manifest.get("sources", {})

        # Models: map full path / schema.alias / alias → unique_id
        for unique_id, n in nodes.items():
            if not unique_id.startswith("model."):
                continue
            short = unique_id.split(".")[-1]
            self._model_short_name[unique_id] = short
            db = (n.get("database") or "").lower()
            schema = (n.get("schema") or "").lower()
            alias = (n.get("alias") or n.get("name") or "").lower()
            name = (n.get("name") or "").lower()
            self._register_table(db, schema, alias, short)
            if name and name != alias:
                self._register_table(db, schema, name, short)

        # Sources: map full path / schema.identifier → source-unique_id (we use
        # a synthetic short name "source__schema__identifier" so they never
        # collide with model names).
        for unique_id, s in sources.items():
            db = (s.get("database") or "").lower()
            schema = (s.get("schema") or "").lower()
            ident = (s.get("identifier") or s.get("name") or "").lower()
            short = self._source_short_name(s)
            self._model_short_name[unique_id] = short
            self._register_table(db, schema, ident, short)

        # Build SQLGlot schema dict from catalog
        self._sqlglot_schema = self._build_sqlglot_schema()

    def _register_table(self, db: str, schema: str, table: str, target: str) -> None:
        """Register table identifiers under multiple keys for flexible matching."""
        if not table:
            return
        if db and schema:
            self._table_index[f"{db}.{schema}.{table}"] = target
        if schema:
            # schema.table fallback (without database)
            self._table_index.setdefault(f"{schema}.{table}", target)
        # bare table name (last-resort match — only if unique)
        if table not in self._table_index:
            self._table_index[table] = target
        else:
            # Already taken — bare-name match is ambiguous, drop it
            existing = self._table_index.get(table)
            if existing and existing != target:
                # Mark as ambiguous by removing — forces fully-qualified match
                self._table_index[table] = ""

    @staticmethod
    def _source_short_name(source: dict[str, Any]) -> str:
        schema = source.get("schema") or source.get("source_name") or ""
        ident = source.get("identifier") or source.get("name") or ""
        return f"source.{schema}.{ident}"

    DEFAULT_DB_PLACEHOLDER = "_default_"

    def _build_sqlglot_schema(self) -> dict[str, Any]:
        """Translate catalog.json into a 3-level SQLGlot schema dict.

        SQLGlot enforces a single nesting depth across the whole schema dict,
        so we emit ``database -> schema -> table -> {col: type}`` for every
        entry, using ``DEFAULT_DB_PLACEHOLDER`` for entries whose database
        is missing in the catalog metadata.
        """
        schema: dict[str, Any] = {}
        catalog_nodes = self.catalog.get("nodes", {}) if self.catalog else {}
        catalog_sources = self.catalog.get("sources", {}) if self.catalog else {}

        def _add(db: str, sch: str, table: str, columns: dict[str, Any]) -> None:
            cols = {c.get("name", ""): (c.get("type") or "STRING").upper() for c in columns.values()}
            cols = {k: v for k, v in cols.items() if k}
            if not cols:
                return
            db_key = db or self.DEFAULT_DB_PLACEHOLDER
            schema.setdefault(db_key, {}).setdefault(sch, {})[table] = cols

        for catalog_dict in (catalog_nodes, catalog_sources):
            for catalog_id, entry in catalog_dict.items():
                meta = entry.get("metadata", {})
                db = (meta.get("database") or "").lower()
                sch = (meta.get("schema") or "").lower()
                table = self._catalog_table_name(catalog_id, meta)
                if not (sch and table):
                    continue
                _add(db, sch, table, entry.get("columns", {}))

        return schema

    def _catalog_table_name(self, catalog_id: str, meta: dict[str, Any]) -> str:
        """Best-effort resolution of catalog entry → physical table name."""
        manifest_node = (
            self.manifest.get("nodes", {}).get(catalog_id)
            or self.manifest.get("sources", {}).get(catalog_id)
        )
        if manifest_node:
            return (
                manifest_node.get("alias")
                or manifest_node.get("identifier")
                or manifest_node.get("name")
                or ""
            ).lower()
        # Fallback: catalog metadata 'name' field
        return (meta.get("name") or "").lower()

    # ----- lineage walking -----

    @staticmethod
    def _iter_leaves(node: Node) -> list[Node]:
        """Return all leaf nodes (no further downstream) in the lineage tree."""
        leaves: list[Node] = []
        stack: list[Node] = [node]
        while stack:
            cur = stack.pop()
            if not cur.downstream:
                leaves.append(cur)
            else:
                stack.extend(cur.downstream)
        return leaves

    def _resolve_leaf_to_node_id(self, leaf: Node) -> Optional[str]:
        """Map a SQLGlot leaf node to the upstream dbt model's column node id.

        Returns None when the leaf cannot be resolved (e.g., references a
        CTE / unknown table that isn't in the manifest).
        """
        # Leaf name format from sqlglot is "table.column"
        leaf_name = leaf.name or ""
        if "." not in leaf_name:
            return None
        _, _, column = leaf_name.rpartition(".")
        if not column:
            return None

        # Recover the physical table from the leaf's expression AST.
        # sqlglot wraps the source in an Alias(this=Table(...)) node.
        source = getattr(leaf, "source", None)
        table_expr: Optional[exp.Table] = None
        if isinstance(source, exp.Table):
            table_expr = source
        elif isinstance(source, exp.Alias) and isinstance(source.this, exp.Table):
            table_expr = source.this
        elif leaf.expression is not None:
            for found in leaf.expression.find_all(exp.Table):
                table_expr = found
                break

        if table_expr is None:
            return None

        db = (table_expr.args.get("catalog").name if table_expr.args.get("catalog") else "").lower()
        schema = (table_expr.args.get("db").name if table_expr.args.get("db") else "").lower()
        table = (table_expr.name or "").lower()

        upstream_model = self._lookup_table(db, schema, table)
        if not upstream_model:
            return None
        return make_node_id(upstream_model, column)

    def _lookup_table(self, db: str, schema: str, table: str) -> Optional[str]:
        """Resolve (db, schema, table) → dbt short name via the table index."""
        if db and schema:
            hit = self._table_index.get(f"{db}.{schema}.{table}")
            if hit:
                return hit
        if schema:
            hit = self._table_index.get(f"{schema}.{table}")
            if hit:
                return hit
        hit = self._table_index.get(table)
        if hit:  # empty string means ambiguous
            return hit
        return None

    @staticmethod
    def _classify_transform(leaf: Node) -> str:
        """Coarse-grained transform tag for the edge.

        Heuristic: if the leaf expression is a bare Column, label it
        'passthrough'; otherwise 'derived'. Cheap and good enough for a
        first iteration; can be refined later (renamed/aggregated/cast).
        """
        expr = leaf.expression
        if expr is None:
            return "unknown"
        # The expression is typically Alias(this=...) — peel one level
        inner = expr.this if isinstance(expr, exp.Alias) else expr
        if isinstance(inner, exp.Column):
            return "passthrough"
        if isinstance(inner, exp.Table):
            return "passthrough"
        return "derived"

    def _column_types_for_model(self, unique_id: str) -> dict[str, str]:
        """Return {column_name: data_type} for a model from catalog (best-effort)."""
        if not self.catalog:
            return {}
        node = self.catalog.get("nodes", {}).get(unique_id)
        if not node:
            return {}
        return {
            (col.get("name") or "").lower(): (col.get("type") or "").upper()
            for col in node.get("columns", {}).values()
            if col.get("name")
        }
