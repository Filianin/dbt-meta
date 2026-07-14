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

Parallelism: ``sqlglot.lineage`` is CPU-bound and almost entirely pure Python
(the GIL is held through ``qualify_columns``), so per-model work is dispatched
across processes when ``jobs > 1``. The pure per-model core lives in the
module-level ``_process_one`` function, which both the sequential loop and the
process-pool worker call — a single source of truth. Workers receive the
read-only indices once via ``_init_worker`` (spawn-safe) and return flat,
picklable ``ModelResult`` records; the main process aggregates them into the
(non-picklable, rustworkx-backed) ``LineageGraph``.
"""

from __future__ import annotations

import multiprocessing as mp
import signal
import sys
import time
from collections.abc import Iterator
from concurrent.futures import ProcessPoolExecutor, as_completed
from concurrent.futures.process import BrokenProcessPool
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable

import sqlglot
from sqlglot import exp
from sqlglot.lineage import Node, lineage

from dbt_meta.lineage.graph import LineageGraph, make_node_id

# Skip reasons carried on ModelResult. ``None`` = parsed OK; ``"silent"`` =
# parsed but sqlglot returned a non-dict (counted as neither parsed nor skipped,
# matching the historical single-threaded behaviour).
_SKIP_NO_SQL = "no_sql"
_SKIP_PARSE_ERROR = "parse_error"
_SKIP_TIMEOUT = "timeout"
_SKIP_SILENT = "silent"
_SKIP_WIDE = "wide"

# Skip models whose resolved column count exceeds this — all-columns SQLGlot
# lineage on a very wide table (e.g. a 1400+ column `SELECT *` staging
# passthrough) blows up to GBs of RAM and minutes of pure-Python work.
# Downstream models still resolve edges to the skipped model's columns by name;
# only the skipped model's own internal passthrough edges are absent. Override
# via ``DBT_LINEAGE_MAX_COLUMNS``; ``<=0`` disables the guard.
_MAX_MODEL_COLUMNS = 400


class _ModelTimeoutError(Exception):
    """Raised when a single model exceeds the per-model parse budget."""


@contextmanager
def _per_model_timeout(seconds: int) -> Iterator[None]:
    """SIGALRM-based timeout for a single sqlglot.lineage call.

    Only used on POSIX main threads (signal.SIGALRM is unavailable on
    Windows and from non-main threads). Falls back to a no-op silently.

    In the parallel build each pool worker runs its task on its own main
    thread, so this transfers unchanged into workers.
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


def _raise_timeout(signum: int, frame: Any) -> None:  # pragma: no cover — signal handler
    raise _ModelTimeoutError("sqlglot.lineage exceeded per-model budget")


@dataclass
class ModelResult:
    """Flat, picklable outcome of processing one model.

    Crosses the process boundary from workers back to the main process, which
    replays ``nodes``/``edges`` into the LineageGraph and folds the skip/warning
    into BuildStats. Never carries sqlglot ``Node``/AST objects.
    """

    model_name: str
    nodes: list[tuple[str, dict[str, Any]]] = field(default_factory=list)
    edges: list[tuple[str, str, dict[str, Any]]] = field(default_factory=list)
    warning: str | None = None
    skip: str | None = None
    elapsed: float = 0.0


@dataclass
class BuildStats:
    """Summary statistics for a single LineageBuilder run."""

    models_total: int = 0
    models_parsed: int = 0
    models_skipped_no_sql: int = 0
    models_skipped_parse_error: int = 0
    models_skipped_timeout: int = 0
    models_skipped_wide: int = 0
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
        jobs: Number of worker processes. ``1`` (default) runs the historical
            single-threaded path; ``>1`` dispatches per-model work across a
            ``spawn`` process pool.

    Examples:
        >>> import orjson
        >>> from pathlib import Path
        >>> manifest = orjson.loads((Path.home() / 'dbt-state' / 'manifest.json').read_bytes())
        >>> catalog = orjson.loads((Path.home() / 'dbt-state' / 'catalog.json').read_bytes())
        >>> builder = LineageBuilder(manifest, catalog, jobs=4)
        >>> graph, stats = builder.build()
        >>> print(stats.models_parsed, stats.edges)
    """

    def __init__(
        self,
        manifest: dict[str, Any],
        catalog: dict[str, Any] | None = None,
        dialect: str = "bigquery",
        per_model_timeout: int = 0,
        slow_threshold_seconds: float = 3.0,
        progress_callback: Callable[[int, int, str, float], None] | None = None,
        jobs: int = 1,
        max_columns: int = _MAX_MODEL_COLUMNS,
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
                after each model completes. In the parallel path ``idx`` is a
                monotonic completed-counter (completion order, not manifest
                order) and ``elapsed`` is the worker-measured per-model time.
            jobs: Worker process count. ``1`` = sequential; ``>1`` = parallel.
            max_columns: Skip models resolving to more than this many columns
                (all-columns lineage on very wide tables is O(cols) in RAM and
                time). ``<=0`` disables the guard.
        """
        self.manifest = manifest
        self.catalog = catalog or {}
        self.dialect = dialect
        self.per_model_timeout = per_model_timeout
        self.slow_threshold_seconds = slow_threshold_seconds
        self.progress_callback = progress_callback
        self.jobs = max(1, jobs)
        self.max_columns = max_columns

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
        col_types_map = self._build_col_types_map()
        graph = LineageGraph()
        stats = BuildStats()

        nodes = self.manifest.get("nodes", {})
        models = {uid: n for uid, n in nodes.items() if uid.startswith("model.")}
        stats.models_total = len(models)

        tasks: list[tuple[str, str, str]] = [
            (
                unique_id,
                self._model_short_name.get(unique_id) or model.get("name") or unique_id,
                model.get("compiled_code") or "",
            )
            for unique_id, model in models.items()
        ]

        if self.jobs > 1 and len(tasks) > 1:
            results = self._run_parallel(tasks, col_types_map, stats)
        else:
            results = self._run_sequential(tasks, col_types_map)

        for completed, result in enumerate(results, start=1):
            self._apply_result(result, graph, stats)
            if result.elapsed >= self.slow_threshold_seconds:
                stats.slow_models.append((result.model_name, result.elapsed))
            if self.progress_callback is not None:
                self.progress_callback(
                    completed, stats.models_total, result.model_name, result.elapsed
                )

        stats.columns = graph.node_count
        stats.edges = graph.edge_count
        return graph, stats

    # ----- dispatch -----

    def _run_sequential(
        self,
        tasks: list[tuple[str, str, str]],
        col_types_map: dict[str, dict[str, str]],
    ) -> Iterator[ModelResult]:
        """Process every model in the current process (jobs == 1)."""
        for unique_id, model_name, compiled_sql in tasks:
            yield _process_one(
                unique_id,
                model_name,
                compiled_sql,
                table_index=self._table_index,
                sqlglot_schema=self._sqlglot_schema,
                col_types=col_types_map.get(unique_id, {}),
                dialect=self.dialect,
                per_model_timeout=self.per_model_timeout,
                max_columns=self.max_columns,
            )

    def _run_parallel(
        self,
        tasks: list[tuple[str, str, str]],
        col_types_map: dict[str, dict[str, str]],
        stats: BuildStats,
    ) -> Iterator[ModelResult]:
        """Process models across a ``spawn`` process pool.

        On ``BrokenProcessPool`` (a worker segfault / OOM-kill), the remaining
        unfinished models are finished sequentially in the main process so the
        artifact is always completed — worst case is a slower tail, never a
        total loss.
        """
        ctx = mp.get_context("spawn")
        initargs = (
            self._table_index,
            self._sqlglot_schema,
            col_types_map,
            self.dialect,
            self.per_model_timeout,
            self.max_columns,
        )
        pool_kwargs: dict[str, Any] = {
            "max_workers": self.jobs,
            "mp_context": ctx,
            "initializer": _init_worker,
            "initargs": initargs,
        }

        # Track unfinished models so a broken pool can be recovered sequentially.
        remaining: dict[str, tuple[str, str, str]] = {t[0]: t for t in tasks}

        try:
            with ProcessPoolExecutor(**pool_kwargs) as executor:
                futures = {
                    executor.submit(_process_one_task, uid, name, sql): uid
                    for uid, name, sql in tasks
                }
                for future in as_completed(futures):
                    uid = futures[future]
                    result = future.result()
                    remaining.pop(uid, None)
                    yield result
        except BrokenProcessPool:
            stats.warnings.append(
                f"process pool broke; finishing {len(remaining)} remaining models sequentially"
            )
            for uid, name, sql in remaining.values():
                yield _process_one(
                    uid,
                    name,
                    sql,
                    table_index=self._table_index,
                    sqlglot_schema=self._sqlglot_schema,
                    col_types=col_types_map.get(uid, {}),
                    dialect=self.dialect,
                    per_model_timeout=self.per_model_timeout,
                    max_columns=self.max_columns,
                )

    def _apply_result(self, result: ModelResult, graph: LineageGraph, stats: BuildStats) -> None:
        """Fold one ModelResult into the graph and stats (main process only)."""
        if result.skip == _SKIP_NO_SQL:
            stats.models_skipped_no_sql += 1
        elif result.skip == _SKIP_PARSE_ERROR:
            stats.models_skipped_parse_error += 1
        elif result.skip == _SKIP_TIMEOUT:
            stats.models_skipped_timeout += 1
        elif result.skip == _SKIP_WIDE:
            stats.models_skipped_wide += 1
        elif result.skip is None:
            stats.models_parsed += 1
        # _SKIP_SILENT: counted as neither parsed nor skipped

        if result.warning:
            stats.warnings.append(result.warning)

        for node_id, attrs in result.nodes:
            graph.add_node(node_id, attrs)
        for src, dst, attrs in result.edges:
            graph.add_edge(src, dst, attrs)

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
            cols = {
                c.get("name", ""): (c.get("type") or "STRING").upper() for c in columns.values()
            }
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
        manifest_node = self.manifest.get("nodes", {}).get(catalog_id) or self.manifest.get(
            "sources", {}
        ).get(catalog_id)
        if manifest_node:
            return (
                manifest_node.get("alias")
                or manifest_node.get("identifier")
                or manifest_node.get("name")
                or ""
            ).lower()
        # Fallback: catalog metadata 'name' field
        return (meta.get("name") or "").lower()

    def _build_col_types_map(self) -> dict[str, dict[str, str]]:
        """Return ``{unique_id: {column_name: data_type}}`` from catalog.

        Precomputed once in the main process and shipped to workers so the
        per-model worker can set ``data_type`` attrs without touching catalog.
        """
        if not self.catalog:
            return {}
        out: dict[str, dict[str, str]] = {}
        for unique_id, node in self.catalog.get("nodes", {}).items():
            out[unique_id] = {
                (col.get("name") or "").lower(): (col.get("type") or "").upper()
                for col in node.get("columns", {}).values()
                if col.get("name")
            }
        return out

    def _lookup_table(self, db: str, schema: str, table: str) -> str | None:
        """Resolve (db, schema, table) → dbt short name via the table index."""
        return _lookup_table(self._table_index, db, schema, table)

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


# -----------------------------------------------------------------------------
# Module-level per-model core — shared by the sequential loop and pool workers.
# Free functions (not methods) so they are picklable under the ``spawn`` start
# method and cannot drift between the two execution paths.
# -----------------------------------------------------------------------------

# Populated by _init_worker in each pool worker (read-only for the run).
_WORKER_STATE: dict[str, Any] = {}


def _init_worker(
    table_index: dict[str, str],
    sqlglot_schema: dict[str, Any],
    col_types_map: dict[str, dict[str, str]],
    dialect: str,
    per_model_timeout: int,
    max_columns: int,
) -> None:
    """Pool initializer — stash the read-only indices in a worker global once."""
    _WORKER_STATE["table_index"] = table_index
    _WORKER_STATE["sqlglot_schema"] = sqlglot_schema
    _WORKER_STATE["col_types_map"] = col_types_map
    _WORKER_STATE["dialect"] = dialect
    _WORKER_STATE["per_model_timeout"] = per_model_timeout
    _WORKER_STATE["max_columns"] = max_columns


def _process_one_task(unique_id: str, model_name: str, compiled_sql: str) -> ModelResult:
    """Pool worker entrypoint — reads indices from the worker global."""
    return _process_one(
        unique_id,
        model_name,
        compiled_sql,
        table_index=_WORKER_STATE["table_index"],
        sqlglot_schema=_WORKER_STATE["sqlglot_schema"],
        col_types=_WORKER_STATE["col_types_map"].get(unique_id, {}),
        dialect=_WORKER_STATE["dialect"],
        per_model_timeout=_WORKER_STATE["per_model_timeout"],
        max_columns=_WORKER_STATE["max_columns"],
    )


def _process_one(
    unique_id: str,
    model_name: str,
    compiled_sql: str,
    *,
    table_index: dict[str, str],
    sqlglot_schema: dict[str, Any],
    col_types: dict[str, str],
    dialect: str,
    per_model_timeout: int,
    max_columns: int = 0,
) -> ModelResult:
    """Compute column lineage for one model. Pure given its keyword indices.

    Never raises: parse/timeout failures are captured as a skip reason + warning
    on the returned ModelResult so a single broken model never aborts the build.
    """
    t0 = time.perf_counter()

    if not compiled_sql.strip():
        return ModelResult(
            model_name,
            warning=f"{model_name}: no compiled_code in manifest",
            skip=_SKIP_NO_SQL,
            elapsed=time.perf_counter() - t0,
        )

    if max_columns > 0 and len(col_types) > max_columns:
        return ModelResult(
            model_name,
            warning=(
                f"{model_name}: skipped — {len(col_types)} columns exceed "
                f"max_columns={max_columns} (all-columns lineage would blow up "
                f"RAM/time); downstream still resolves edges to its columns"
            ),
            skip=_SKIP_WIDE,
            elapsed=time.perf_counter() - t0,
        )

    try:
        with _per_model_timeout(per_model_timeout):
            results = lineage(
                column=None,
                sql=compiled_sql,
                schema=sqlglot_schema,
                dialect=dialect,
            )
    except _ModelTimeoutError:
        return ModelResult(
            model_name,
            warning=(
                f"{model_name}: timeout after {per_model_timeout}s "
                f"(SQL size {len(compiled_sql)} chars)"
            ),
            skip=_SKIP_TIMEOUT,
            elapsed=time.perf_counter() - t0,
        )
    except (sqlglot.errors.SqlglotError, RecursionError, AttributeError) as exc:
        # SqlglotError covers ParseError, OptimizeError, SchemaError, etc.
        # AttributeError defends against malformed nodes returned for
        # edge-case SQL the parser swallows but the optimizer trips on.
        return ModelResult(
            model_name,
            warning=f"{model_name}: parse error: {type(exc).__name__}: {exc!s}"[:200],
            skip=_SKIP_PARSE_ERROR,
            elapsed=time.perf_counter() - t0,
        )

    if not isinstance(results, dict):
        return ModelResult(model_name, skip=_SKIP_SILENT, elapsed=time.perf_counter() - t0)

    nodes: list[tuple[str, dict[str, Any]]] = []
    edges: list[tuple[str, str, dict[str, Any]]] = []

    for output_col, node in results.items():
        dst_id = make_node_id(model_name, output_col)
        nodes.append(
            (
                dst_id,
                {
                    "model": model_name,
                    "column": output_col,
                    "data_type": col_types.get(output_col, ""),
                },
            )
        )
        for leaf in _iter_leaves(node):
            src = _resolve_leaf_to_node_id(leaf, table_index)
            if src is None:
                continue
            edges.append((src, dst_id, {"transform": _classify_transform(leaf)}))

    return ModelResult(model_name, nodes=nodes, edges=edges, elapsed=time.perf_counter() - t0)


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


def _resolve_leaf_to_node_id(leaf: Node, table_index: dict[str, str]) -> str | None:
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
    table_expr: exp.Table | None = None
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

    catalog_arg = table_expr.args.get("catalog")
    db = (catalog_arg.name if catalog_arg else "").lower()
    schema_arg = table_expr.args.get("db")
    schema = (schema_arg.name if schema_arg else "").lower()
    table = (table_expr.name or "").lower()

    upstream_model = _lookup_table(table_index, db, schema, table)
    if not upstream_model:
        return None
    return make_node_id(upstream_model, column)


def _lookup_table(table_index: dict[str, str], db: str, schema: str, table: str) -> str | None:
    """Resolve (db, schema, table) → dbt short name via the table index."""
    if db and schema:
        hit = table_index.get(f"{db}.{schema}.{table}")
        if hit:
            return hit
    if schema:
        hit = table_index.get(f"{schema}.{table}")
        if hit:
            return hit
    hit = table_index.get(table)
    if hit:  # empty string means ambiguous
        return hit
    return None


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
