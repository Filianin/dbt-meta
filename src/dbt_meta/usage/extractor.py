"""ColumnUsageExtractor — extract column-usage events from a downstream SQL.

Given the compiled SQL of a downstream dbt model and the table-name(s) of the
upstream model whose columns we want to track, returns a list of UsageEvents
describing every WHERE / JOIN-ON / GROUP BY / ORDER BY / QUALIFY / window
PARTITION BY reference to those columns.

Approach: walk every Scope produced by ``sqlglot.optimizer.scope.traverse_scope``,
resolve which scope-local alias corresponds to the upstream model (via
``selected_sources``), then for each clause node extract every Column whose
``.table`` matches one of the resolved aliases. The nearest enclosing operator
(``EQ`` / ``In`` / ``Between`` / ``GT`` / …) is mapped to a coarse
``operator`` tag, and the right-hand operand is inspected to classify
``selectivity`` (``literal`` vs ``subquery`` vs ``range`` vs ``none``).
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from sqlglot import exp, parse_one
from sqlglot.errors import SqlglotError
from sqlglot.optimizer.qualify_columns import qualify_columns
from sqlglot.optimizer.qualify_tables import qualify_tables
from sqlglot.optimizer.scope import Scope, build_scope, traverse_scope

# Coarse operator buckets — keep small, downstream advisors weight by these.
_OP_BY_CLASS = {
    exp.EQ: "eq",
    exp.NEQ: "neq",
    exp.In: "in",
    exp.Between: "between",
    exp.GT: "gt",
    exp.GTE: "ge",
    exp.LT: "lt",
    exp.LTE: "le",
    exp.Is: "is_null",
    exp.Like: "like",
    exp.ILike: "like",
}

_RANGE_OPS = {"between", "gt", "ge", "lt", "le"}


@dataclass(frozen=True)
class UsageEvent:
    """A single column reference in a downstream model's SQL.

    Attributes:
        column: Bare column name (lower-cased) of the upstream model.
        clause: One of 'where' / 'join' / 'group_by' / 'order_by' /
            'partition_by' / 'qualify' / 'select'. PARTITION BY here means
            the SQL clause inside an OVER() window — *not* a table-level
            partition.
        operator: Coarse operator tag — see ``_OP_BY_CLASS``. ``'fn'`` is
            kept as a back-compat sentinel for events whose column is
            wrapped in a function AND the enclosing comparison was
            unknown; modern consumers should check ``wrapping_fn``
            instead. ``'none'`` means no operator applied (e.g. plain
            ``GROUP BY t.x``).
        wrapping_fn: Lowercased name of the function wrapping the
            column (``"date"``, ``"upper"`` …) or ``""`` when the
            column is referenced bare. Surfaces *separately* from
            ``operator`` because BigQuery prunes through some monotonic
            wrappers (DATE, TIMESTAMP_TRUNC on a TIMESTAMP partition,
            etc.) but not through arbitrary ones — only the partition
            advisor knows which is which.
        selectivity: ``'literal'`` (compared against literal/literal-list),
            ``'subquery'``, ``'range'`` (between or two-sided GT/LT pair),
            or ``'none'``.
        downstream_model: Name of the model whose SQL we parsed.
    """

    column: str
    clause: str
    operator: str
    selectivity: str
    downstream_model: str
    wrapping_fn: str = ""


# Clause-finder strategy:
# We resolve clauses *per scope* rather than via parsed.find_all so that
# subqueries don't leak their columns into the parent scope's WHERE etc.
def _scope_clause_iter(scope: Scope) -> Iterable[tuple[str, exp.Expression]]:
    """Yield (clause_kind, clause_node) pairs for the given scope's SELECT."""
    expression = scope.expression
    if not isinstance(expression, exp.Select):
        return

    if (where := expression.args.get("where")) is not None:
        yield ("where", where)

    if (group := expression.args.get("group")) is not None:
        yield ("group_by", group)

    if (order := expression.args.get("order")) is not None:
        yield ("order_by", order)

    if (qualify := expression.args.get("qualify")) is not None:
        yield ("qualify", qualify)

    for j in expression.args.get("joins") or []:
        on_node = j.args.get("on")
        if on_node is not None:
            yield ("join", on_node)


def _walk_window_clauses(scope: Scope) -> Iterable[tuple[str, exp.Expression]]:
    """Yield window-internal PARTITION BY / ORDER BY clauses from a scope."""
    if not isinstance(scope.expression, exp.Select):
        return
    for window in scope.expression.find_all(exp.Window):
        for sub in window.args.get("partition_by") or []:
            yield ("partition_by", sub)
        order = window.args.get("order")
        if order is not None:
            yield ("order_by", order)


class ColumnUsageExtractor:
    """Extract column-usage events for a target upstream model from a SQL.

    The same instance is safe to reuse across many SQL strings (it has no
    mutable state).
    """

    def __init__(self, dialect: str = "bigquery") -> None:
        self.dialect = dialect

    # ----- public API -----

    def extract(
        self,
        downstream_sql: str,
        downstream_model: str,
        upstream_table_names: Iterable[str],
    ) -> list[UsageEvent]:
        """Walk SQL and return UsageEvents for the upstream model.

        Args:
            downstream_sql: Compiled SQL of the downstream model.
            downstream_model: Short name of the downstream model
                (e.g. ``core_clients__rfm_score``); echoed into every event.
            upstream_table_names: Iterable of identifiers that resolve to the
                upstream model in the SQL — typically a set containing the
                model's bare name plus its ``schema.table`` and
                ``database.schema.table``. Case-insensitive matching.

        Returns:
            List of ``UsageEvent``. Empty when SQL is empty or no references
            to the upstream model are found.
        """
        if not downstream_sql.strip():
            return []

        try:
            parsed = parse_one(downstream_sql, dialect=self.dialect)
            # Run sqlglot's optimizer pass to qualify table aliases AND
            # back-propagate them to bare column refs. Without this,
            # ``WHERE event_type = …`` (no alias prefix) leaves
            # ``col.table = ''`` and our scope-based alias filter never
            # matches. This is the single most common reason real-world
            # dbt SQL produces zero events.
            try:
                parsed = qualify_tables(parsed)
                root_scope = build_scope(parsed)
                if root_scope is not None:
                    parsed = qualify_columns(parsed, schema={}, infer_schema=True)
            except (SqlglotError, AttributeError, KeyError):
                # Optimizer can fail on weird schemas; bare-name fallback
                # below still works for fully qualified refs.
                pass
        except (SqlglotError, RecursionError):
            return []

        target_keys = {t.lower() for t in upstream_table_names if t}
        if not target_keys:
            return []

        events: list[UsageEvent] = []
        try:
            scopes = list(traverse_scope(parsed))
        except (SqlglotError, RecursionError, AttributeError):
            return []
        for scope in scopes:
            try:
                aliases = self._aliases_for_target(scope, target_keys)
            except (SqlglotError, AttributeError):
                continue
            if not aliases:
                continue
            for clause_kind, clause_node in _scope_clause_iter(scope):
                events.extend(
                    self._events_from_clause(
                        clause_kind, clause_node, aliases, downstream_model
                    )
                )
            for clause_kind, clause_node in _walk_window_clauses(scope):
                events.extend(
                    self._events_from_clause(
                        clause_kind, clause_node, aliases, downstream_model
                    )
                )
            # SELECT projection: track if upstream columns appear in output
            select_node = scope.expression
            if isinstance(select_node, exp.Select):
                for proj in select_node.expressions:
                    events.extend(
                        self._events_from_clause(
                            "select", proj, aliases, downstream_model
                        )
                    )
        return events

    # ----- alias resolution -----

    @staticmethod
    def _aliases_for_target(scope: Scope, target_keys: set[str]) -> set[str]:
        """Return scope-local aliases that point at the upstream model.

        ``target_keys`` is matched against the underlying source's bare name
        and any qualified ``[db.]schema.name`` form.

        ``scope.selected_sources`` returns ``alias -> (node, source)`` tuples
        in modern SQLGlot; ``source`` is either an ``exp.Table`` (physical
        table reference) or a ``Scope`` (CTE / subquery). We only resolve
        physical-table aliases here.
        """
        hits: set[str] = set()
        for alias, value in scope.selected_sources.items():
            table_expr: exp.Table | None = None
            # Modern sqlglot: tuple (node, source); older: bare value
            if isinstance(value, tuple):
                # Find the Table in the tuple
                for item in value:
                    if isinstance(item, exp.Table):
                        table_expr = item
                        break
            elif isinstance(value, exp.Table):
                table_expr = value

            if table_expr is None:
                # CTE / subquery — skip
                continue

            catalog_arg = table_expr.args.get("catalog")
            db_arg = table_expr.args.get("db")
            db = (catalog_arg.name if catalog_arg else "").lower()
            sch = (db_arg.name if db_arg else "").lower()
            tbl = (table_expr.name or "").lower()
            candidates = {tbl}
            if sch:
                candidates.add(f"{sch}.{tbl}")
            if db and sch:
                candidates.add(f"{db}.{sch}.{tbl}")
            if candidates & target_keys:
                hits.add(alias.lower())
                hits.add(tbl)
        return hits

    # ----- clause walking -----

    def _events_from_clause(
        self,
        clause_kind: str,
        clause_node: exp.Expression,
        aliases: set[str],
        downstream_model: str,
    ) -> list[UsageEvent]:
        events: list[UsageEvent] = []
        # Window-internal cols are reported separately via
        # _walk_window_clauses; suppress them here unless we *are* walking
        # a window clause to avoid double-counting.
        suppress_inside_window = clause_kind not in ("partition_by",)
        for col in clause_node.find_all(exp.Column):
            if (col.table or "").lower() not in aliases:
                continue
            if suppress_inside_window and self._inside_window(col, clause_node):
                continue
            operator, selectivity, wrapping_fn = self._classify(col, clause_kind)
            events.append(
                UsageEvent(
                    column=col.name.lower(),
                    clause=clause_kind,
                    operator=operator,
                    selectivity=selectivity,
                    downstream_model=downstream_model,
                    wrapping_fn=wrapping_fn,
                )
            )
        return events

    @staticmethod
    def _inside_window(col: exp.Column, clause_node: exp.Expression) -> bool:
        """True if ``col`` has an ``exp.Window`` ancestor up to ``clause_node``."""
        node: exp.Expr | None = col.parent
        while node is not None and node is not clause_node:
            if isinstance(node, exp.Window):
                return True
            node = node.parent
        return False

    # ----- operator / selectivity classification -----

    @staticmethod
    def _classify(col: exp.Column, clause_kind: str) -> tuple[str, str, str]:
        """Classify ``(operator, selectivity, wrapping_fn)`` for a column ref.

        ``operator`` and ``wrapping_fn`` are reported separately. The
        legacy ``op_tag == "fn"`` collapse hid the actual comparison
        from downstream advisors, which prevented partition-pruning-
        friendly wrappers (``DATE()``, ``TIMESTAMP_TRUNC()``) from ever
        being recognised. ``"fn"`` is still emitted as the operator
        when no comparison was found and the column is wrapped, so
        existing back-compat code paths keep working.
        """
        node: exp.Expr | None = col.parent
        op_tag: str | None = None
        # ``wrapping_fn``: closest function ancestor between the column
        # and the eventual comparison operator. We record the first one
        # we encounter — that's the one BigQuery actually evaluates the
        # column through.
        wrapping_fn = ""
        while node is not None and op_tag is None:
            for cls, tag in _OP_BY_CLASS.items():
                if isinstance(node, cls):
                    op_tag = tag
                    break
            if op_tag is None:
                if isinstance(node, exp.Func) and not wrapping_fn:
                    # ``key`` is sqlglot's lower-cased function name
                    # (``date``, ``timestamp_trunc``, ``upper`` …).
                    wrapping_fn = (getattr(node, "key", "") or type(node).__name__).lower()
                if isinstance(
                    node,
                    (exp.Where, exp.Group, exp.Order, exp.Qualify, exp.Window, exp.Join),
                ):
                    break
                node = node.parent

        if op_tag is None:
            return (
                "fn" if wrapping_fn else "none",
                "none",
                wrapping_fn,
            )

        # Selectivity heuristic
        selectivity = "none"
        op_node = col.parent
        while op_node is not None and not isinstance(op_node, tuple(_OP_BY_CLASS.keys())):
            op_node = op_node.parent
        if op_node is not None:
            if any(isinstance(c, exp.Subquery) for c in op_node.find_all(exp.Subquery)):
                selectivity = "subquery"
            elif op_tag in _RANGE_OPS:
                selectivity = "range"
            elif any(
                isinstance(c, (exp.Literal, exp.Null, exp.Boolean))
                for c in op_node.find_all(exp.Literal, exp.Null, exp.Boolean)
            ):
                selectivity = "literal"
        return (op_tag, selectivity, wrapping_fn)
