"""Clustering-key advisor.

Given a target model with high downstream fan-out, score each of its columns
by how heavily downstream models filter on it (WHERE / JOIN / GROUP BY) and
recommend up to N (default 4) cluster keys ordered by score.

Heuristic weights (explainable, tunable):

    where_eq      × 3.0
    where_in      × 2.5
    where_range   × 2.0  (BETWEEN, GT, GE, LT, LE)
    where_fn      × 0.5  (column wrapped in a function — limited prunability)
    join          × 2.0
    group_by      × 1.0
    × log2(downstream_models_using_column + 1)   — frequency multiplier

Excluded:
    - the model's own ``partition_by`` column (BigQuery rule)
    - column types unfit for clustering: STRUCT, ARRAY, GEOGRAPHY, JSON
    - columns absent from catalog (we can't verify their type)
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Optional

from dbt_meta.usage._common import (
    collect_events,
    column_types,
    diagnose_no_extraction,
    direct_downstream,
    find_target_node,
    model_partition_columns,
    upstream_table_aliases,
)
from dbt_meta.usage.extractor import ColumnUsageExtractor, UsageEvent

# BigQuery upper bound for clustering keys
BQ_MAX_CLUSTER_KEYS = 4

# Column types not usable as cluster keys in BigQuery
CLUSTER_DISALLOWED_TYPES = {"STRUCT", "ARRAY", "GEOGRAPHY", "JSON"}


def _base_type(dtype: str) -> str:
    """Extract the BigQuery base type name from a catalog type string.

    Handles both ``STRING(100)`` and ``STRUCT<x INT64>`` / ``ARRAY<INT64>`` /
    ``RECORD<...>``.
    """
    if not dtype:
        return ""
    # Trim either parenthesised or angle-bracketed parameter list
    base = dtype
    for sep in ("(", "<"):
        if sep in base:
            base = base.split(sep, 1)[0]
    return base.strip().upper()


@dataclass
class _ColScore:
    score: float = 0.0
    where_eq: int = 0
    where_in: int = 0
    where_range: int = 0
    where_fn: int = 0
    join: int = 0
    group_by: int = 0
    downstream_set: set[str] = field(default_factory=set)


@dataclass
class ClusterRecommendation:
    column: str
    data_type: str
    score: float
    reasoning: list[str]


@dataclass
class ClusterAdvisorResult:
    target_model: str
    target_partition_by: list[str]
    current_cluster_by: list[str]
    direct_downstream_count: int
    analysed_downstream_count: int
    recommendations: list[ClusterRecommendation]
    excluded: list[dict[str, str]]
    matches_current: bool = False
    warnings: list[str] = field(default_factory=list)


class ClusterAdvisor:
    """Recommend clustering keys for a target model."""

    # Operators NOT listed here contribute 0.0 — clustering benefits only
    # from prunable predicates. ``neq``/``is_null``/``like``/``none`` from
    # the extractor are intentionally absent: they neither prune scans nor
    # constrain block ranges, so giving them a fallback weight would
    # silently inflate scores without surfacing in ``reasoning``.
    WEIGHTS_WHERE = {
        "eq": 3.0,
        "in": 2.5,
        "between": 2.0,
        "gt": 2.0, "ge": 2.0, "lt": 2.0, "le": 2.0,
        "fn": 0.5,
    }
    WEIGHT_JOIN = 2.0
    WEIGHT_GROUP = 1.0

    def __init__(
        self,
        manifest: dict[str, Any],
        catalog: Optional[dict[str, Any]] = None,
        extractor: Optional[ColumnUsageExtractor] = None,
        top_n: int = BQ_MAX_CLUSTER_KEYS,
    ) -> None:
        self.manifest = manifest
        self.catalog = catalog or {}
        self.extractor = extractor or ColumnUsageExtractor(dialect="bigquery")
        self.top_n = max(1, min(top_n, BQ_MAX_CLUSTER_KEYS))

    def recommend(self, target_model_short: str) -> ClusterAdvisorResult:
        target = find_target_node(self.manifest, target_model_short)
        if target is None:
            return ClusterAdvisorResult(
                target_model=target_model_short,
                target_partition_by=[],
                current_cluster_by=[],
                direct_downstream_count=0,
                analysed_downstream_count=0,
                recommendations=[],
                excluded=[],
                warnings=[f"Model '{target_model_short}' not found in manifest"],
            )
        unique_id, model = target
        partition_cols = model_partition_columns(model)
        from dbt_meta.usage._common import model_cluster_columns

        current_cluster = sorted(model_cluster_columns(model))
        # Only DIRECT children matter for clustering: grandchildren read
        # the intermediate model's storage, not the original table.
        downstream_ids = direct_downstream(self.manifest, unique_id)

        events = collect_events(self.manifest, model, self.extractor, downstream_ids)
        analysed_downstream = {e.downstream_model for e in events}

        scores = self._score(events)
        col_types = column_types(self.catalog, unique_id)

        recommendations, excluded = self._rank(
            scores, col_types, partition_cols, analysed_downstream
        )

        warnings: list[str] = []
        if not analysed_downstream and downstream_ids:
            diag = diagnose_no_extraction(self.manifest, downstream_ids)
            if diag:
                warnings.append(diag)

        # Match by set equality (order doesn't matter for cluster_by;
        # BigQuery does prefix-match pruning but the dbt config is just
        # a list of columns).
        recommended_cols = sorted(r.column.lower() for r in recommendations)
        matches_current = bool(
            recommendations
            and current_cluster
            and recommended_cols == sorted(c.lower() for c in current_cluster)
        )

        return ClusterAdvisorResult(
            target_model=target_model_short,
            target_partition_by=sorted(partition_cols),
            current_cluster_by=current_cluster,
            direct_downstream_count=len(downstream_ids),
            analysed_downstream_count=len(analysed_downstream),
            recommendations=recommendations,
            excluded=excluded,
            matches_current=matches_current,
            warnings=warnings,
        )

    # ----- scoring -----

    def _score(self, events: list[UsageEvent]) -> dict[str, _ColScore]:
        scores: dict[str, _ColScore] = {}
        for ev in events:
            # Function-wrapped column ref (``UPPER(col) = …``,
            # ``CAST(col AS INT64) > …``, etc.) — clustering still helps
            # a bit (BQ scans fewer blocks when the wrapped value
            # correlates with the cluster key) but much less than a bare
            # filter, hence the 0.5 weight. ``operator == "fn"`` is the
            # back-compat case (column wrapped, no comparison found).
            is_fn_wrapped = ev.operator == "fn" or bool(ev.wrapping_fn)

            if ev.clause == "where":
                if is_fn_wrapped:
                    weight = self.WEIGHTS_WHERE["fn"]
                else:
                    if ev.operator not in self.WEIGHTS_WHERE:
                        continue
                    weight = self.WEIGHTS_WHERE[ev.operator]
            elif ev.clause == "join":
                weight = self.WEIGHT_JOIN
            elif ev.clause == "group_by":
                weight = self.WEIGHT_GROUP
            else:
                continue

            cs = scores.setdefault(ev.column, _ColScore())
            cs.downstream_set.add(ev.downstream_model)
            cs.score += weight

            if ev.clause == "where":
                if is_fn_wrapped:
                    cs.where_fn += 1
                elif ev.operator == "eq":
                    cs.where_eq += 1
                elif ev.operator == "in":
                    cs.where_in += 1
                elif ev.operator in ("between", "gt", "ge", "lt", "le"):
                    cs.where_range += 1
            elif ev.clause == "join":
                cs.join += 1
            elif ev.clause == "group_by":
                cs.group_by += 1

        # Frequency multiplier
        for cs in scores.values():
            mult = math.log2(len(cs.downstream_set) + 1)
            cs.score *= max(mult, 1.0)
        return scores

    def _rank(
        self,
        scores: dict[str, _ColScore],
        col_types: dict[str, str],
        partition_cols: set[str],
        analysed_downstream: set[str],
    ) -> tuple[list[ClusterRecommendation], list[dict[str, str]]]:
        recs: list[ClusterRecommendation] = []
        excluded: list[dict[str, str]] = []

        for col, cs in scores.items():
            if cs.score <= 0:
                continue
            dtype = col_types.get(col, "")
            base = _base_type(dtype)
            if col in partition_cols:
                excluded.append({"column": col, "reason": "is partition column"})
                continue
            if base in CLUSTER_DISALLOWED_TYPES:
                excluded.append({"column": col, "reason": f"type {base} not allowed for clustering"})
                continue

            reasons = self._format_reasons(cs, len(analysed_downstream))
            recs.append(
                ClusterRecommendation(
                    column=col,
                    data_type=dtype,
                    score=round(cs.score, 2),
                    reasoning=reasons,
                )
            )

        recs.sort(key=lambda r: r.score, reverse=True)
        return recs[: self.top_n], excluded

    @staticmethod
    def _format_reasons(cs: _ColScore, analysed_total: int) -> list[str]:
        """Human-readable reasons — one bullet per signal that scored.

        Replaces cryptic ``WHERE eq ×3 · JOIN ×7`` with sentences that
        say what downstream does and why it helps clustering.
        """
        bits: list[str] = []
        if cs.where_eq:
            bits.append(
                f"{cs.where_eq} downstream filter"
                f"{'s' if cs.where_eq != 1 else ''} use equality (=) "
                "— clustering greatly speeds these up"
            )
        if cs.where_in:
            bits.append(
                f"{cs.where_in} downstream filter"
                f"{'s' if cs.where_in != 1 else ''} use IN (...) "
                "— clustering speeds these up"
            )
        if cs.where_range:
            bits.append(
                f"{cs.where_range} downstream filter"
                f"{'s' if cs.where_range != 1 else ''} use range "
                "(BETWEEN / >, <) — clustering helps"
            )
        if cs.join:
            bits.append(
                f"{cs.join} downstream JOIN"
                f"{'s' if cs.join != 1 else ''} on this column — "
                "clustering reduces shuffle"
            )
        if cs.group_by:
            bits.append(
                f"{cs.group_by} downstream GROUP BY"
                f"{'s' if cs.group_by != 1 else ''} — clustering helps "
                "co-locate keys"
            )
        if cs.where_fn:
            bits.append(
                f"{cs.where_fn} downstream filter"
                f"{'s' if cs.where_fn != 1 else ''} wrap the column in a "
                "function — clustering helps less but still scans fewer blocks"
            )
        users = len(cs.downstream_set)
        bits.append(
            f"used by {users} of {analysed_total} analysed direct downstream"
        )
        return bits

