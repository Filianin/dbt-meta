"""Partition column advisor.

BigQuery allows exactly ONE partition column per table. The advisor
recommends the column that maximises partition pruning across the
target's **direct** downstream models (transitive descendants read the
intermediate model's storage, not the original table, so their WHERE
clauses don't affect this table's pruning).

Eligibility:
    - DATE / DATETIME / TIMESTAMP — time-based partitioning
    - INT64 — RANGE_BUCKET partitioning (less common, flagged as such)
    - all other types are excluded

Score per column = sum over WHERE-events of:
    eq      × 2.0
    between × 3.0
    range   × 2.5  (single-sided GT/GE/LT/LE)
    in      × 1.5
    fn      × 0.0  (function-wrapped column kills partition pruning)
× type bonus (TIMESTAMP/DATE × 1.5, DATETIME × 1.3, INT64 × 1.0)
× log2(direct_downstream_filtering_on_column + 1).

Output adds two operational signals beyond the score:
    - ``models_using_pruning``  — direct downstream with a prunable
      WHERE on a candidate column (the advisor recommends to keep).
    - ``models_without_pruning`` — direct downstream that scan the table
      with no prunable WHERE (full-scan candidates the user might
      revisit).
    - ``matches_current`` flag — set when the top recommendation equals
      ``config.partition_by``, so the CLI can show
      "current partitioning is optimal" instead of an apparent diff.
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
    downstream_short_to_materialized,
    find_target_node,
)

# Materialization values where reading the upstream table partially
# (with partition pruning) is the intent — so missing pruning is a bug.
INCREMENTAL_MATERIALIZATIONS = {"incremental", "materialized_view"}

# Wrapping functions that BigQuery's optimizer prunes through. A
# TIMESTAMP partition column accessed via ``DATE(col)`` /
# ``TIMESTAMP_TRUNC(col, DAY)`` / similar still prunes — BigQuery
# recognises these monotonic transformations. Other wrappers (``UPPER``,
# ``CAST``, arithmetic) defeat pruning.
#
# Names are sqlglot's lower-cased function keys.
# Names are sqlglot's lower-cased function keys — *without* underscores,
# which is how sqlglot normalises ``TIMESTAMP_TRUNC`` → ``timestamptrunc``
# etc. Don't add underscored variants; they won't match.
PARTITION_PRUNING_FRIENDLY_FNS = {
    "date",
    "timestamp",
    "datetime",
    "timestamptrunc",
    "datetimetrunc",
    "datetrunc",
    "timetrunc",
}
from dbt_meta.usage.extractor import ColumnUsageExtractor, UsageEvent

TIME_TYPES = {"TIMESTAMP", "DATE", "DATETIME"}
INT_TYPES = {"INT64", "INTEGER", "INT", "BIGINT"}


def _base_type(dtype: str) -> str:
    if not dtype:
        return ""
    base = dtype
    for sep in ("(", "<"):
        if sep in base:
            base = base.split(sep, 1)[0]
    return base.strip().upper()


@dataclass
class _PartitionCandidate:
    score: float = 0.0
    eq: int = 0
    in_: int = 0
    range_: int = 0
    between: int = 0
    fn: int = 0
    downstream_set: set[str] = field(default_factory=set)


@dataclass
class PartitionRecommendation:
    column: str
    data_type: str
    granularity: str
    score: float
    pruning_impact_pct: float
    reasoning: list[str]
    # Per-downstream classification. Each list contains downstream short
    # names; the materialization category determines whether a missing
    # filter is a bug or an expected full-scan refresh pattern.
    incremental_with_pruning: list[str] = field(default_factory=list)
    # ❗ The problem list: incremental downstream that read this table
    # without filtering on the partition column. Each one triggers a
    # full upstream scan on every incremental run.
    incremental_without_pruning: list[str] = field(default_factory=list)
    non_incremental_with_pruning: list[str] = field(default_factory=list)
    non_incremental_full_scan: list[str] = field(default_factory=list)
    # Backwards-compatible aggregate lists (union of the four buckets
    # above) — kept so JSON consumers and tests that referenced the old
    # field names don't break.
    models_using_pruning: list[str] = field(default_factory=list)
    models_without_pruning: list[str] = field(default_factory=list)


@dataclass
class PartitionAdvisorResult:
    target_model: str
    current_partition_by: list[str]
    direct_downstream_count: int
    analysed_downstream_count: int
    recommendation: Optional[PartitionRecommendation] = None
    alternatives: list[PartitionRecommendation] = field(default_factory=list)
    # Direct-downstream breakdown by materialization. ``incremental_count``
    # is where partition pruning matters most — these run repeatedly and
    # should filter on the partition column. Full-scan reads in
    # ``non_incremental_count`` are expected (table/view refresh).
    incremental_count: int = 0
    non_incremental_count: int = 0
    matches_current: bool = False
    warnings: list[str] = field(default_factory=list)


class PartitionAdvisor:
    OP_WEIGHTS = {
        "eq": 2.0,
        "in": 1.5,
        "between": 3.0,
        "gt": 2.5, "ge": 2.5, "lt": 2.5, "le": 2.5,
        "fn": 0.0,
    }
    TYPE_BONUS = {
        "TIMESTAMP": 1.5, "DATE": 1.5,
        "DATETIME": 1.3,
        "INT64": 1.0, "INTEGER": 1.0, "INT": 1.0, "BIGINT": 1.0,
    }

    def __init__(
        self,
        manifest: dict[str, Any],
        catalog: Optional[dict[str, Any]] = None,
        extractor: Optional[ColumnUsageExtractor] = None,
    ) -> None:
        self.manifest = manifest
        self.catalog = catalog or {}
        self.extractor = extractor or ColumnUsageExtractor(dialect="bigquery")

    def recommend(self, target_model_short: str) -> PartitionAdvisorResult:
        target = find_target_node(self.manifest, target_model_short)
        if target is None:
            return PartitionAdvisorResult(
                target_model=target_model_short,
                current_partition_by=[],
                direct_downstream_count=0,
                analysed_downstream_count=0,
                warnings=[f"Model '{target_model_short}' not found in manifest"],
            )
        unique_id, model = target
        from dbt_meta.usage._common import model_partition_columns

        current_part = sorted(model_partition_columns(model))
        # Only direct readers can benefit from partition pruning; deeper
        # descendants read the intermediate model's storage.
        downstream_ids = direct_downstream(self.manifest, unique_id)
        materialized_map = downstream_short_to_materialized(self.manifest, downstream_ids)
        # Pre-compute the incremental/non-incremental split across ALL
        # direct downstream — not just the analyzed ones — so the
        # "13 of 15 read this table without filtering" line is honest
        # even when the extractor skipped a model.
        incremental_total = sum(
            1 for mat in materialized_map.values()
            if mat in INCREMENTAL_MATERIALIZATIONS
        )
        non_incremental_total = sum(
            1 for mat in materialized_map.values()
            if mat and mat not in INCREMENTAL_MATERIALIZATIONS
        )

        events = collect_events(self.manifest, model, self.extractor, downstream_ids)
        analysed_downstream = {e.downstream_model for e in events}

        col_types = column_types(self.catalog, unique_id)
        candidates = self._score(events, col_types)

        # The full universe of downstream "without pruning" for any
        # given recommendation must include direct readers the extractor
        # never produced events for (SELECT * passthroughs, jinja-only
        # SQL, etc.) — those still scan the upstream and matter for the
        # incremental-bug classification.
        all_direct_shorts = {
            uid.split(".")[-1] for uid in downstream_ids
        }

        ranked = sorted(candidates.items(), key=lambda kv: kv[1].score, reverse=True)
        recs: list[PartitionRecommendation] = []
        for col, cand in ranked:
            if cand.score <= 0:
                continue
            dtype = col_types.get(col, "")
            base = _base_type(dtype)
            granularity = self._pick_granularity(base)
            pruning_pct = (
                100.0 * len(cand.downstream_set) / len(analysed_downstream)
                if analysed_downstream else 0.0
            )
            models_using = sorted(cand.downstream_set)
            models_without = sorted(all_direct_shorts - cand.downstream_set)

            # Split each bucket by materialization so the CLI can
            # highlight the bug class — incremental WITHOUT pruning.
            inc_with: list[str] = []
            non_inc_with: list[str] = []
            for m in models_using:
                if materialized_map.get(m, "") in INCREMENTAL_MATERIALIZATIONS:
                    inc_with.append(m)
                else:
                    non_inc_with.append(m)
            inc_without: list[str] = []
            non_inc_without: list[str] = []
            for m in models_without:
                if materialized_map.get(m, "") in INCREMENTAL_MATERIALIZATIONS:
                    inc_without.append(m)
                else:
                    non_inc_without.append(m)

            recs.append(
                PartitionRecommendation(
                    column=col,
                    data_type=dtype,
                    granularity=granularity,
                    score=round(cand.score, 2),
                    pruning_impact_pct=round(pruning_pct, 1),
                    reasoning=self._format_reasons(
                        cand, base, models_using, models_without,
                        inc_without_count=len(inc_without),
                    ),
                    incremental_with_pruning=inc_with,
                    incremental_without_pruning=inc_without,
                    non_incremental_with_pruning=non_inc_with,
                    non_incremental_full_scan=non_inc_without,
                    models_using_pruning=models_using,
                    models_without_pruning=models_without,
                )
            )

        primary = recs[0] if recs else None
        alts = recs[1:5]
        warnings: list[str] = []
        if not analysed_downstream and downstream_ids:
            diag = diagnose_no_extraction(self.manifest, downstream_ids)
            if diag:
                warnings.append(diag)

        matches_current = bool(
            primary
            and len(current_part) == 1
            and primary.column.lower() == current_part[0].lower()
        )

        return PartitionAdvisorResult(
            target_model=target_model_short,
            current_partition_by=current_part,
            direct_downstream_count=len(downstream_ids),
            analysed_downstream_count=len(analysed_downstream),
            recommendation=primary,
            alternatives=alts,
            incremental_count=incremental_total,
            non_incremental_count=non_incremental_total,
            matches_current=matches_current,
            warnings=warnings,
        )

    def _score(
        self,
        events: list[UsageEvent],
        col_types: dict[str, str],
    ) -> dict[str, _PartitionCandidate]:
        out: dict[str, _PartitionCandidate] = {}
        for ev in events:
            if ev.clause != "where":
                continue
            if ev.operator not in self.OP_WEIGHTS:
                # Unknown WHERE operator (neq/is_null/like/none) is not
                # partition-prunable — skip entirely so the score matches
                # the visible reasoning.
                continue
            dtype = col_types.get(ev.column, "")
            base = _base_type(dtype)
            if base not in TIME_TYPES and base not in INT_TYPES:
                continue

            # ``wrapping_fn`` decides whether BigQuery prunes through the
            # function wrap. ``DATE(timestamp_col)``,
            # ``TIMESTAMP_TRUNC(col, DAY)``, etc. ARE pruned by BQ — treat
            # them as a normal range/equality filter. Other wrappers
            # (``UPPER``, arithmetic, CAST to incompatible type) defeat
            # pruning — count as ``fn`` (weight 0) and skip the
            # downstream from the "uses pruning" set.
            wrapping = (ev.wrapping_fn or "").lower()
            if ev.operator == "fn" or (wrapping and wrapping not in PARTITION_PRUNING_FRIENDLY_FNS):
                cand = out.setdefault(ev.column, _PartitionCandidate())
                # fn weight is 0 — keeps the candidate from being
                # recommended on the strength of unprunable filters
                # alone, but lets us record the count for reasoning.
                cand.fn += 1
                continue

            cand = out.setdefault(ev.column, _PartitionCandidate())
            w = self.OP_WEIGHTS[ev.operator]
            cand.score += w
            cand.downstream_set.add(ev.downstream_model)
            if ev.operator == "eq":
                cand.eq += 1
            elif ev.operator == "in":
                cand.in_ += 1
            elif ev.operator == "between":
                cand.between += 1
            elif ev.operator in ("gt", "ge", "lt", "le"):
                cand.range_ += 1

        for col, cand in out.items():
            base = col_types.get(col, "").split("(", 1)[0].strip().upper()
            cand.score *= self.TYPE_BONUS.get(base, 1.0)
            cand.score *= max(math.log2(len(cand.downstream_set) + 1), 1.0)
        return out

    @staticmethod
    def _pick_granularity(base_type: str) -> str:
        if base_type in TIME_TYPES:
            return "DAY"
        if base_type in INT_TYPES:
            return "RANGE_BUCKET"
        return "NONE"

    @staticmethod
    def _format_reasons(
        cand: _PartitionCandidate,
        base_type: str,
        models_using: list[str],
        models_without: list[str],
        inc_without_count: int = 0,
    ) -> list[str]:
        """Human-readable explanation — one bullet per signal that fed score.

        Avoids cryptic shorthand like ``WHERE range ×1``. Each bullet
        names what the downstream does and whether it helps pruning.
        """
        bits: list[str] = []
        using = len(models_using)
        without = len(models_without)
        if cand.between:
            bits.append(
                f"{cand.between} downstream model"
                f"{'s' if cand.between != 1 else ''} use BETWEEN range filter"
            )
        if cand.range_:
            bits.append(
                f"{cand.range_} downstream model"
                f"{'s' if cand.range_ != 1 else ''} use >, <, >=, <= range filter"
            )
        if cand.eq:
            bits.append(
                f"{cand.eq} downstream model"
                f"{'s' if cand.eq != 1 else ''} use equality (=) filter"
            )
        if cand.in_:
            bits.append(
                f"{cand.in_} downstream model"
                f"{'s' if cand.in_ != 1 else ''} use IN (...) filter"
            )
        if cand.fn:
            bits.append(
                f"{cand.fn} downstream model"
                f"{'s' if cand.fn != 1 else ''} wrap the column in a function "
                "(e.g. DATE(col)) — those queries CANNOT prune partitions"
            )
        bits.append(
            f"{using} of {using + without} direct downstream would use partition pruning"
        )
        if inc_without_count:
            bits.append(
                f"❗ {inc_without_count} incremental downstream model"
                f"{'s' if inc_without_count != 1 else ''} read this table "
                "WITHOUT filtering on the partition column — that's a bug "
                "(incremental run scans the whole upstream every time)"
            )
        bits.append(f"type {base_type} ({'time' if base_type in TIME_TYPES else 'int range'} partitioning)")
        return bits
