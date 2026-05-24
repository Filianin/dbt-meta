"""Tests for PartitionAdvisor."""

from dbt_meta.usage import PartitionAdvisor


def _model(short, *, sql, partition_by=None, alias=None, materialized="table"):
    config: dict = {"materialized": materialized}
    if partition_by:
        config["partition_by"] = partition_by
    return f"model.pkg.{short}", {
        "name": short,
        "alias": alias or short,
        "schema": "ds",
        "database": "proj",
        "compiled_code": sql,
        "config": config,
        "package_name": "pkg",
        "resource_type": "model",
        "depends_on": {"nodes": []},
    }


def _build_manifest(target, downstreams):
    nodes = {target[0]: target[1]}
    child_map = {target[0]: [d[0] for d in downstreams]}
    for uid, payload in downstreams:
        nodes[uid] = payload
        child_map.setdefault(uid, [])
    return {"nodes": nodes, "sources": {}, "child_map": child_map}


def _catalog(unique_id, columns):
    return {
        "nodes": {
            unique_id: {
                "metadata": {"database": "proj", "schema": "ds", "name": "target"},
                "columns": {c.lower(): {"name": c, "type": t} for c, t in columns.items()},
            }
        },
        "sources": {},
    }


class TestPartitionAdvisor:
    def test_picks_timestamp_with_range_filters(self):
        target = _model("target", sql="SELECT 1", alias="t")
        ds_range = _model(
            "ds_range",
            sql="SELECT * FROM proj.ds.t t WHERE t.created_at BETWEEN '2026-01-01' AND '2026-02-01'",
        )
        manifest = _build_manifest(target, [ds_range])
        catalog = _catalog(target[0], {"created_at": "TIMESTAMP", "name": "STRING"})

        advisor = PartitionAdvisor(manifest, catalog)
        res = advisor.recommend("target")
        assert res.recommendation is not None
        assert res.recommendation.column == "created_at"
        assert res.recommendation.granularity == "DAY"

    def test_skips_non_time_non_int_columns(self):
        target = _model("target", sql="SELECT 1", alias="t")
        ds = _model("ds1", sql="SELECT * FROM proj.ds.t t WHERE t.country = 'EE'")
        manifest = _build_manifest(target, [ds])
        catalog = _catalog(target[0], {"country": "STRING"})

        advisor = PartitionAdvisor(manifest, catalog)
        res = advisor.recommend("target")
        assert res.recommendation is None
        assert res.alternatives == []

    def test_int64_gets_range_bucket_granularity(self):
        target = _model("target", sql="SELECT 1", alias="t")
        ds = _model("ds1", sql="SELECT * FROM proj.ds.t t WHERE t.bucket_id BETWEEN 0 AND 100")
        manifest = _build_manifest(target, [ds])
        catalog = _catalog(target[0], {"bucket_id": "INT64"})

        advisor = PartitionAdvisor(manifest, catalog)
        res = advisor.recommend("target")
        assert res.recommendation is not None
        assert res.recommendation.granularity == "RANGE_BUCKET"

    def test_pruning_pct_reflects_filter_coverage(self):
        target = _model("target", sql="SELECT 1", alias="t")
        ds_filt = _model("ds_filt", sql="SELECT * FROM proj.ds.t t WHERE t.d BETWEEN '2026-01-01' AND '2026-02-01'")
        ds_no   = _model("ds_no",   sql="SELECT * FROM proj.ds.t t WHERE t.country = 'EE'")
        manifest = _build_manifest(target, [ds_filt, ds_no])
        catalog = _catalog(target[0], {"d": "DATE", "country": "STRING"})

        advisor = PartitionAdvisor(manifest, catalog)
        res = advisor.recommend("target")
        assert res.recommendation is not None
        # Filter present in 1 of 2 parsed downstream that referenced target
        assert 0.0 < res.recommendation.pruning_impact_pct <= 100.0

    def test_unknown_target_warns(self):
        advisor = PartitionAdvisor({"nodes": {}, "child_map": {}})
        res = advisor.recommend("nope")
        assert res.recommendation is None
        assert any("not found" in w for w in res.warnings)

    def test_pruning_defeating_function_wrap_scores_zero(self):
        # ``EXTRACT(YEAR FROM ts)`` collapses a timestamp into an int —
        # BigQuery cannot prune through it. Score must be zero (no
        # recommendation), unlike DATE/TIMESTAMP_TRUNC which BQ DOES
        # prune through (see test_friendly_wrapper_still_prunes).
        target = _model("target", sql="SELECT 1", alias="t")
        ds = _model("ds1", sql="SELECT * FROM proj.ds.t t WHERE EXTRACT(YEAR FROM t.created_at) = 2026")
        manifest = _build_manifest(target, [ds])
        catalog = _catalog(target[0], {"created_at": "TIMESTAMP"})

        advisor = PartitionAdvisor(manifest, catalog)
        res = advisor.recommend("target")
        assert res.recommendation is None

    def test_friendly_wrapper_still_prunes(self):
        # Regression: ``DATE(timestamp_col)`` / ``TIMESTAMP_TRUNC(col, DAY)``
        # are partition-pruning-friendly in BigQuery. The advisor must
        # treat them as bare comparisons, not function-wraps.
        target = _model("target", sql="SELECT 1", alias="t")
        ds_date = _model(
            "ds_date",
            sql="SELECT * FROM proj.ds.t t WHERE DATE(t.event_time) >= '2026-01-01'",
            materialized="incremental",
        )
        ds_trunc = _model(
            "ds_trunc",
            sql=(
                "SELECT * FROM proj.ds.t t "
                "WHERE TIMESTAMP_TRUNC(t.event_time, DAY) >= TIMESTAMP('2026-01-01')"
            ),
            materialized="incremental",
        )
        manifest = _build_manifest(target, [ds_date, ds_trunc])
        catalog = _catalog(target[0], {"event_time": "TIMESTAMP"})

        advisor = PartitionAdvisor(manifest, catalog)
        res = advisor.recommend("target")
        assert res.recommendation is not None
        assert res.recommendation.column == "event_time"
        # Both downstream qualify as "using pruning" thanks to the
        # whitelist — neither lands in the bug bucket.
        assert set(res.recommendation.incremental_with_pruning) == {"ds_date", "ds_trunc"}
        assert res.recommendation.incremental_without_pruning == []

    def test_classifies_downstream_by_materialization(self):
        # Incremental downstream WITHOUT partition pruning is a bug —
        # the advisor must call those out separately from table/view
        # downstream that scan the table by design (full-refresh).
        target = _model("target", sql="SELECT 1", alias="t")
        # Incremental + uses pruning → good
        inc_good = _model(
            "inc_good",
            sql="SELECT * FROM proj.ds.t t WHERE t.event_time >= '2026-01-01'",
            materialized="incremental",
        )
        # Incremental + NO filter on event_time → BUG
        inc_bug = _model(
            "inc_bug",
            sql="SELECT * FROM proj.ds.t t",
            materialized="incremental",
        )
        # Table + no filter → expected full-refresh scan
        table_scan = _model(
            "table_scan",
            sql="SELECT * FROM proj.ds.t t",
            materialized="table",
        )
        manifest = _build_manifest(target, [inc_good, inc_bug, table_scan])
        catalog = _catalog(target[0], {"event_time": "TIMESTAMP"})

        advisor = PartitionAdvisor(manifest, catalog)
        res = advisor.recommend("target")
        assert res.direct_downstream_count == 3
        assert res.incremental_count == 2
        assert res.non_incremental_count == 1

        rec = res.recommendation
        assert rec is not None
        assert rec.column == "event_time"
        assert rec.incremental_with_pruning == ["inc_good"]
        # The bug class — incremental that reads the table without
        # filtering on the partition column.
        assert rec.incremental_without_pruning == ["inc_bug"]
        assert rec.non_incremental_full_scan == ["table_scan"]
        # Reasoning must call out the bug explicitly.
        assert any("incremental" in line and "WITHOUT" in line for line in rec.reasoning)

    def test_diagnoses_empty_compiled_code(self):
        # Regression: a dev manifest from ``dbt parse`` has empty
        # compiled_code everywhere → extractor yields 0 events → advisor
        # reports ``0/N`` and "no qualifying downstream usage", which
        # reads as "no consumers" to users unaware of the parse/compile
        # distinction. Surface the real cause as a warning.
        target = _model("target", sql="SELECT 1", alias="t")
        # 5 downstream models, all with empty compiled_code
        downstreams = [
            (
                f"model.pkg.ds{i}",
                {
                    "name": f"ds{i}",
                    "alias": f"ds{i}",
                    "schema": "ds",
                    "database": "proj",
                    "compiled_code": "",
                    "config": {},
                    "package_name": "pkg",
                    "resource_type": "model",
                    "depends_on": {"nodes": []},
                },
            )
            for i in range(5)
        ]
        manifest = _build_manifest(target, downstreams)
        advisor = PartitionAdvisor(manifest, _catalog(target[0], {"x": "INT64"}))
        res = advisor.recommend("target")
        assert res.direct_downstream_count == 5
        assert res.analysed_downstream_count == 0
        assert res.recommendation is None
        assert any("compiled_code" in w for w in res.warnings)
        assert any("dbt parse" in w or "dbt compile" in w for w in res.warnings)

    def test_unknown_operators_dropped_no_silent_fallback(self):
        # Regression: WHERE neq/like/is_null used to silently add 0.5 to
        # the score via ``dict.get(..., 0.5)`` and surface as TIMESTAMP/INT
        # columns with mysterious low scores and empty reasoning counters.
        target = _model("target", sql="SELECT 1", alias="t")
        downstream = _model(
            "ds1",
            sql=(
                "SELECT * FROM proj.ds.t t "
                "WHERE t.never_prunes != 0 "
                "AND t.also_no_prune IS NULL "
                "AND t.like_col LIKE 'foo%'"
            ),
        )
        manifest = _build_manifest(target, [downstream])
        catalog = _catalog(
            target[0],
            {
                "never_prunes": "TIMESTAMP",
                "also_no_prune": "DATE",
                "like_col": "TIMESTAMP",
            },
        )
        advisor = PartitionAdvisor(manifest, catalog)
        res = advisor.recommend("target")
        # None of these operators enable partition pruning, so the
        # advisor must return nothing — not a 0.75-score "alternative"
        # whose reasoning shows only "in 1 models · type bonus: …".
        assert res.recommendation is None
        assert res.alternatives == []
