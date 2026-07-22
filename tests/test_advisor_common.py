"""Unit tests for the shared advisor helpers in ``usage/_common.py``.

Covers the pipeline preamble (``resolve_advisor_context``), the warning tail
(``no_extraction_warnings``) and the ``base_type`` helper — the pieces the
cluster and partition advisors share after the C4 dedup.
"""

import pytest

from dbt_meta.usage._common import (
    base_type,
    no_extraction_warnings,
    resolve_advisor_context,
)
from dbt_meta.usage.extractor import ColumnUsageExtractor


class TestBaseType:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("", ""),
            ("STRING", "STRING"),
            ("STRING(100)", "STRING"),
            ("STRUCT<x INT64>", "STRUCT"),
            ("ARRAY<INT64>", "ARRAY"),
            ("  int64  ", "INT64"),
            ("NUMERIC(38, 9)", "NUMERIC"),
        ],
    )
    def test_base_type(self, raw, expected):
        assert base_type(raw) == expected


class TestResolveAdvisorContext:
    @staticmethod
    def _manifest():
        return {
            "nodes": {
                "model.pkg.tgt": {
                    "name": "tgt", "alias": "tgt",
                    "schema": "core", "database": "db",
                },
                "model.pkg.child": {
                    "name": "child",
                    "compiled_code": (
                        "SELECT * FROM db.core.tgt "
                        "WHERE event_date = '2024-01-01'"
                    ),
                },
            },
            "child_map": {
                "model.pkg.tgt": ["model.pkg.child"],
                "model.pkg.child": [],
            },
        }

    def test_not_found_returns_none(self):
        ctx = resolve_advisor_context(
            {"nodes": {}, "child_map": {}}, {}, ColumnUsageExtractor(), "ghost"
        )
        assert ctx is None

    def test_happy_path_populates_context(self):
        ctx = resolve_advisor_context(
            self._manifest(), {}, ColumnUsageExtractor(), "tgt"
        )
        assert ctx is not None
        assert ctx.unique_id == "model.pkg.tgt"
        assert ctx.downstream_ids == ["model.pkg.child"]
        assert "child" in ctx.analysed_downstream
        assert any(e.column == "event_date" for e in ctx.events)

    def test_no_downstream_leaves_events_empty(self):
        manifest = {
            "nodes": {"model.pkg.tgt": {"name": "tgt", "schema": "core"}},
            "child_map": {"model.pkg.tgt": []},
        }
        ctx = resolve_advisor_context(manifest, {}, ColumnUsageExtractor(), "tgt")
        assert ctx is not None
        assert ctx.downstream_ids == []
        assert ctx.events == []
        assert ctx.analysed_downstream == set()


class TestNoExtractionWarnings:
    def test_analysed_nonempty_no_warning(self):
        assert no_extraction_warnings({}, {"child"}, ["model.pkg.child"]) == []

    def test_no_downstream_no_warning(self):
        assert no_extraction_warnings({}, set(), []) == []

    def test_downstream_but_none_analysed_warns(self):
        manifest = {"nodes": {"model.pkg.c": {"compiled_code": ""}}, "child_map": {}}
        out = no_extraction_warnings(manifest, set(), ["model.pkg.c"])
        assert len(out) == 1
        assert "compiled_code" in out[0]

    def test_diagnose_returns_none_no_warning(self):
        # unresolved downstream id → diagnose sees total == 0 → None → []
        out = no_extraction_warnings(
            {"nodes": {}, "child_map": {}}, set(), ["model.pkg.missing"]
        )
        assert out == []
