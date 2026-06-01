"""Tests for `meta columns --all <pattern>` — repo-wide column search."""

from __future__ import annotations

import pytest

from dbt_meta.commands import columns_search
from dbt_meta.manifest.parser import ManifestParser


def _pick_documented_column(manifest_path: str) -> tuple[str, str]:
    """Find any (model_unique_id, column_name) pair with a declared column."""
    parser = ManifestParser(str(manifest_path))
    for uid, node in parser.manifest.get('nodes', {}).items():
        if not uid.startswith('model.'):
            continue
        cols = node.get('columns') or {}
        if cols:
            return uid, next(iter(cols))
    pytest.skip("No models with documented columns in manifest")


def test_columns_search_finds_known_column(prod_manifest):
    uid, col = _pick_documented_column(str(prod_manifest))
    result = columns_search(str(prod_manifest), col)
    matched = [r for r in result if r['unique_id'] == uid and r['column'] == col]
    assert matched, f"Expected hit for {uid}.{col}, got {len(result)} results"


def test_columns_search_substring_match(prod_manifest):
    """Substring of a known column also matches."""
    uid, col = _pick_documented_column(str(prod_manifest))
    if len(col) < 3:
        pytest.skip("column name too short for substring test")
    needle = col[1:-1]
    result = columns_search(str(prod_manifest), needle)
    assert any(r['column'] == col and r['unique_id'] == uid for r in result)


def test_columns_search_case_insensitive_by_default(prod_manifest):
    uid, col = _pick_documented_column(str(prod_manifest))
    result = columns_search(str(prod_manifest), col.upper())
    assert any(r['column'] == col and r['unique_id'] == uid for r in result)


def test_columns_search_case_sensitive_flag(prod_manifest):
    """case_sensitive=True must not match mismatched case."""
    uid, col = _pick_documented_column(str(prod_manifest))
    # Build a name that exists only in lowercase form
    flipped = col.swapcase()
    if flipped == col:
        pytest.skip("column name has no case to flip")
    result = columns_search(str(prod_manifest), flipped, case_sensitive=True)
    assert not any(r['column'] == col and r['unique_id'] == uid for r in result)


def test_columns_search_nonexistent_returns_empty(prod_manifest):
    result = columns_search(str(prod_manifest), "definitely_no_such_column_xyz_zzz")
    assert result == []


def test_columns_search_result_shape(prod_manifest):
    uid, col = _pick_documented_column(str(prod_manifest))
    result = columns_search(str(prod_manifest), col)
    assert result
    row = result[0]
    for key in ('model', 'unique_id', 'column', 'data_type', 'description'):
        assert key in row
