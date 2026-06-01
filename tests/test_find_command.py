"""Tests for `meta find <fqn>` — reverse FQN → dbt model lookup."""

from __future__ import annotations

import pytest

from dbt_meta.commands import find
from dbt_meta.errors import DbtMetaError
from dbt_meta.manifest.parser import ManifestParser


def _pick_known_model(manifest_path: str) -> dict:
    """Pick a real model from the prod manifest as a target for find()."""
    parser = ManifestParser(str(manifest_path))
    for uid, node in parser.manifest.get('nodes', {}).items():
        if not uid.startswith('model.'):
            continue
        if not node.get('schema'):
            continue
        return {
            'uid': uid,
            'name': uid.split('.')[-1],
            'schema': node['schema'],
            'database': node.get('database', ''),
            'table': node.get('config', {}).get('alias') or node.get('name'),
        }
    pytest.skip("No models with schema in manifest")


def test_find_by_table_only(prod_manifest):
    """Bare table name matches every model with that physical alias."""
    target = _pick_known_model(prod_manifest)
    result = find(str(prod_manifest), target['table'])
    assert any(r['unique_id'] == target['uid'] for r in result), \
        f"Expected {target['uid']} in result, got {[r['unique_id'] for r in result]}"


def test_find_by_schema_table(prod_manifest):
    """schema.table narrows to one schema."""
    target = _pick_known_model(prod_manifest)
    result = find(str(prod_manifest), f"{target['schema']}.{target['table']}")
    assert len(result) >= 1
    for r in result:
        assert r['schema'] == target['schema']
        assert r['table'] == target['table']


def test_find_by_database_schema_table(prod_manifest):
    """Full FQN restricts to exact database/schema/table."""
    target = _pick_known_model(prod_manifest)
    if not target['database']:
        pytest.skip("Model has no database set")
    fqn = f"{target['database']}.{target['schema']}.{target['table']}"
    result = find(str(prod_manifest), fqn)
    assert any(r['unique_id'] == target['uid'] for r in result)


def test_find_nonexistent_returns_empty(prod_manifest):
    """Unknown table → empty list, not error."""
    result = find(str(prod_manifest), "definitely_nonexistent_table_xyz")
    assert result == []


def test_find_rejects_four_part_name(prod_manifest):
    """4-part FQNs are nonsense for BigQuery — raise."""
    with pytest.raises(DbtMetaError):
        find(str(prod_manifest), "a.b.c.d")


def test_find_result_shape(prod_manifest):
    """Result entries expose the full lookup contract."""
    target = _pick_known_model(prod_manifest)
    result = find(str(prod_manifest), target['table'])
    assert result, "expected at least one hit"
    row = result[0]
    for key in ('name', 'unique_id', 'database', 'schema', 'table', 'alias',
                'materialized', 'file'):
        assert key in row, f"missing key {key!r} in find() result"
