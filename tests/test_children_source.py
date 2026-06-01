"""Tests for `meta children --source <ref>` — downstream models of a source."""

from __future__ import annotations

import pytest

from dbt_meta.commands import children
from dbt_meta.manifest.parser import ManifestParser


def _pick_consumed_source(manifest_path: str) -> dict:
    """Find a source that has at least one downstream consumer in child_map."""
    parser = ManifestParser(str(manifest_path))
    child_map = parser.manifest.get('child_map', {})
    sources = parser.manifest.get('sources', {})
    for uid, src in sources.items():
        if not uid.startswith('source.'):
            continue
        if child_map.get(uid):
            return {
                'uid': uid,
                'schema': src.get('schema', ''),
                'identifier': src.get('identifier') or src.get('name', ''),
                'source_name': src.get('source_name', ''),
            }
    pytest.skip("No consumed sources in manifest")


def test_children_from_source_by_schema_table(prod_manifest):
    src = _pick_consumed_source(str(prod_manifest))
    ref = f"{src['schema']}.{src['identifier']}"
    result = children(str(prod_manifest), ref, source_ref=ref)
    assert result, f"Expected downstream consumers for {ref}"
    assert all('table' in r and 'path' in r for r in result)


def test_children_from_source_by_logical_name(prod_manifest):
    src = _pick_consumed_source(str(prod_manifest))
    if not src['source_name']:
        pytest.skip("source has no logical source_name")
    ref = f"{src['source_name']}.{src['identifier']}"
    result = children(str(prod_manifest), ref, source_ref=ref)
    assert result


def test_children_unknown_source_returns_none(prod_manifest):
    result = children(
        str(prod_manifest),
        "nonexistent_ds.nonexistent_table_xyz",
        source_ref="nonexistent_ds.nonexistent_table_xyz",
    )
    assert result is None


def test_children_default_path_unchanged(prod_manifest):
    """source_ref=None must not regress the standard model lookup."""
    parser = ManifestParser(str(prod_manifest))
    for uid, _ in parser.manifest.get('nodes', {}).items():
        if uid.startswith('model.'):
            model_name = uid.split('.')[-1]
            break
    else:
        pytest.skip("No models")
    result = children(str(prod_manifest), model_name)
    assert result is not None
