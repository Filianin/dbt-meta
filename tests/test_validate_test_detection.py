"""ValidateCommand should distinguish tests from models and return structured JSON."""

from __future__ import annotations

import pytest

from dbt_meta.commands import validate
from dbt_meta.manifest.parser import ManifestParser


def _pick_test(manifest_path: str) -> str:
    parser = ManifestParser(str(manifest_path))
    for uid, node in parser.manifest.get('nodes', {}).items():
        if node.get('resource_type') == 'test':
            return uid.split('.')[-1]
    pytest.skip("No tests in manifest")


def test_validate_test_returns_structured_result(prod_manifest):
    test_name = _pick_test(str(prod_manifest))
    result = validate(str(prod_manifest), test_name, use_dev=False, json_output=True)
    assert result is not None, "should not return None for a known test"
    assert result['valid'] is False
    assert result['kind'] == 'test'
    assert 'error' in result and 'test' in result['error'].lower()


def test_validate_genuinely_missing_model_still_returns_none(prod_manifest):
    """A bogus name that is neither model nor test must still surface None."""
    result = validate(
        str(prod_manifest),
        "definitely_nonexistent_xyz_zzz_123",
        use_dev=False,
        json_output=True,
    )
    assert result is None
