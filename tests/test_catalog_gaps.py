"""Tests covering gap branches in catalog/parser.py.

Targets:
- line 65: json.load fallback when orjson unavailable
- lines 130-137: get_table_stats populated-result path
- lines 172-174: is_stale invalid-timestamp handler
- line 236: _normalize_type with empty/None type
"""

from __future__ import annotations

import json

import pytest

from dbt_meta.catalog.parser import CatalogParser


@pytest.fixture
def catalog_with_stats(tmp_path):
    """Catalog file with full table stats for a model."""
    path = tmp_path / 'catalog.json'
    payload = {
        'metadata': {'generated_at': '2026-03-01T00:00:00Z'},
        'nodes': {
            'model.admirals_bi_dwh.core__events': {
                'stats': {
                    'row_count': {'value': 123456},
                    'bytes': {'value': 9876543},
                },
                'columns': {},
            },
        },
    }
    path.write_text(json.dumps(payload))
    return str(path)


class TestGetTableStats:
    def test_returns_stats_when_model_found(self, catalog_with_stats):
        parser = CatalogParser(catalog_with_stats)
        stats = parser.get_table_stats('core__events')

        assert stats == {'row_count': 123456, 'bytes': 9876543}

    def test_returns_none_when_model_missing(self, catalog_with_stats):
        parser = CatalogParser(catalog_with_stats)
        assert parser.get_table_stats('missing__model') is None

    def test_returns_nulls_when_stats_absent(self, tmp_path):
        path = tmp_path / 'catalog.json'
        path.write_text(json.dumps({
            'metadata': {},
            'nodes': {
                'model.admirals_bi_dwh.no_stats': {
                    'columns': {},
                    # stats key missing entirely
                },
            },
        }))
        parser = CatalogParser(str(path))
        stats = parser.get_table_stats('no_stats')
        assert stats == {'row_count': None, 'bytes': None}

    def test_accepts_custom_project_name(self, tmp_path):
        path = tmp_path / 'catalog.json'
        path.write_text(json.dumps({
            'metadata': {},
            'nodes': {
                'model.my_project.x': {
                    'stats': {'row_count': {'value': 1}, 'bytes': {'value': 2}},
                    'columns': {},
                },
            },
        }))
        parser = CatalogParser(str(path))
        stats = parser.get_table_stats('x', project_name='my_project')
        assert stats == {'row_count': 1, 'bytes': 2}


class TestIsStaleInvalidTimestamp:
    def test_invalid_timestamp_treated_as_stale(self, tmp_path):
        path = tmp_path / 'catalog.json'
        path.write_text(json.dumps({
            'metadata': {'generated_at': 'not-a-real-timestamp'},
            'nodes': {},
        }))
        parser = CatalogParser(str(path))
        assert parser.is_stale(max_age_hours=1) is True

    def test_invalid_timestamp_returns_none_for_age(self, tmp_path):
        path = tmp_path / 'catalog.json'
        path.write_text(json.dumps({
            'metadata': {'generated_at': 'garbage'},
            'nodes': {},
        }))
        parser = CatalogParser(str(path))
        assert parser.get_age_hours() is None


class TestNormalizeTypeEdgeCases:
    def test_empty_string_returns_unknown(self):
        assert CatalogParser._normalize_type('') == 'unknown'

    def test_none_returns_unknown(self):
        # the public API passes string but guard handles falsy input
        assert CatalogParser._normalize_type(None) == 'unknown'

    def test_unknown_type_is_lowercased(self):
        assert CatalogParser._normalize_type('CustomType') == 'customtype'


class TestJsonFallback:
    """Cover line 65: json.load path when orjson is unavailable."""

    def test_uses_json_stdlib_when_orjson_disabled(self, tmp_path, monkeypatch):
        from dbt_meta.catalog import parser as parser_module

        # Force the import flag off so the fallback branch is taken
        monkeypatch.setattr(parser_module, 'HAS_ORJSON', False)
        # Ensure `json` module is available as attribute under expected name
        # (it was imported inside the `except ImportError` branch of the real code,
        # which may or may not have run; make sure the reference exists)
        if not hasattr(parser_module, 'json'):
            import json as stdlib_json
            monkeypatch.setattr(parser_module, 'json', stdlib_json, raising=False)

        path = tmp_path / 'catalog.json'
        payload = {'metadata': {}, 'nodes': {'model.p.m': {'columns': {}}}}
        path.write_text(json.dumps(payload))

        parser = CatalogParser(str(path))
        data = parser.catalog  # triggers lazy load

        assert 'nodes' in data
        assert 'model.p.m' in data['nodes']
