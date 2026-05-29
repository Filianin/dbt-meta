"""Tests covering gap branches in command_impl/columns.py.

Targets:
- lines 212-214: infer_table_parts fallback in _fetch_from_bigquery_without_model
- lines 248-249: suggestion printed for NEW_UNCOMMITTED/COMMITTED when not found
- lines 278, 286-289, 294, 306-307, 312-314, 325-333: _try_fetch_from_catalog branches
- line 401: MODIFIED_IN_DEV dev-version warning
- line 412: json_output early return in _print_not_found_message
- line 421: NEW_UNCOMMITTED/COMMITTED suggestion in _print_not_found_message
- line 453: process_model backward-compat stub
"""

from __future__ import annotations

import json
from io import StringIO
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from dbt_meta.command_impl.columns import ColumnsCommand
from dbt_meta.fallback import FallbackLevel
from dbt_meta.utils.model_state import ModelState


def make_config(tmp_path, fallback_catalog=True, prod_catalog_path=None, dev_catalog_path=None, use_dev_default=False):
    """Build a Mock config with sensible defaults."""
    cfg = Mock()
    cfg.fallback_bigquery_enabled = True
    cfg.fallback_catalog_enabled = fallback_catalog
    cfg.fallback_dev_enabled = True
    cfg.prod_table_name_strategy = 'alias_or_name'
    cfg.prod_schema_source = 'config_or_model'
    cfg.prod_catalog_path = prod_catalog_path
    cfg.dev_catalog_path = dev_catalog_path
    cfg.dev_schema = 'personal_test_user'
    cfg.prod_manifest_path = str(tmp_path / 'prod_manifest.json')
    cfg.dev_manifest_path = str(tmp_path / 'dev_manifest.json')
    return cfg


class TestFetchBigQueryWithoutModelFallback:
    """Lines 212-214: infer_table_parts fallback when prod_model is None."""

    def test_modified_uncommitted_no_prod_model_uses_infer(self, tmp_path, monkeypatch):
        cfg = make_config(tmp_path)
        cmd = ColumnsCommand(
            config=cfg,
            manifest_path='/fake/manifest.json',
            model_name='core__events',
            use_dev=False,
        )

        # Stub BigQuery fetch so we can inspect call args
        called = {}

        def _fake_fetch(schema, table, database=None):
            called['schema'] = schema
            called['table'] = table
            called['database'] = database
            return [{'name': 'id', 'data_type': 'INT64'}]

        monkeypatch.setattr(
            'dbt_meta.command_impl.columns._fetch_columns_from_bigquery_direct',
            _fake_fetch,
        )
        monkeypatch.setattr(
            'dbt_meta.utils.bigquery.infer_table_parts',
            lambda name: ('core', 'events'),
        )

        result = cmd._fetch_from_bigquery_without_model(
            state=ModelState.MODIFIED_UNCOMMITTED,
            prod_model=None,
        )

        assert result is not None
        assert called == {'schema': 'core', 'table': 'events', 'database': None}


class TestFetchBigQueryWithoutModelNewNotFound:
    """Lines 248-249: NEW state final suggestion when nothing found."""

    def test_new_uncommitted_final_suggestion(self, tmp_path, monkeypatch, capsys):
        cfg = make_config(tmp_path)
        cmd = ColumnsCommand(
            config=cfg,
            manifest_path='/fake/manifest.json',
            model_name='brand_new',
            use_dev=False,
        )

        # BigQuery never returns any columns
        monkeypatch.setattr(
            'dbt_meta.command_impl.columns._fetch_columns_from_bigquery_direct',
            lambda *a, **kw: None,
        )

        result = cmd._fetch_from_bigquery_without_model(
            state=ModelState.NEW_UNCOMMITTED,
            prod_model=None,
        )

        assert result is None
        err = capsys.readouterr().err
        # Suggests building via defer run
        assert 'defer run --select brand_new' in err


class TestTryFetchFromCatalog:
    """Cover _try_fetch_from_catalog branches with DBT_META_DEBUG and staleness."""

    def _make_cmd(self, cfg, model_name='m', use_dev=False):
        return ColumnsCommand(
            config=cfg,
            manifest_path='/fake/manifest.json',
            model_name=model_name,
            use_dev=use_dev,
        )

    def test_disabled_fallback_returns_none(self, tmp_path, monkeypatch, capsys):
        cfg = make_config(tmp_path, fallback_catalog=False)
        monkeypatch.setenv('DBT_META_DEBUG', '1')
        cmd = self._make_cmd(cfg)

        result = cmd._try_fetch_from_catalog({'schema': 's', 'alias': 't'}, ModelState.PROD_STABLE)

        assert result is None
        err = capsys.readouterr().err
        assert 'Catalog disabled' in err

    def test_missing_catalog_path_returns_none(self, tmp_path, monkeypatch, capsys):
        cfg = make_config(tmp_path, prod_catalog_path=None)
        monkeypatch.setenv('DBT_META_DEBUG', '1')
        cmd = self._make_cmd(cfg, use_dev=False)

        result = cmd._try_fetch_from_catalog({'schema': 's', 'alias': 't'}, ModelState.PROD_STABLE)

        assert result is None
        err = capsys.readouterr().err
        assert 'Catalog path not configured' in err

    def test_missing_catalog_file_returns_none(self, tmp_path, monkeypatch, capsys):
        missing = str(tmp_path / 'nope.json')
        cfg = make_config(tmp_path, prod_catalog_path=missing)
        monkeypatch.setenv('DBT_META_DEBUG', '1')
        cmd = self._make_cmd(cfg)

        result = cmd._try_fetch_from_catalog({'schema': 's', 'alias': 't'}, ModelState.PROD_STABLE)

        assert result is None
        err = capsys.readouterr().err
        assert 'Catalog not found' in err

    def test_stale_file_mtime_returns_none(self, tmp_path, monkeypatch, capsys):
        # Make a file that mtime-wise is very old
        import os
        import time
        catalog = tmp_path / 'catalog.json'
        catalog.write_text(json.dumps({'metadata': {'generated_at': '2020-01-01T00:00:00Z'}, 'nodes': {}}))
        # Set mtime to 50 hours ago
        very_old = time.time() - (50 * 3600)
        os.utime(str(catalog), (very_old, very_old))

        cfg = make_config(tmp_path, prod_catalog_path=str(catalog))
        cmd = self._make_cmd(cfg)

        result = cmd._try_fetch_from_catalog({'schema': 's', 'alias': 't'}, ModelState.PROD_STABLE)

        assert result is None
        err = capsys.readouterr().err
        assert 'not updated for' in err

    def test_old_internal_age_logs_info_but_still_uses_catalog(self, tmp_path, capsys, monkeypatch):
        """Fresh file but generated_at >7d → info message, still returns columns."""
        import os
        import time
        # Internal timestamp is 10 days old
        catalog = tmp_path / 'catalog.json'
        catalog.write_text(json.dumps({
            'metadata': {'generated_at': '2020-01-01T00:00:00Z'},
            'nodes': {
                'model.admirals_bi_dwh.m': {
                    'columns': {
                        'id': {'name': 'id', 'type': 'INT64', 'index': 0},
                    },
                },
            },
        }))
        # File mtime is recent
        recent = time.time() - 60
        os.utime(str(catalog), (recent, recent))

        cfg = make_config(tmp_path, prod_catalog_path=str(catalog))
        cmd = self._make_cmd(cfg)

        result = cmd._try_fetch_from_catalog({'schema': 's', 'alias': 't'}, ModelState.PROD_STABLE)

        assert result is not None
        assert result[0]['name'] == 'id'
        err = capsys.readouterr().err
        # Info message about age
        assert 'Catalog was generated' in err

    def test_model_not_in_catalog_returns_none(self, tmp_path, monkeypatch, capsys):
        catalog = tmp_path / 'catalog.json'
        catalog.write_text(json.dumps({
            'metadata': {'generated_at': '2026-03-01T00:00:00Z'},
            'nodes': {},  # no models
        }))

        cfg = make_config(tmp_path, prod_catalog_path=str(catalog))
        monkeypatch.setenv('DBT_META_DEBUG', '1')
        cmd = self._make_cmd(cfg, model_name='missing_model')

        result = cmd._try_fetch_from_catalog({'schema': 's', 'alias': 't'}, ModelState.PROD_STABLE)

        assert result is None
        err = capsys.readouterr().err
        assert 'Model not in catalog' in err

    def test_catalog_parse_error_returns_none(self, tmp_path, monkeypatch, capsys):
        # invalid JSON → orjson/json raises inside CatalogParser.catalog
        catalog = tmp_path / 'catalog.json'
        catalog.write_text('{"this is": "broken')

        cfg = make_config(tmp_path, prod_catalog_path=str(catalog))
        monkeypatch.setenv('DBT_META_DEBUG', '1')
        cmd = self._make_cmd(cfg)

        result = cmd._try_fetch_from_catalog({'schema': 's', 'alias': 't'}, ModelState.PROD_STABLE)

        assert result is None
        err = capsys.readouterr().err
        assert 'Catalog read failed' in err


class TestPrintResultMessageDevInDev:
    """Line 401: MODIFIED_IN_DEV dev-version warning."""

    def test_modified_in_dev_prints_dev_version_warning(self, tmp_path, capsys):
        cfg = make_config(tmp_path)
        cmd = ColumnsCommand(
            config=cfg,
            manifest_path='/fake/manifest.json',
            model_name='m',
            use_dev=True,
        )

        cmd._print_result_message(
            state=ModelState.MODIFIED_IN_DEV,
            column_count=4,
            table='personal_test_user.m',
            is_dev_table=True,
        )

        err = capsys.readouterr().err
        assert 'Using dev version' in err
        assert 'compiled in dev' in err


class TestPrintNotFoundMessage:
    """Lines 411-421: json_output early return + NEW state suggestion."""

    def test_json_mode_suppresses_output(self, tmp_path, capsys):
        cfg = make_config(tmp_path)
        cmd = ColumnsCommand(
            config=cfg,
            manifest_path='/fake/manifest.json',
            model_name='m',
            json_output=True,
        )

        cmd._print_not_found_message(ModelState.NOT_FOUND, attempted_table='s.t')

        err = capsys.readouterr().err
        assert err == '' or err.strip() == ''

    def test_new_state_suggestion_printed(self, tmp_path, capsys):
        cfg = make_config(tmp_path)
        cmd = ColumnsCommand(
            config=cfg,
            manifest_path='/fake/manifest.json',
            model_name='new_one',
        )

        cmd._print_not_found_message(ModelState.NEW_UNCOMMITTED, attempted_table='p.t')

        err = capsys.readouterr().err
        assert 'NEW but not built in dev' in err


class TestProcessModelBackwardCompat:
    """Line 453: deprecated process_model stub always returns None."""

    def test_process_model_returns_none(self, tmp_path):
        cfg = make_config(tmp_path)
        cmd = ColumnsCommand(
            config=cfg,
            manifest_path='/fake/manifest.json',
            model_name='m',
        )

        assert cmd.process_model({'name': 'm'}, level=FallbackLevel.PROD_MANIFEST) is None
