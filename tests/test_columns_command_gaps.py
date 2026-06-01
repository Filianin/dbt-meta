"""Tests covering gap branches in column_source.py and columns.py.

Targets:
- infer_table_parts fallback in BigQueryColumnSource._fetch_without_model
- suggestion printed for NEW_UNCOMMITTED/COMMITTED when not found
- CatalogColumnSource._try_catalog branches (disabled, missing path/file, stale, old age, not in catalog, parse error)
- MODIFIED_IN_DEV dev-version warning in BigQueryColumnSource
- json_output early return in BigQueryColumnSource._print_not_found_message
- NEW_UNCOMMITTED/COMMITTED suggestion in BigQueryColumnSource._print_not_found_message
- process_model backward-compat stub on ColumnsCommand
"""

from __future__ import annotations

import json
import os
import time
from unittest.mock import Mock, patch

import pytest

from dbt_meta.command_impl.column_source import BigQueryColumnSource, CatalogColumnSource
from dbt_meta.command_impl.columns import ColumnsCommand
from dbt_meta.config import Config
from dbt_meta.fallback import FallbackLevel
from dbt_meta.utils.model_state import ModelState


def _bq(use_dev=False, json_output=False):
    return BigQueryColumnSource(use_dev=use_dev, json_output=json_output)


def _catalog_source(cfg, use_dev=False):
    return CatalogColumnSource(cfg, use_dev=use_dev)


def _cfg_mock(tmp_path, fallback_catalog=True, prod_catalog_path=None, dev_catalog_path=None):
    cfg = Mock()
    cfg.fallback_catalog_enabled = fallback_catalog
    cfg.prod_catalog_path = prod_catalog_path
    cfg.dev_catalog_path = dev_catalog_path
    return cfg


class TestFetchBigQueryWithoutModelFallback:
    """infer_table_parts fallback when prod_model is None."""

    def test_modified_uncommitted_no_prod_model_uses_infer(self, monkeypatch):
        source = _bq(use_dev=False)
        called = {}

        def _fake_fetch(schema, table, database=None):
            called.update(schema=schema, table=table, database=database)
            return [{'name': 'id', 'data_type': 'INT64'}]

        monkeypatch.setattr('dbt_meta.command_impl.column_source._fetch_columns_from_bigquery_direct', _fake_fetch)
        monkeypatch.setattr('dbt_meta.command_impl.column_source.infer_table_parts', lambda name: ('core', 'events'))

        result = source._fetch_without_model('core__events', ModelState.MODIFIED_UNCOMMITTED, prod_model=None)

        assert result is not None
        assert called == {'schema': 'core', 'table': 'events', 'database': None}


class TestFetchBigQueryWithoutModelNewNotFound:
    """NEW state final suggestion when nothing found."""

    def test_new_uncommitted_final_suggestion(self, monkeypatch, capsys):
        source = _bq(use_dev=False)
        monkeypatch.setattr(
            'dbt_meta.command_impl.column_source._fetch_columns_from_bigquery_direct',
            lambda *a, **kw: None,
        )
        monkeypatch.setattr(
            'dbt_meta.command_impl.column_source._calculate_dev_schema',
            lambda: 'personal_test',
        )

        result = source._fetch_without_model('brand_new', ModelState.NEW_UNCOMMITTED, prod_model=None)

        assert result is None
        err = capsys.readouterr().err
        assert 'defer run --select brand_new' in err


class TestTryFetchFromCatalog:
    """Cover CatalogColumnSource._try_catalog branches."""

    def test_disabled_fallback_returns_none(self, tmp_path, monkeypatch, capsys):
        cfg = _cfg_mock(tmp_path, fallback_catalog=False)
        monkeypatch.setenv('DBT_META_DEBUG', '1')

        result = _catalog_source(cfg)._try_catalog({'schema': 's', 'alias': 't'}, 'm', ModelState.PROD_STABLE)

        assert result is None
        assert 'Catalog disabled' in capsys.readouterr().err

    def test_missing_catalog_path_returns_none(self, tmp_path, monkeypatch, capsys):
        cfg = _cfg_mock(tmp_path, prod_catalog_path=None)
        monkeypatch.setenv('DBT_META_DEBUG', '1')

        result = _catalog_source(cfg)._try_catalog({'schema': 's', 'alias': 't'}, 'm', ModelState.PROD_STABLE)

        assert result is None
        assert 'Catalog path not configured' in capsys.readouterr().err

    def test_missing_catalog_file_returns_none(self, tmp_path, monkeypatch, capsys):
        missing = str(tmp_path / 'nope.json')
        cfg = _cfg_mock(tmp_path, prod_catalog_path=missing)
        monkeypatch.setenv('DBT_META_DEBUG', '1')

        result = _catalog_source(cfg)._try_catalog({'schema': 's', 'alias': 't'}, 'm', ModelState.PROD_STABLE)

        assert result is None
        assert 'Catalog not found' in capsys.readouterr().err

    def test_stale_file_mtime_returns_none(self, tmp_path, capsys):
        catalog = tmp_path / 'catalog.json'
        catalog.write_text(json.dumps({'metadata': {'generated_at': '2020-01-01T00:00:00Z'}, 'nodes': {}}))
        very_old = time.time() - (50 * 3600)
        os.utime(str(catalog), (very_old, very_old))

        cfg = _cfg_mock(tmp_path, prod_catalog_path=str(catalog))
        result = _catalog_source(cfg)._try_catalog({'schema': 's', 'alias': 't'}, 'm', ModelState.PROD_STABLE)

        assert result is None
        assert 'not updated for' in capsys.readouterr().err

    def test_old_internal_age_logs_info_but_still_uses_catalog(self, tmp_path, capsys):
        catalog = tmp_path / 'catalog.json'
        catalog.write_text(json.dumps({
            'metadata': {'generated_at': '2020-01-01T00:00:00Z'},
            'nodes': {
                'model.admirals_bi_dwh.m': {
                    'columns': {'id': {'name': 'id', 'type': 'INT64', 'index': 0}},
                },
            },
        }))
        os.utime(str(catalog), (time.time() - 60, time.time() - 60))

        cfg = _cfg_mock(tmp_path, prod_catalog_path=str(catalog))
        result = _catalog_source(cfg)._try_catalog({'schema': 's', 'alias': 't'}, 'm', ModelState.PROD_STABLE)

        assert result is not None
        assert result[0]['name'] == 'id'
        assert 'Catalog was generated' in capsys.readouterr().err

    def test_model_not_in_catalog_returns_none(self, tmp_path, monkeypatch, capsys):
        catalog = tmp_path / 'catalog.json'
        catalog.write_text(json.dumps({
            'metadata': {'generated_at': '2026-03-01T00:00:00Z'},
            'nodes': {},
        }))
        monkeypatch.setenv('DBT_META_DEBUG', '1')

        cfg = _cfg_mock(tmp_path, prod_catalog_path=str(catalog))
        result = _catalog_source(cfg)._try_catalog({'schema': 's', 'alias': 't'}, 'missing_model', ModelState.PROD_STABLE)

        assert result is None
        assert 'Model not in catalog' in capsys.readouterr().err

    def test_catalog_parse_error_returns_none(self, tmp_path, monkeypatch, capsys):
        catalog = tmp_path / 'catalog.json'
        catalog.write_text('{"this is": "broken')
        monkeypatch.setenv('DBT_META_DEBUG', '1')

        cfg = _cfg_mock(tmp_path, prod_catalog_path=str(catalog))
        result = _catalog_source(cfg)._try_catalog({'schema': 's', 'alias': 't'}, 'm', ModelState.PROD_STABLE)

        assert result is None
        assert 'Catalog read failed' in capsys.readouterr().err


class TestPrintResultMessageDevInDev:
    """MODIFIED_IN_DEV dev-version warning."""

    def test_modified_in_dev_prints_dev_version_warning(self, capsys):
        BigQueryColumnSource._print_result_message(ModelState.MODIFIED_IN_DEV, 4, 'personal_test_user.m', is_dev=True)

        err = capsys.readouterr().err
        assert 'Using dev version' in err
        assert 'compiled in dev' in err


class TestPrintNotFoundMessage:
    """json_output early return + NEW state suggestion."""

    def test_json_mode_suppresses_output(self, capsys):
        _bq(json_output=True)._print_not_found_message(ModelState.NOT_FOUND, 'm', attempted='s.t')
        assert capsys.readouterr().err.strip() == ''

    def test_new_state_suggestion_printed(self, capsys):
        _bq()._print_not_found_message(ModelState.NEW_UNCOMMITTED, 'new_one', attempted='p.t')
        assert 'NEW but not built in dev' in capsys.readouterr().err


class TestProcessModelBackwardCompat:
    """process_model stub always returns None."""

    def test_process_model_returns_none(self, tmp_path):
        cfg = Config.from_env()
        cfg.prod_manifest_path = str(tmp_path / 'prod_manifest.json')
        cfg.dev_manifest_path = str(tmp_path / 'dev_manifest.json')
        cmd = ColumnsCommand(config=cfg, manifest_path='/fake/manifest.json', model_name='m')

        assert cmd.process_model({'name': 'm'}, level=FallbackLevel.PROD_MANIFEST) is None
