"""Tests covering gap branches in command_impl/path.py.

Targets:
- line 79: early return from _search_by_bigquery_format_dev when not use_dev
- lines 88-90: ManifestNotFoundError/ManifestParseError in dev search
- line 95: bigquery-format input with <2 parts in dev search
- line 113: schema mismatch skip in dev search
- lines 121-125: dev_pattern branches (alias, unknown)
- lines 145-147: manifest error in prod search
- line 152: <2 parts in prod search
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from dbt_meta.command_impl.path import PathCommand
from dbt_meta.errors import ManifestNotFoundError


def build_config(tmp_path, dev_manifest=None):
    cfg = Mock()
    cfg.fallback_dev_enabled = True
    cfg.fallback_bigquery_enabled = False
    cfg.prod_manifest_path = str(tmp_path / 'prod_manifest.json')
    cfg.dev_manifest_path = str(dev_manifest or tmp_path / 'dev_manifest.json')
    return cfg


def write_manifest(path: Path, nodes: dict) -> None:
    path.write_text(json.dumps({
        'metadata': {'project_name': 'p'},
        'nodes': nodes,
        'sources': {},
        'macros': {},
        'exposures': {},
    }))


class TestSearchBigQueryFormatDev:
    def test_returns_none_when_not_use_dev(self, tmp_path):
        cfg = build_config(tmp_path)
        cmd = PathCommand(
            config=cfg,
            manifest_path='/fake/manifest.json',
            model_name='core.events',
            use_dev=False,
        )

        assert cmd._search_by_bigquery_format_dev() is None

    def test_returns_none_when_less_than_two_parts(self, tmp_path):
        dev_manifest = tmp_path / 'dev.json'
        write_manifest(dev_manifest, {})

        cfg = build_config(tmp_path, dev_manifest=dev_manifest)
        cmd = PathCommand(
            config=cfg,
            manifest_path=str(dev_manifest),
            model_name='singleword',  # no dot
            use_dev=True,
        )

        assert cmd._search_by_bigquery_format_dev() is None

    def test_returns_none_when_dev_manifest_missing(self, tmp_path, monkeypatch):
        # find_dev_manifest returns None
        monkeypatch.setattr(
            'dbt_meta.utils.dev.find_dev_manifest',
            lambda _p: None,
        )

        cfg = build_config(tmp_path)
        cmd = PathCommand(
            config=cfg,
            manifest_path='/fake/manifest.json',
            model_name='core.events',
            use_dev=True,
        )

        assert cmd._search_by_bigquery_format_dev() is None

    def test_handles_manifest_parse_error_gracefully(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            'dbt_meta.utils.dev.find_dev_manifest',
            lambda _p: '/nonexistent/manifest.json',
        )

        def _raise(_path):
            raise ManifestNotFoundError(['/nonexistent/manifest.json'])

        monkeypatch.setattr('dbt_meta.utils.get_cached_parser', _raise)

        cfg = build_config(tmp_path)
        cmd = PathCommand(
            config=cfg,
            manifest_path='/fake/manifest.json',
            model_name='core.events',
            use_dev=True,
        )

        assert cmd._search_by_bigquery_format_dev() is None

    def test_skips_nodes_with_mismatched_schema(self, tmp_path, monkeypatch):
        dev_manifest = tmp_path / 'dev.json'
        write_manifest(dev_manifest, {
            'model.p.other': {
                'resource_type': 'model',
                'schema': 'other_schema',  # doesn't match
                'name': 'events',
                'config': {},
                'original_file_path': 'models/x.sql',
            },
        })

        monkeypatch.setattr(
            'dbt_meta.utils.dev.find_dev_manifest',
            lambda _p: str(dev_manifest),
        )

        cfg = build_config(tmp_path, dev_manifest=dev_manifest)
        cmd = PathCommand(
            config=cfg,
            manifest_path=str(dev_manifest),
            model_name='core.events',
            use_dev=True,
        )

        assert cmd._search_by_bigquery_format_dev() is None

    def test_finds_model_with_alias_pattern(self, tmp_path, monkeypatch):
        dev_manifest = tmp_path / 'dev.json'
        write_manifest(dev_manifest, {
            'model.p.events': {
                'resource_type': 'model',
                'schema': 'core',
                'name': 'stg_events',
                'config': {'alias': 'events'},
                'original_file_path': 'models/core/events.sql',
            },
        })

        monkeypatch.setattr(
            'dbt_meta.utils.dev.find_dev_manifest',
            lambda _p: str(dev_manifest),
        )
        monkeypatch.setenv('DBT_DEV_TABLE_PATTERN', 'alias')

        cfg = build_config(tmp_path, dev_manifest=dev_manifest)
        cmd = PathCommand(
            config=cfg,
            manifest_path=str(dev_manifest),
            model_name='core.events',
            use_dev=True,
        )

        result = cmd._search_by_bigquery_format_dev()
        assert result is not None
        assert result['name'] == 'stg_events'

    def test_unknown_pattern_falls_back_to_name(self, tmp_path, monkeypatch):
        dev_manifest = tmp_path / 'dev.json'
        write_manifest(dev_manifest, {
            'model.p.events': {
                'resource_type': 'model',
                'schema': 'core',
                'name': 'events',
                'config': {},
                'original_file_path': 'models/core/events.sql',
            },
        })

        monkeypatch.setattr(
            'dbt_meta.utils.dev.find_dev_manifest',
            lambda _p: str(dev_manifest),
        )
        monkeypatch.setenv('DBT_DEV_TABLE_PATTERN', 'custom_unknown')

        cfg = build_config(tmp_path, dev_manifest=dev_manifest)
        cmd = PathCommand(
            config=cfg,
            manifest_path=str(dev_manifest),
            model_name='core.events',
            use_dev=True,
        )

        result = cmd._search_by_bigquery_format_dev()
        assert result is not None


class TestSearchBigQueryFormatProd:
    def test_returns_none_when_less_than_two_parts(self, tmp_path):
        prod_manifest = tmp_path / 'prod.json'
        write_manifest(prod_manifest, {})

        cfg = build_config(tmp_path)
        cmd = PathCommand(
            config=cfg,
            manifest_path=str(prod_manifest),
            model_name='singleword',
            use_dev=False,
        )

        assert cmd._search_by_bigquery_format_prod() is None

    def test_handles_manifest_error_gracefully(self, tmp_path, monkeypatch):
        def _raise(_p):
            raise ManifestNotFoundError(['/nonexistent/manifest.json'])

        monkeypatch.setattr('dbt_meta.utils.get_cached_parser', _raise)

        cfg = build_config(tmp_path)
        cmd = PathCommand(
            config=cfg,
            manifest_path='/nonexistent/manifest.json',
            model_name='core.events',
            use_dev=False,
        )

        assert cmd._search_by_bigquery_format_prod() is None
