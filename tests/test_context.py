"""Tests for the context command - single queryable-shape bundle.

Covers ContextCommand orchestration (FQN, config fields, column left-join,
catalog stats) and CLI multi-model keyed output.
"""

import json
import warnings

import pytest
from typer.testing import CliRunner

from dbt_meta.cli import app
from dbt_meta.command_impl.context import ContextCommand
from dbt_meta.config import Config


def _cfg() -> Config:
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', category=DeprecationWarning)
        return Config.from_env()


def _make_cmd(model_name="core_client__events", use_dev=False):
    return ContextCommand(_cfg(), "/nonexistent/manifest.json", model_name, use_dev, False)


PROD_MODEL = {
    'name': 'events',
    'database': 'my_project',
    'schema': 'analytics',
    'description': 'Event stream table',
    'tags': ['daily', 'core'],
    'config': {
        'materialized': 'incremental',
        'alias': 'core_client__events',
        'partition_by': 'event_date',
        'cluster_by': ['user_id', 'event_type'],
        'unique_key': ['event_id'],
    },
}


class TestProcessModel:
    """Bundle assembly from a resolved model."""

    def test_prod_bundle_has_all_fields(self, mocker):
        cmd = _make_cmd()
        mocker.patch.object(cmd, '_build_columns', return_value=[
            {'name': 'event_id', 'data_type': 'STRING', 'description': 'PK'},
        ])
        mocker.patch.object(cmd, '_table_stats', return_value=(123, 456))

        bundle = cmd.process_model(PROD_MODEL)

        assert bundle['name'] == 'core_client__events'
        assert bundle['full_name'] == 'my_project.analytics.core_client__events'
        assert bundle['materialized'] == 'incremental'
        assert bundle['description'] == 'Event stream table'
        assert bundle['tags'] == ['daily', 'core']
        assert bundle['partition_by'] == 'event_date'
        assert bundle['cluster_by'] == ['user_id', 'event_type']
        assert bundle['unique_key'] == ['event_id']
        assert bundle['row_count'] == 123
        assert bundle['bytes'] == 456
        assert bundle['columns'][0]['name'] == 'event_id'

    def test_dev_bundle_uses_dev_schema(self, mocker, monkeypatch):
        monkeypatch.setenv('DBT_DEV_SCHEMA', 'personal_tester')
        cmd = _make_cmd(use_dev=True)
        mocker.patch.object(cmd, '_build_columns', return_value=[])
        mocker.patch.object(cmd, '_table_stats', return_value=(None, None))

        bundle = cmd.process_model(PROD_MODEL)

        assert bundle['full_name'].startswith('personal_tester.')
        assert bundle['row_count'] is None
        assert bundle['bytes'] is None

    def test_missing_config_fields_default_to_none(self, mocker):
        cmd = _make_cmd()
        mocker.patch.object(cmd, '_build_columns', return_value=[])
        mocker.patch.object(cmd, '_table_stats', return_value=(None, None))

        bundle = cmd.process_model({'name': 'm', 'database': 'd', 'schema': 's', 'config': {}})

        assert bundle['partition_by'] is None
        assert bundle['cluster_by'] is None
        assert bundle['unique_key'] is None
        assert bundle['materialized'] == 'table'
        assert bundle['description'] == ''
        assert bundle['tags'] == []


class TestBuildColumns:
    """BQ/catalog spine with manifest descriptions left-joined."""

    def test_left_join_descriptions(self, mocker):
        cmd = _make_cmd()
        cols_cls = mocker.patch('dbt_meta.command_impl.context.ColumnsCommand')
        cols_cls.return_value.execute.return_value = [
            {'name': 'event_id', 'data_type': 'STRING'},
            {'name': 'amount', 'data_type': 'NUMERIC'},
        ]
        docs_cls = mocker.patch('dbt_meta.command_impl.context.DocsCommand')
        docs_cls.return_value.execute.return_value = [
            {'name': 'event_id', 'data_type': 'STRING', 'description': 'Primary key'},
            {'name': 'ghost', 'data_type': 'STRING', 'description': 'not in BQ'},
        ]

        result = cmd._build_columns()

        assert result == [
            {'name': 'event_id', 'data_type': 'STRING', 'description': 'Primary key'},
            {'name': 'amount', 'data_type': 'NUMERIC', 'description': ''},
        ]
        # Column present only in docs (not BQ) is dropped (BQ is the spine).
        assert all(c['name'] != 'ghost' for c in result)

    def test_no_columns_returns_empty(self, mocker):
        cmd = _make_cmd()
        mocker.patch('dbt_meta.command_impl.context.ColumnsCommand').return_value.execute.return_value = None
        mocker.patch('dbt_meta.command_impl.context.DocsCommand').return_value.execute.return_value = None

        assert cmd._build_columns() == []


class TestTableStats:
    """Catalog stats lookup; degrades to (None, None)."""

    def test_no_catalog_path(self, monkeypatch):
        cmd = _make_cmd()
        cmd.config.prod_catalog_path = '/definitely/missing/catalog.json'
        assert cmd._table_stats() == (None, None)

    def test_reads_catalog(self, tmp_path):
        catalog = tmp_path / "catalog.json"
        catalog.write_text(json.dumps({
            "metadata": {"generated_at": "2026-06-10T00:00:00Z"},
            "nodes": {
                "model.admirals_bi_dwh.core_client__events": {
                    "stats": {
                        "num_rows": {"value": 999},
                        "num_bytes": {"value": 8888},
                    }
                }
            },
        }))
        cmd = _make_cmd()
        cmd.config.prod_catalog_path = str(catalog)

        assert cmd._table_stats() == (999, 8888)

    def test_model_absent_from_catalog(self, tmp_path):
        catalog = tmp_path / "catalog.json"
        catalog.write_text(json.dumps({"metadata": {}, "nodes": {}}))
        cmd = _make_cmd()
        cmd.config.prod_catalog_path = str(catalog)

        assert cmd._table_stats() == (None, None)

    def test_malformed_catalog_degrades(self, tmp_path):
        catalog = tmp_path / "catalog.json"
        catalog.write_text("{ not valid json")
        cmd = _make_cmd()
        cmd.config.prod_catalog_path = str(catalog)

        assert cmd._table_stats() == (None, None)


class TestExecute:
    """End-to-end execute() against a real manifest."""

    def test_nonexistent_model_returns_none(self, prod_manifest):
        cmd = ContextCommand(_cfg(), str(prod_manifest), "nonexistent__model_xyz", False, False)
        assert cmd.execute() is None

    def test_process_model_is_noop_safe(self, mocker):
        # execute() delegates to process_model after a successful lookup.
        cmd = _make_cmd()
        mocker.patch.object(cmd, 'get_model_with_fallback', return_value=PROD_MODEL)
        mocker.patch.object(cmd, '_build_columns', return_value=[])
        mocker.patch.object(cmd, '_table_stats', return_value=(None, None))
        assert cmd.execute()['name'] == 'core_client__events'

    def test_execute_returns_none_when_lookup_fails(self, mocker):
        cmd = _make_cmd()
        mocker.patch.object(cmd, 'get_model_with_fallback', return_value=None)
        assert cmd.execute() is None


class TestContextCli:
    """CLI keyed-output and multi-model orchestration."""

    def setup_method(self):
        self.runner = CliRunner()

    def test_single_model_keyed_json(self, prod_manifest, test_model, mocker):
        mocker.patch(
            'dbt_meta.command_impl.context.ContextCommand.execute',
            return_value={'name': test_model, 'full_name': 'p.d.t', 'columns': []},
        )
        result = self.runner.invoke(app, ['context', test_model, '-j'])
        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        assert test_model in payload
        assert payload[test_model]['full_name'] == 'p.d.t'

    def test_multi_model_batch(self, prod_manifest, mocker):
        mocker.patch(
            'dbt_meta.command_impl.context.ContextCommand.execute',
            side_effect=lambda: {'name': 'x', 'full_name': 'p.d.x', 'columns': []},
        )
        result = self.runner.invoke(app, ['context', 'a', 'b', 'c', '-j'])
        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        assert set(payload.keys()) == {'a', 'b', 'c'}

    def test_mixed_found_and_missing(self, prod_manifest, mocker):
        def fake_execute(self):
            return None if self.model_name == 'missing' else {'name': self.model_name, 'columns': []}

        mocker.patch('dbt_meta.command_impl.context.ContextCommand.execute', new=fake_execute)
        result = self.runner.invoke(app, ['context', 'found', 'missing', '-j'])
        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        assert payload['missing'] is None
        assert payload['found']['name'] == 'found'

    def test_rich_output_renders(self, prod_manifest, mocker):
        mocker.patch(
            'dbt_meta.command_impl.context.ContextCommand.execute',
            return_value={
                'name': 'm', 'full_name': 'p.d.m', 'materialized': 'table',
                'description': 'desc', 'tags': ['t'], 'partition_by': 'dt',
                'cluster_by': ['c'], 'unique_key': ['k'], 'row_count': 5, 'bytes': 10,
                'columns': [{'name': 'c', 'data_type': 'STRING', 'description': ''}],
            },
        )
        result = self.runner.invoke(app, ['context', 'm'])
        assert result.exit_code == 0
        assert 'Context: m' in result.stdout
        assert 'p.d.m' in result.stdout

    def test_rich_output_missing_model(self, prod_manifest, mocker):
        mocker.patch('dbt_meta.command_impl.context.ContextCommand.execute', return_value=None)
        result = self.runner.invoke(app, ['context', 'ghost'])
        assert result.exit_code == 0
        assert 'Model not found' in result.stdout
