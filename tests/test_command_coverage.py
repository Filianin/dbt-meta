"""Additional tests to improve coverage for command_impl modules.

Focus on uncovered code paths in:
- info.py: use_dev mode and BigQuery fallback
- config.py: BigQuery fallback
- path.py: BigQuery format search
- deps.py: edge cases
"""

import json
from unittest.mock import patch

import pytest

from dbt_meta.commands import config, deps, info, path


class TestInfoCommandCoverage:
    """Tests for info.py uncovered lines (73-76, 113-123)."""

    def test_info_with_dev_flag_uses_dev_schema(self, tmp_path, monkeypatch, enable_fallbacks):
        """Test info command with --dev flag uses dev schema."""
        # Setup manifests
        prod_manifest = tmp_path / ".dbt-state" / "manifest.json"
        dev_manifest = tmp_path / "target" / "manifest.json"

        prod_manifest.parent.mkdir(parents=True)
        dev_manifest.parent.mkdir(parents=True)

        # Production manifest (empty for this test)
        prod_manifest.write_text(json.dumps({
            "metadata": {},
            "nodes": {}
        }))

        # Dev manifest with test model
        dev_manifest.write_text(json.dumps({
            "metadata": {},
            "nodes": {
                "model.test_project.test_model": {
                    "name": "test_model",
                    "alias": "test_model_alias",
                    "schema": "personal_testuser",
                    "database": "test-project",
                    "original_file_path": "models/test/test_model.sql",
                    "unique_id": "model.test_project.test_model",
                    "tags": ["daily"],
                    "config": {
                        "materialized": "table",
                        "alias": "test_model_alias"
                    }
                }
            }
        }))

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))
        monkeypatch.setenv('DBT_DEV_MANIFEST_PATH', str(dev_manifest))
        monkeypatch.setenv('USER', 'testuser')
        monkeypatch.setenv('DBT_DEV_SCHEMA_PREFIX', 'personal')

        # Test with use_dev=True
        result = info(str(prod_manifest), 'test_model', use_dev=True, json_output=False)

        assert result is not None
        assert result['name'] == 'test_model'
        assert result['schema'] == 'personal_testuser'
        assert result['database'] == ''  # Dev mode doesn't use database
        assert result['table'] == 'test_model'  # Uses name, not alias in dev
        assert result['full_name'] == 'personal_testuser.test_model'
        assert result['materialized'] == 'table'


    def test_info_bigquery_fallback_dev_mode(self, enable_fallbacks, tmp_path, monkeypatch, mocker):
        """Test info command with BigQuery fallback when model not in dev manifest."""
        # Setup empty manifests
        prod_manifest = tmp_path / ".dbt-state" / "manifest.json"
        dev_manifest = tmp_path / "target" / "manifest.json"

        prod_manifest.parent.mkdir(parents=True)
        dev_manifest.parent.mkdir(parents=True)

        prod_manifest.write_text(json.dumps({"metadata": {}, "nodes": {}}))
        dev_manifest.write_text(json.dumps({"metadata": {}, "nodes": {}}))

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))
        monkeypatch.setenv('DBT_DEV_MANIFEST_PATH', str(dev_manifest))
        monkeypatch.setenv('USER', 'testuser')
        monkeypatch.setenv('DBT_DEV_SCHEMA_PREFIX', 'personal')

        # Mock BigQuery response
        mock_bq_metadata = {
            'type': 'TABLE',
            'tableReference': {
                'projectId': 'test-project',
                'datasetId': 'personal_testuser',
                'tableId': 'my_table'
            }
        }

        with patch('dbt_meta.command_impl.info._fetch_table_metadata_from_bigquery') as mock_fetch:
            mock_fetch.return_value = mock_bq_metadata

            # Mock git check
            mock_git = mocker.patch('dbt_meta.command_impl.base._check_manifest_git_mismatch')
            mock_git.return_value = []

            # CRITICAL: use_dev=True to trigger BigQuery fallback in dev mode
            result = info(str(prod_manifest), 'my_table', use_dev=True, json_output=False)

        assert result is not None
        assert result['schema'] == 'personal_testuser'
        assert result['table'] == 'my_table'
        assert result['full_name'] == 'personal_testuser.my_table'
        assert result['materialized'] == 'table'


class TestConfigCommandCoverage:
    """Tests for config.py uncovered lines (71-117)."""

    def test_config_bigquery_fallback_dev_mode(self, enable_fallbacks, tmp_path, monkeypatch, mocker):
        """Test config command with BigQuery fallback in dev mode."""
        # Setup manifests (model not in manifests)
        prod_manifest = tmp_path / ".dbt-state" / "manifest.json"
        dev_manifest = tmp_path / "target" / "manifest.json"

        prod_manifest.parent.mkdir(parents=True)
        dev_manifest.parent.mkdir(parents=True)

        prod_manifest.write_text(json.dumps({"metadata": {}, "nodes": {}}))
        dev_manifest.write_text(json.dumps({"metadata": {}, "nodes": {}}))

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))
        monkeypatch.setenv('DBT_DEV_MANIFEST_PATH', str(dev_manifest))
        monkeypatch.setenv('USER', 'testuser')
        monkeypatch.setenv('DBT_DEV_SCHEMA_PREFIX', 'personal')

        # Mock BigQuery response with partition and clustering
        mock_bq_metadata = {
            'type': 'TABLE',
            'tableReference': {
                'projectId': 'test-project',
                'datasetId': 'core_events',
                'tableId': 'events'
            },
            'timePartitioning': {
                'field': 'event_date'
            },
            'clustering': {
                'fields': ['user_id', 'event_type']
            }
        }

        with patch('dbt_meta.command_impl.config._fetch_table_metadata_from_bigquery') as mock_fetch:
            mock_fetch.return_value = mock_bq_metadata

            # Mock git check
            mock_git = mocker.patch('dbt_meta.command_impl.base._check_manifest_git_mismatch')
            mock_git.return_value = []

            # CRITICAL: use_dev=True to trigger _get_model_bigquery_dev() path
            result = config(str(prod_manifest), 'events', use_dev=True, json_output=False)

        assert result is not None
        assert 'materialized' in result
        assert result['materialized'] == 'table'
        assert result['partition_by'] == 'event_date'
        assert result['cluster_by'] == ['user_id', 'event_type']


class TestPathCommandCoverage:
    """Tests for path.py uncovered lines (45, 75-130, 145-176)."""

    def test_path_with_bigquery_format_dev_mode(self, enable_fallbacks, tmp_path, monkeypatch):
        """Test path command with BigQuery format (schema.table) in dev mode."""
        # Setup dev manifest
        dev_manifest = tmp_path / "target" / "manifest.json"
        dev_manifest.parent.mkdir(parents=True)

        dev_manifest.write_text(json.dumps({
            "metadata": {},
            "nodes": {
                "model.test_project.personal_testuser__my_model": {
                    "name": "my_model",
                    "schema": "personal_testuser",
                    "original_file_path": "models/personal/my_model.sql",
                    "resource_type": "model",
                    "config": {
                        "schema": "personal_testuser"
                    }
                }
            }
        }))

        monkeypatch.setenv('DBT_DEV_MANIFEST_PATH', str(dev_manifest))
        monkeypatch.setenv('DBT_DEV_TABLE_PATTERN', 'name')

        # Query with BigQuery format: schema.table
        result = path(str(dev_manifest), 'personal_testuser.my_model', use_dev=True, json_output=False)

        assert result is not None
        assert 'my_model.sql' in result


class TestDepsCommandCoverage:
    """Tests for deps.py uncovered lines (50-54, 70)."""

    def test_deps_returns_empty_for_model_without_deps(self, prod_manifest):
        """Test deps command for model without dependencies."""
        from dbt_meta.manifest.parser import ManifestParser

        parser = ManifestParser(str(prod_manifest))
        nodes = parser.manifest.get('nodes', {})

        # Find any model
        test_model = None
        for node_id, node_data in nodes.items():
            if node_data.get('resource_type') == 'model':
                test_model = node_id.split('.')[-1]
                break

        if not test_model:
            pytest.skip("No models found in manifest")

        result = deps(str(prod_manifest), test_model, use_dev=False, json_output=False)

        # Should have dependency structure (refs and sources only)
        assert result is not None
        assert 'refs' in result
        assert 'sources' in result
        # Lists might be empty but structure should exist
        assert isinstance(result['refs'], list)
        assert isinstance(result['sources'], list)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
