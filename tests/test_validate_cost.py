"""Tests for validate and cost commands.

These commands use BigQuery dry run to:
- validate: Check SQL syntax without executing
- cost: Estimate query scan size
"""

import json
from unittest.mock import patch

import pytest

from dbt_meta.commands import cost, validate


class TestValidateCommand:
    """Test validate command - SQL syntax validation via BigQuery dry run."""

    def test_validate_with_valid_sql(self, tmp_path, monkeypatch):
        """Model with valid SQL returns valid=True."""
        manifest = tmp_path / "manifest.json"
        manifest.write_text(json.dumps({
            "nodes": {
                "model.test.my_model": {
                    "name": "my_model",
                    "compiled_code": "SELECT 1 as id",
                    "original_file_path": "models/my_model.sql",
                    "schema": "test",
                    "database": "project",
                    "config": {}
                }
            }
        }))

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(manifest))

        with patch('dbt_meta.config.Config.find_config_file', return_value=None), \
             patch('dbt_meta.command_impl.validate.run_dry_run_query') as mock_dry_run:
            mock_dry_run.return_value = {'valid': True, 'bytes_processed': 1000}

            result = validate(str(manifest), 'my_model', use_dev=False, json_output=False)

        assert result is not None
        assert result['valid'] is True
        assert result['error'] is None
        assert result['model'] == 'my_model'

    def test_validate_with_invalid_sql(self, tmp_path, monkeypatch):
        """Model with invalid SQL returns error message."""
        manifest = tmp_path / "manifest.json"
        manifest.write_text(json.dumps({
            "nodes": {
                "model.test.broken_model": {
                    "name": "broken_model",
                    "compiled_code": "SELECT * FROM nonexistent_table",
                    "original_file_path": "models/broken_model.sql",
                    "schema": "test",
                    "database": "project",
                    "config": {}
                }
            }
        }))

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(manifest))

        with patch('dbt_meta.config.Config.find_config_file', return_value=None), \
             patch('dbt_meta.command_impl.validate.run_dry_run_query') as mock_dry_run:
            mock_dry_run.return_value = {
                'valid': False,
                'error': 'Table not found: nonexistent_table'
            }

            result = validate(str(manifest), 'broken_model', use_dev=False, json_output=False)

        assert result is not None
        assert result['valid'] is False
        assert result['error'] == 'Table not found: nonexistent_table'
        assert result['model'] == 'broken_model'

    def test_validate_without_compiled_sql(self, tmp_path, monkeypatch):
        """Model without compiled_code returns error."""
        manifest = tmp_path / "manifest.json"
        manifest.write_text(json.dumps({
            "nodes": {
                "model.test.no_sql_model": {
                    "name": "no_sql_model",
                    "original_file_path": "models/no_sql_model.sql",
                    "schema": "test",
                    "database": "project",
                    "config": {}
                    # No compiled_code field
                }
            }
        }))

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(manifest))

        with patch('dbt_meta.config.Config.find_config_file', return_value=None):
            result = validate(str(manifest), 'no_sql_model', use_dev=False, json_output=False)

        assert result is not None
        assert result['valid'] is False
        assert 'No compiled SQL' in result['error']

    def test_validate_model_not_found(self, tmp_path, monkeypatch, capsys):
        """Non-existent model returns None with error message."""
        manifest = tmp_path / "manifest.json"
        manifest.write_text(json.dumps({"nodes": {}}))

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(manifest))
        monkeypatch.setenv('DBT_FALLBACK_TARGET', 'false')
        monkeypatch.setenv('DBT_FALLBACK_BIGQUERY', 'false')

        with patch('dbt_meta.config.Config.find_config_file', return_value=None):
            result = validate(str(manifest), 'nonexistent', use_dev=False, json_output=False)

        assert result is None
        captured = capsys.readouterr()
        assert 'Cannot validate' in captured.err


class TestCostCommand:
    """Test cost command - query scan size estimation via BigQuery dry run."""

    def test_cost_with_valid_sql(self, tmp_path, monkeypatch):
        """Model with valid SQL returns bytes and formatted size."""
        manifest = tmp_path / "manifest.json"
        manifest.write_text(json.dumps({
            "nodes": {
                "model.test.my_model": {
                    "name": "my_model",
                    "compiled_code": "SELECT * FROM big_table",
                    "original_file_path": "models/my_model.sql",
                    "schema": "test",
                    "database": "project",
                    "config": {}
                }
            }
        }))

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(manifest))

        with patch('dbt_meta.config.Config.find_config_file', return_value=None), \
             patch('dbt_meta.command_impl.cost.run_dry_run_query') as mock_dry_run:
            # 3.5 GB
            mock_dry_run.return_value = {'valid': True, 'bytes_processed': 3758096384}

            result = cost(str(manifest), 'my_model', use_dev=False, json_output=False)

        assert result is not None
        assert result['bytes'] == 3758096384
        assert result['formatted'] == '3.5 GB'
        assert result['error'] is None
        assert result['model'] == 'my_model'

    def test_cost_with_small_query(self, tmp_path, monkeypatch):
        """Small query returns size in MB."""
        manifest = tmp_path / "manifest.json"
        manifest.write_text(json.dumps({
            "nodes": {
                "model.test.small_model": {
                    "name": "small_model",
                    "compiled_code": "SELECT 1",
                    "original_file_path": "models/small_model.sql",
                    "schema": "test",
                    "database": "project",
                    "config": {}
                }
            }
        }))

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(manifest))

        with patch('dbt_meta.config.Config.find_config_file', return_value=None), \
             patch('dbt_meta.command_impl.cost.run_dry_run_query') as mock_dry_run:
            # 150 MB
            mock_dry_run.return_value = {'valid': True, 'bytes_processed': 157286400}

            result = cost(str(manifest), 'small_model', use_dev=False, json_output=False)

        assert result is not None
        assert result['bytes'] == 157286400
        assert result['formatted'] == '150.0 MB'
        assert result['error'] is None

    def test_cost_with_invalid_sql(self, tmp_path, monkeypatch):
        """Model with invalid SQL returns error."""
        manifest = tmp_path / "manifest.json"
        manifest.write_text(json.dumps({
            "nodes": {
                "model.test.broken_model": {
                    "name": "broken_model",
                    "compiled_code": "SELECT * FROM nonexistent",
                    "original_file_path": "models/broken_model.sql",
                    "schema": "test",
                    "database": "project",
                    "config": {}
                }
            }
        }))

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(manifest))

        with patch('dbt_meta.config.Config.find_config_file', return_value=None), \
             patch('dbt_meta.command_impl.cost.run_dry_run_query') as mock_dry_run:
            mock_dry_run.return_value = {
                'valid': False,
                'error': 'Table not found'
            }

            result = cost(str(manifest), 'broken_model', use_dev=False, json_output=False)

        assert result is not None
        assert result['bytes'] is None
        assert result['formatted'] is None
        assert result['error'] == 'Table not found'

    def test_cost_without_compiled_sql(self, tmp_path, monkeypatch):
        """Model without compiled_code returns error."""
        manifest = tmp_path / "manifest.json"
        manifest.write_text(json.dumps({
            "nodes": {
                "model.test.no_sql_model": {
                    "name": "no_sql_model",
                    "original_file_path": "models/no_sql_model.sql",
                    "schema": "test",
                    "database": "project",
                    "config": {}
                }
            }
        }))

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(manifest))

        with patch('dbt_meta.config.Config.find_config_file', return_value=None):
            result = cost(str(manifest), 'no_sql_model', use_dev=False, json_output=False)

        assert result is not None
        assert result['bytes'] is None
        assert result['formatted'] is None
        assert 'No compiled SQL' in result['error']

    def test_cost_model_not_found(self, tmp_path, monkeypatch, capsys):
        """Non-existent model returns None with error message."""
        manifest = tmp_path / "manifest.json"
        manifest.write_text(json.dumps({"nodes": {}}))

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(manifest))
        monkeypatch.setenv('DBT_FALLBACK_TARGET', 'false')
        monkeypatch.setenv('DBT_FALLBACK_BIGQUERY', 'false')

        with patch('dbt_meta.config.Config.find_config_file', return_value=None):
            result = cost(str(manifest), 'nonexistent', use_dev=False, json_output=False)

        assert result is None
        captured = capsys.readouterr()
        assert 'Cannot estimate cost' in captured.err

    def test_cost_with_zero_bytes(self, tmp_path, monkeypatch):
        """Query returning 0 bytes (cached or metadata query)."""
        manifest = tmp_path / "manifest.json"
        manifest.write_text(json.dumps({
            "nodes": {
                "model.test.cached_model": {
                    "name": "cached_model",
                    "compiled_code": "SELECT 1",
                    "original_file_path": "models/cached_model.sql",
                    "schema": "test",
                    "database": "project",
                    "config": {}
                }
            }
        }))

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(manifest))

        with patch('dbt_meta.config.Config.find_config_file', return_value=None), \
             patch('dbt_meta.command_impl.cost.run_dry_run_query') as mock_dry_run:
            mock_dry_run.return_value = {'valid': True, 'bytes_processed': 0}

            result = cost(str(manifest), 'cached_model', use_dev=False, json_output=False)

        assert result is not None
        assert result['bytes'] == 0
        # 0 is falsy, so formatted is None (see cost.py:72)
        assert result['formatted'] is None
        assert result['error'] is None


class TestFormatBytes:
    """Test format_bytes utility function."""

    def test_format_bytes_mb(self):
        """Values under 1000 MB shown in MB."""
        from dbt_meta.utils.bigquery import format_bytes

        assert format_bytes(1048576) == '1.0 MB'  # 1 MB
        assert format_bytes(524288000) == '500.0 MB'  # 500 MB
        assert format_bytes(1047527424) == '999.0 MB'  # ~999 MB

    def test_format_bytes_gb(self):
        """Values 1000 MB and above shown in GB."""
        from dbt_meta.utils.bigquery import format_bytes

        # GB threshold is 1000 MB
        # 1073741824 bytes = 1024 MB >= 1000, so shown as GB
        assert format_bytes(1073741824) == '1.0 GB'  # 1024 MB = 1.0 GB
        assert format_bytes(1073741824000) == '1000.0 GB'  # 1000 GB
        assert format_bytes(5368709120000) == '5000.0 GB'  # 5000 GB

    def test_format_bytes_zero(self):
        """Zero bytes formatted correctly."""
        from dbt_meta.utils.bigquery import format_bytes

        assert format_bytes(0) == '0.0 MB'

    def test_format_bytes_large_mb(self):
        """Large MB values just under GB threshold."""
        from dbt_meta.utils.bigquery import format_bytes

        # 999.9 MB should still be in MB
        assert format_bytes(1048471142) == '999.9 MB'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
