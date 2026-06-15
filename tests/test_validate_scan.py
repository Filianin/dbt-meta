"""Tests for validate and scan commands.

These commands use BigQuery dry run to:
- validate: Check SQL syntax without executing
- scan: Estimate query scan size
"""

import json
from unittest.mock import patch

import pytest

from tests.helpers_cmd import scan, validate


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

    def test_validate_dev_uses_dev_manifest_sql(self, tmp_path, monkeypatch):
        """--dev uses compiled SQL from dev manifest (dev schema refs), not prod."""
        prod_manifest = tmp_path / "prod_manifest.json"
        prod_manifest.write_text(json.dumps({
            "nodes": {
                "model.test.my_model": {
                    "name": "my_model",
                    "compiled_code": "SELECT * FROM core.demo_trade_stats",  # prod ref
                    "original_file_path": "models/my_model.sql",
                    "schema": "core",
                    "database": "project",
                    "config": {}
                }
            }
        }))

        # Dev manifest with compiled SQL referencing dev schema
        dev_dir = tmp_path / "project" / "target"
        dev_dir.mkdir(parents=True)
        dev_manifest = dev_dir / "manifest.json"
        dev_manifest.write_text(json.dumps({
            "nodes": {
                "model.test.my_model": {
                    "name": "my_model",
                    "compiled_code": "SELECT * FROM personal_user.demo_trade_stats",  # dev ref
                    "original_file_path": "models/my_model.sql",
                    "schema": "personal_user",
                    "database": "project",
                    "config": {}
                }
            }
        }))

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))
        monkeypatch.setenv('DBT_DEV_MANIFEST_PATH', str(dev_manifest))
        monkeypatch.setenv('DBT_FALLBACK_TARGET', 'true')
        monkeypatch.chdir(tmp_path / "project")

        captured_sql = []

        def capture_dry_run(sql):
            captured_sql.append(sql)
            return {'valid': True, 'bytes_processed': 0}

        with patch('dbt_meta.config.Config.find_config_file', return_value=None), \
             patch('dbt_meta.command_impl.validate.run_dry_run_query', side_effect=capture_dry_run):
            result = validate(str(prod_manifest), 'my_model', use_dev=True, json_output=False)

        assert result is not None
        assert result['valid'] is True
        # Must use dev SQL (personal_user), not prod SQL (core)
        assert len(captured_sql) == 1
        assert 'personal_user' in captured_sql[0]
        assert 'core' not in captured_sql[0]

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


class TestScanCommand:
    """Test scan command - query scan size estimation via BigQuery dry run."""

    def test_scan_with_valid_sql(self, tmp_path, monkeypatch):
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
             patch('dbt_meta.command_impl.scan.run_dry_run_query') as mock_dry_run:
            # 3.5 GB
            mock_dry_run.return_value = {'valid': True, 'bytes_processed': 3758096384}

            result = scan(str(manifest), 'my_model', use_dev=False, json_output=False)

        assert result is not None
        assert result['bytes'] == 3758096384
        assert result['formatted'] == '3.5 GB'
        assert result['error'] is None
        assert result['model'] == 'my_model'

    def test_scan_with_small_query(self, tmp_path, monkeypatch):
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
             patch('dbt_meta.command_impl.scan.run_dry_run_query') as mock_dry_run:
            # 150 MB
            mock_dry_run.return_value = {'valid': True, 'bytes_processed': 157286400}

            result = scan(str(manifest), 'small_model', use_dev=False, json_output=False)

        assert result is not None
        assert result['bytes'] == 157286400
        assert result['formatted'] == '150.0 MB'
        assert result['error'] is None

    def test_scan_with_invalid_sql(self, tmp_path, monkeypatch):
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
             patch('dbt_meta.command_impl.scan.run_dry_run_query') as mock_dry_run:
            mock_dry_run.return_value = {
                'valid': False,
                'error': 'Table not found'
            }

            result = scan(str(manifest), 'broken_model', use_dev=False, json_output=False)

        assert result is not None
        assert result['bytes'] is None
        assert result['formatted'] is None
        assert result['error'] == 'Table not found'

    def test_scan_without_compiled_sql(self, tmp_path, monkeypatch):
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
            result = scan(str(manifest), 'no_sql_model', use_dev=False, json_output=False)

        assert result is not None
        assert result['bytes'] is None
        assert result['formatted'] is None
        assert 'No compiled SQL' in result['error']

    def test_scan_model_not_found(self, tmp_path, monkeypatch, capsys):
        """Non-existent model returns None with error message."""
        manifest = tmp_path / "manifest.json"
        manifest.write_text(json.dumps({"nodes": {}}))

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(manifest))
        monkeypatch.setenv('DBT_FALLBACK_TARGET', 'false')
        monkeypatch.setenv('DBT_FALLBACK_BIGQUERY', 'false')

        with patch('dbt_meta.config.Config.find_config_file', return_value=None):
            result = scan(str(manifest), 'nonexistent', use_dev=False, json_output=False)

        assert result is None
        captured = capsys.readouterr()
        assert 'Cannot estimate scan' in captured.err

    def test_scan_with_zero_bytes(self, tmp_path, monkeypatch):
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
             patch('dbt_meta.command_impl.scan.run_dry_run_query') as mock_dry_run:
            mock_dry_run.return_value = {'valid': True, 'bytes_processed': 0}

            result = scan(str(manifest), 'cached_model', use_dev=False, json_output=False)

        assert result is not None
        assert result['bytes'] == 0
        # 0 is falsy, so formatted is None (see scan.py:72)
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


class TestRunDryRunQuery:
    """Test run_dry_run_query utility function directly."""

    def test_bq_not_found_fallback(self):
        """Test fallback when bq not in PATH."""
        from dbt_meta.utils.bigquery import run_dry_run_query

        with patch('dbt_meta.utils.bigquery.shutil.which', return_value=None), \
             patch('dbt_meta.utils.bigquery.os.path.isfile', return_value=False), \
             patch('dbt_meta.utils.bigquery.os.access', return_value=False):
            result = run_dry_run_query("SELECT 1")

        assert result['valid'] is False
        assert 'not found' in result['error']

    @patch('shutil.which')
    @patch('subprocess.run')
    def test_valid_query_with_bytes(self, mock_run, mock_which):
        """Test valid query returns bytes processed."""
        from dbt_meta.utils.bigquery import run_dry_run_query

        mock_which.return_value = '/usr/bin/bq'
        mock_run.return_value = type('Result', (), {
            'stdout': 'Query successfully validated. Assuming the tables are not modified, running this query will process upper bound of 12345678 bytes of data.',
            'stderr': '',
            'returncode': 0
        })()

        result = run_dry_run_query("SELECT * FROM table")

        assert result['valid'] is True
        assert result['bytes_processed'] == 12345678
        assert result['error'] is None

    @patch('shutil.which')
    @patch('subprocess.run')
    def test_invalid_query(self, mock_run, mock_which):
        """Test invalid query returns error."""
        from dbt_meta.utils.bigquery import run_dry_run_query

        mock_which.return_value = '/usr/bin/bq'
        mock_run.return_value = type('Result', (), {
            'stdout': '',
            'stderr': 'Error in query string: Syntax error at position 10',
            'returncode': 1
        })()

        result = run_dry_run_query("SELECT * FORM table")

        assert result['valid'] is False
        assert 'Syntax error' in result['error']

    @patch('shutil.which')
    @patch('subprocess.run')
    def test_timeout(self, mock_run, mock_which):
        """Test timeout handling."""
        import subprocess
        from dbt_meta.utils.bigquery import run_dry_run_query

        mock_which.return_value = '/usr/bin/bq'
        mock_run.side_effect = subprocess.TimeoutExpired(cmd='bq', timeout=30)

        result = run_dry_run_query("SELECT 1", timeout=30)

        assert result['valid'] is False
        assert 'timed out' in result['error']

    @patch('shutil.which')
    @patch('subprocess.run')
    def test_valid_query_no_bytes_match(self, mock_run, mock_which):
        """Test valid query without bytes in output."""
        from dbt_meta.utils.bigquery import run_dry_run_query

        mock_which.return_value = '/usr/bin/bq'
        mock_run.return_value = type('Result', (), {
            'stdout': 'Query successfully validated.',
            'stderr': '',
            'returncode': 0
        })()

        result = run_dry_run_query("SELECT 1")

        assert result['valid'] is True
        assert result['bytes_processed'] is None

    @patch('shutil.which')
    @patch('subprocess.run')
    def test_error_in_stdout(self, mock_run, mock_which):
        """Test error message in stdout is captured."""
        from dbt_meta.utils.bigquery import run_dry_run_query

        mock_which.return_value = '/usr/bin/bq'
        mock_run.return_value = type('Result', (), {
            'stdout': 'Error in query string: Table not found',
            'stderr': '',
            'returncode': 1
        })()

        result = run_dry_run_query("SELECT * FROM missing")

        assert result['valid'] is False
        assert 'Table not found' in result['error']


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
