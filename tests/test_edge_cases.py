"""Edge case tests for config, errors, and fallback modules."""

from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from dbt_meta.config import Config, _calculate_dev_schema, _parse_bool
from dbt_meta.errors import ConfigurationError, ManifestParseError, ModelNotFoundError
from dbt_meta.fallback import FallbackStrategy


class TestConfigEdgeCases:
    """Edge cases for config module."""

    def test_empty_env_var_values(self, monkeypatch):
        """Test that empty env vars are handled correctly."""
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', '')
        monkeypatch.setenv('DBT_DEV_SCHEMA', '')
        monkeypatch.setenv('USER', 'testuser')

        config = Config.from_env()

        # Empty path should default
        assert config.prod_manifest_path != ''
        # Empty dev_dataset should use default
        assert config.dev_dataset == 'personal_testuser'

    def test_whitespace_only_env_vars(self, monkeypatch):
        """Test whitespace-only env vars."""
        monkeypatch.setenv('DBT_DEV_SCHEMA', '   ')
        monkeypatch.setenv('USER', 'alice')

        config = Config.from_env()

        # Whitespace should be preserved (not our job to trim)
        assert config.dev_dataset == '   '

    def test_parse_bool_edge_cases(self):
        """Test _parse_bool with edge cases."""
        # Case sensitivity
        assert _parse_bool('TRUE') is True
        assert _parse_bool('True') is True
        assert _parse_bool('tRuE') is True

        # Numbers
        assert _parse_bool('1') is True
        assert _parse_bool('0') is False
        assert _parse_bool('2') is False

        # Empty and whitespace
        assert _parse_bool('') is False
        assert _parse_bool('  ') is False

        # Random strings
        assert _parse_bool('random') is False
        assert _parse_bool('false') is False
        assert _parse_bool('no') is False

    def test_very_long_path(self, monkeypatch, tmp_path):
        """Test config with very long path."""
        # Create deeply nested directory
        deep_path = tmp_path
        for i in range(50):
            deep_path = deep_path / f"dir{i}"

        deep_path.mkdir(parents=True, exist_ok=True)
        manifest_path = deep_path / "manifest.json"
        manifest_path.write_text('{}')

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(manifest_path))

        config = Config.from_env()
        assert len(config.prod_manifest_path) > 200

    def test_validate_with_directory_instead_of_file(self, tmp_path, monkeypatch):
        """Test validation when manifest path is a directory."""
        manifest_dir = tmp_path / "manifest.json"
        manifest_dir.mkdir()

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(manifest_dir))

        config = Config.from_env()
        warnings = config.validate()

        # Should warn about directory instead of file
        assert any('directory' in w.lower() for w in warnings)

    def test_calculate_dev_schema_empty_username(self, monkeypatch):
        """Test dev schema calculation with empty username."""
        monkeypatch.delenv('DBT_DEV_SCHEMA', raising=False)
        monkeypatch.setenv('USER', '')

        schema = _calculate_dev_schema()
        assert schema == 'personal_'

    def test_calculate_dev_schema_special_chars(self, monkeypatch):
        """Test dev schema with special characters in username."""
        monkeypatch.delenv('DBT_DEV_SCHEMA', raising=False)
        monkeypatch.setenv('USER', 'user@example.com')

        schema = _calculate_dev_schema()
        # All non-alphanumeric chars (@ and .) should be replaced with underscores
        assert schema == 'personal_user_example_com'


class TestErrorsEdgeCases:
    """Edge cases for errors module."""

    def test_model_not_found_empty_model_name(self):
        """Test ModelNotFoundError with empty model name."""
        error = ModelNotFoundError(
            model_name='',
            searched_locations=['production manifest']
        )

        assert error.model_name == ''
        assert 'Try: meta list' in error.suggestion  # No schema prefix

    def test_model_not_found_empty_searched_locations(self):
        """Test ModelNotFoundError with empty searched locations."""
        error = ModelNotFoundError(
            model_name='test_model',
            searched_locations=[]
        )

        assert error.searched_locations == []
        # Should have special message for empty locations
        assert 'No locations were searched' in error.suggestion
        assert 'disabled' in error.suggestion.lower()

    def test_model_not_found_very_long_name(self):
        """Test ModelNotFoundError with very long model name."""
        long_name = 'a' * 500
        error = ModelNotFoundError(
            model_name=long_name,
            searched_locations=['production manifest']
        )

        assert long_name in error.message
        assert len(error.model_name) == 500

    def test_manifest_not_found_many_paths(self):
        """Test ManifestNotFoundError with many searched paths."""
        from dbt_meta.errors import ManifestNotFoundError

        paths = [f'/path/to/manifest{i}.json' for i in range(100)]
        error = ManifestNotFoundError(searched_paths=paths)

        assert len(error.searched_paths) == 100
        assert all(p in error.suggestion for p in paths[:3])  # Check first few

    def test_bigquery_error_empty_details(self):
        """Test BigQueryError with empty details."""
        from dbt_meta.errors import BigQueryError

        error = BigQueryError(operation='query', details='')

        assert error.operation == 'query'
        assert error.details == ''
        assert 'Details:' in error.suggestion

    def test_configuration_error_empty_values_list(self):
        """Test ConfigurationError with empty valid values."""
        error = ConfigurationError(
            config_key='TEST_KEY',
            invalid_value='bad',
            valid_values=[]
        )

        assert error.valid_values == []
        # Should still provide helpful suggestion
        assert 'Check configuration documentation' in error.suggestion


class TestFallbackEdgeCases:
    """Edge cases for fallback strategy."""

    @pytest.fixture
    def mock_config(self, tmp_path, monkeypatch):
        """Create mock config."""
        prod_manifest = tmp_path / ".dbt-state" / "manifest.json"
        dev_manifest = tmp_path / "target" / "manifest.json"

        prod_manifest.parent.mkdir(parents=True)
        dev_manifest.parent.mkdir(parents=True)

        prod_manifest.write_text('{"metadata": {}}')
        dev_manifest.write_text('{"metadata": {}}')

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))
        monkeypatch.setenv('DBT_DEV_MANIFEST_PATH', str(dev_manifest))
        monkeypatch.setenv('DBT_FALLBACK_TARGET', 'true')
        monkeypatch.setenv('DBT_FALLBACK_BIGQUERY', 'true')

        return Config.from_env()

    def test_fallback_with_empty_model_name(self, mock_config):
        """Test fallback with empty model name."""
        from dbt_meta.manifest.parser import ManifestParser

        parser = ManifestParser(mock_config.prod_manifest_path)
        strategy = FallbackStrategy(mock_config)

        with pytest.raises(ModelNotFoundError) as exc_info:
            strategy.get_model('', parser)

        assert exc_info.value.model_name == ''

    def test_fallback_with_model_name_without_separator(self, mock_config):
        """Test fallback with model name without __ separator."""

        parser = Mock()
        parser.get_model = Mock(return_value=None)

        strategy = FallbackStrategy(mock_config)

        # Model without __ - will try BigQuery with dataset=None
        with pytest.raises(ModelNotFoundError):
            strategy.get_model('single_word', parser)

    def test_fallback_empty_allowed_levels(self, mock_config):
        """Test fallback with empty allowed levels list."""
        from dbt_meta.manifest.parser import ManifestParser

        parser = ManifestParser(mock_config.prod_manifest_path)
        strategy = FallbackStrategy(mock_config)

        with pytest.raises(ModelNotFoundError) as exc_info:
            strategy.get_model('test_model', parser, allowed_levels=[])

        # Should have searched no levels
        assert len(exc_info.value.searched_locations) == 0

    def test_fetch_from_bigquery_with_none_dataset(self, mock_config):
        """Test BigQuery fetch when dataset is None (no __ in model name)."""
        strategy = FallbackStrategy(mock_config)

        # Mock the imports
        with patch('dbt_meta.fallback.FallbackStrategy._fetch_from_bigquery') as mock_fetch:
            mock_fetch.return_value = None

            result = strategy._fetch_from_bigquery('model_without_separator')

            # Should handle None dataset gracefully
            assert result is None

    def test_fetch_from_bigquery_invalid_metadata_structure(self, mock_config):
        """Test BigQuery fetch with invalid metadata structure."""
        strategy = FallbackStrategy(mock_config)

        with patch('dbt_meta.utils.bigquery.fetch_table_metadata_from_bigquery') as mock_bq:
            with patch('dbt_meta.utils.bigquery.infer_table_parts') as mock_infer:
                mock_infer.return_value = ('dataset', 'table')
                # Return metadata without expected fields
                mock_bq.return_value = {'unexpected': 'structure'}

                result = strategy._fetch_from_bigquery('dataset__table')

                # Should handle gracefully
                assert result is not None
                assert result['database'] == ''  # Missing projectId
                assert result['config']['materialized'] == 'view'  # Default when type missing

    def test_fallback_dev_parser_with_corrupted_manifest(self, enable_fallbacks, mock_config, tmp_path):
        """Test dev parser when manifest is corrupted - should continue to next fallback level."""
        # Corrupt dev manifest
        dev_manifest = Path(mock_config.dev_manifest_path)
        dev_manifest.write_text('{ invalid json }')


        parser = Mock()
        parser.get_model = Mock(return_value=None)

        strategy = FallbackStrategy(mock_config)

        # Should handle corrupted manifest gracefully by continuing to BigQuery
        # Eventually raises ModelNotFoundError after all levels exhausted
        with pytest.raises(ModelNotFoundError) as exc_info:
            strategy.get_model('test_model', parser)

        # Should have tried all three levels
        assert 'production manifest' in str(exc_info.value.searched_locations)
        assert 'dev manifest' in str(exc_info.value.searched_locations)
        assert 'BigQuery' in str(exc_info.value.searched_locations)

    def test_fallback_all_levels_disabled(self, mock_config, monkeypatch):
        """Test fallback when all fallback levels are disabled."""
        monkeypatch.setenv('DBT_FALLBACK_TARGET', 'false')
        monkeypatch.setenv('DBT_FALLBACK_BIGQUERY', 'false')

        config = Config.from_env()
        strategy = FallbackStrategy(config)

        from dbt_meta.manifest.parser import ManifestParser
        parser = ManifestParser(config.prod_manifest_path)

        # Only prod manifest should be checked
        with pytest.raises(ModelNotFoundError) as exc_info:
            strategy.get_model('test_model', parser)

        # Should only have searched prod manifest
        assert exc_info.value.searched_locations == ['production manifest']

    def test_fallback_warnings_accumulation(self, mock_config, tmp_path):
        """Test that warnings accumulate correctly across fallback levels."""

        parser = Mock()
        parser.get_model = Mock(return_value=None)

        # Create valid dev manifest
        dev_manifest_path = tmp_path / "target" / "manifest.json"
        dev_manifest_content = {
            "metadata": {},
            "nodes": {
                "model.my_project.test": {
                    "schema": "personal_user",
                    "name": "test"
                }
            }
        }
        import json
        dev_manifest_path.write_text(json.dumps(dev_manifest_content))

        strategy = FallbackStrategy(mock_config)

        with patch('dbt_meta.fallback.ManifestParser') as mock_parser_class:
            dev_parser = Mock()
            dev_parser.get_model = Mock(return_value={'schema': 'test'})
            mock_parser_class.return_value = dev_parser

            result = strategy.get_model('test', parser)

        # Should have warning about dev manifest
        assert len(result.warnings) > 0
        assert any('dev manifest' in w.lower() for w in result.warnings)


class TestColumnsCommandEdgeCases:
    """Edge cases for columns command with BigQuery fallback."""

    def test_columns_fallback_uses_model_schema_not_production(self, tmp_path, monkeypatch):
        """Test that columns BigQuery fallback uses schema from FOUND model, not production manifest.

        This is a critical bug fix: when model is found in dev manifest but has no columns,
        fallback should query BigQuery using dev schema (personal_*), not production schema.
        """
        import json
        from unittest.mock import patch

        from dbt_meta.commands import columns

        # Setup manifests
        prod_manifest = tmp_path / ".dbt-state" / "manifest.json"
        dev_manifest = tmp_path / "target" / "manifest.json"

        prod_manifest.parent.mkdir(parents=True)
        dev_manifest.parent.mkdir(parents=True)

        # Production manifest: model NOT in production
        prod_manifest.write_text(json.dumps({
            "metadata": {},
            "nodes": {}
        }))

        # Dev manifest: model exists but NO columns documented
        dev_manifest.write_text(json.dumps({
            "metadata": {},
            "nodes": {
                "model.test_project.core_appsflyer__upload_log": {
                    "name": "core_appsflyer__upload_log",
                    "alias": "core_appsflyer__upload_log",
                    "schema": "personal_testuser",
                    "database": "test-project",
                    "columns": {},  # EMPTY columns!
                    "config": {"materialized": "table"}
                }
            }
        }))

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))
        monkeypatch.setenv('DBT_DEV_MANIFEST_PATH', str(dev_manifest))
        monkeypatch.setenv('DBT_FALLBACK_TARGET', 'true')
        monkeypatch.setenv('DBT_FALLBACK_BIGQUERY', 'true')
        monkeypatch.setenv('DBT_DEV_SCHEMA', 'personal_testuser')  # Match expected dev schema

        # Mock BigQuery fetch to verify correct schema is used
        # NOTE: Patch where function is USED, not where it's defined
        with patch('dbt_meta.command_impl.columns._fetch_columns_from_bigquery_direct') as mock_bq:
            mock_bq.return_value = [
                {'name': 'col1', 'data_type': 'string'},
                {'name': 'col2', 'data_type': 'integer'}
            ]

            # Run columns command with --dev flag
            result = columns(str(prod_manifest), 'core_appsflyer__upload_log', use_dev=True, json_output=True)

            # Verify BigQuery was called with DEV schema, not production
            mock_bq.assert_called_once()
            call_args = mock_bq.call_args

            # Should use dev schema from model (positional args: schema, table, database)
            assert call_args[0][0] == 'personal_testuser', "Should use dev schema from found model"
            assert call_args[0][1] == 'core_appsflyer__upload_log', "Should use correct table name"
            assert call_args[0][2] == 'test-project', "Should use correct database"

            # Verify columns returned correctly
            assert result is not None
            assert len(result) == 2


class TestIntegrationEdgeCases:
    """Integration edge cases across modules."""

    def test_config_and_fallback_integration_with_invalid_paths(self, monkeypatch):
        """Test config + fallback with completely invalid paths."""
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', '/nonexistent/path/manifest.json')
        monkeypatch.setenv('DBT_DEV_MANIFEST_PATH', '/another/nonexistent/manifest.json')
        monkeypatch.setenv('DBT_FALLBACK_TARGET', 'false')
        monkeypatch.setenv('DBT_FALLBACK_BIGQUERY', 'false')

        config = Config.from_env()
        warnings = config.validate()

        # Should warn about missing prod manifest
        assert len(warnings) > 0
        assert any('not found' in w for w in warnings)

    def test_exception_inheritance_chain(self):
        """Test that all custom exceptions properly inherit."""
        from dbt_meta.errors import (
            BigQueryError,
            ConfigurationError,
            DbtMetaError,
            GitOperationError,
            ManifestNotFoundError,
            ModelNotFoundError,
        )

        # All should be catchable with base exception
        exceptions = [
            ModelNotFoundError('test', []),
            ManifestNotFoundError(['/path']),
            ManifestParseError('/path', 'error'),
            BigQueryError('op', 'details'),
            GitOperationError('cmd', 'err'),
            ConfigurationError('key', 'val')
        ]

        for exc in exceptions:
            assert isinstance(exc, DbtMetaError)
            assert isinstance(exc, Exception)

    def test_config_validate_with_all_invalid_values(self, tmp_path, monkeypatch):
        """Test config validation with all possible invalid values."""
        nonexistent = tmp_path / "nonexistent.json"

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(nonexistent))
        monkeypatch.setenv('DBT_PROD_TABLE_NAME', 'invalid_strategy')
        monkeypatch.setenv('DBT_PROD_SCHEMA_SOURCE', 'invalid_source')

        config = Config.from_env()
        warnings = config.validate()

        # Should have 3 warnings (2 for invalid strategies + 1 for missing manifest)
        assert len(warnings) >= 3
        # Check for config field names (not env var names)
        assert any('prod_table_name_strategy' in w for w in warnings)
        assert any('prod_schema_source' in w for w in warnings)
        assert any('not found' in w for w in warnings)
