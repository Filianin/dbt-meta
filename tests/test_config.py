"""Tests for configuration management module."""

import pytest
import os
from pathlib import Path
from dbt_meta.config import Config, _parse_bool, _calculate_dev_schema


class TestParseBool:
    """Test boolean parsing helper."""

    def test_parse_true_values(self):
        """Test that 'true', '1', 'yes' parse to True."""
        assert _parse_bool('true') is True
        assert _parse_bool('TRUE') is True
        assert _parse_bool('True') is True
        assert _parse_bool('1') is True
        assert _parse_bool('yes') is True
        assert _parse_bool('YES') is True

    def test_parse_false_values(self):
        """Test that other values parse to False."""
        assert _parse_bool('false') is False
        assert _parse_bool('FALSE') is False
        assert _parse_bool('0') is False
        assert _parse_bool('no') is False
        assert _parse_bool('') is False
        assert _parse_bool('anything') is False


class TestCalculateDevSchema:
    """Test dev schema calculation."""

    def test_uses_dbt_dev_dataset_if_set(self, monkeypatch):
        """Test that DBT_DEV_SCHEMA takes priority."""
        monkeypatch.setenv('DBT_DEV_SCHEMA', 'custom_dev_schema')
        monkeypatch.setenv('USER', 'alice')

        result = _calculate_dev_schema()

        assert result == 'custom_dev_schema'

    def test_defaults_to_personal_username(self, monkeypatch):
        """Test default naming: personal_{username}."""
        monkeypatch.delenv('DBT_DEV_SCHEMA', raising=False)
        monkeypatch.setenv('USER', 'alice')

        result = _calculate_dev_schema()

        assert result == 'personal_alice'

    def test_handles_missing_user_env(self, monkeypatch):
        """Test fallback when USER env not set."""
        monkeypatch.delenv('DBT_DEV_SCHEMA', raising=False)
        monkeypatch.delenv('USER', raising=False)

        result = _calculate_dev_schema()

        assert result == 'personal_user'


class TestConfigFromEnv:
    """Test Config.from_env() loading."""

    def test_loads_defaults_when_no_env_vars(self, monkeypatch):
        """Test that defaults are used when no env vars set."""
        # Clear all relevant env vars
        for var in ['DBT_PROD_MANIFEST_PATH', 'DBT_DEV_MANIFEST_PATH',
                    'DBT_FALLBACK_TARGET', 'DBT_FALLBACK_BIGQUERY',
                    'DBT_DEV_SCHEMA', 'DBT_PROD_TABLE_NAME', 'DBT_PROD_SCHEMA_SOURCE']:
            monkeypatch.delenv(var, raising=False)

        monkeypatch.setenv('USER', 'alice')

        config = Config.from_env()

        # Check defaults (Path.expanduser() normalizes ./ to nothing)
        assert config.prod_manifest_path.endswith('dbt-state/manifest.json')
        assert config.dev_manifest_path == 'target/manifest.json'
        assert config.fallback_dev_enabled is True
        assert config.fallback_bigquery_enabled is True
        assert config.dev_dataset == 'personal_alice'
        assert config.prod_table_name_strategy == 'alias_or_name'
        assert config.prod_schema_source == 'config_or_model'

    def test_loads_custom_values_from_env(self, monkeypatch, tmp_path):
        """Test that custom env vars override defaults."""
        prod_path = tmp_path / "custom_prod.json"
        dev_path = tmp_path / "custom_dev.json"

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_path))
        monkeypatch.setenv('DBT_DEV_MANIFEST_PATH', str(dev_path))
        monkeypatch.setenv('DBT_FALLBACK_TARGET', 'false')
        monkeypatch.setenv('DBT_FALLBACK_BIGQUERY', '0')
        monkeypatch.setenv('DBT_DEV_SCHEMA', 'my_dev_dataset')
        monkeypatch.setenv('DBT_PROD_TABLE_NAME', 'name')
        monkeypatch.setenv('DBT_PROD_SCHEMA_SOURCE', 'model')

        config = Config.from_env()

        assert config.prod_manifest_path == str(prod_path)
        assert config.dev_manifest_path == str(dev_path)
        assert config.fallback_dev_enabled is False
        assert config.fallback_bigquery_enabled is False
        assert config.dev_dataset == 'my_dev_dataset'
        assert config.prod_table_name_strategy == 'name'
        assert config.prod_schema_source == 'model'

    def test_expands_tilde_in_paths(self, monkeypatch):
        """Test that ~ is expanded to home directory."""
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', '~/custom/manifest.json')

        config = Config.from_env()

        assert config.prod_manifest_path.startswith('/')
        assert '~' not in config.prod_manifest_path
        assert config.prod_manifest_path.endswith('custom/manifest.json')


class TestConfigValidation:
    """Test configuration validation."""

    def test_validate_returns_empty_for_valid_config(self, tmp_path, monkeypatch):
        """Test that valid config returns no warnings."""
        # Create production manifest
        prod_manifest = tmp_path / ".dbt-state" / "manifest.json"
        prod_manifest.parent.mkdir(parents=True)
        prod_manifest.write_text('{"metadata": {}}')

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(prod_manifest))
        monkeypatch.setenv('DBT_PROD_TABLE_NAME', 'alias_or_name')
        monkeypatch.setenv('DBT_PROD_SCHEMA_SOURCE', 'config_or_model')

        config = Config.from_env()
        warnings = config.validate()

        assert warnings == []

    def test_validate_warns_invalid_table_name_strategy(self, monkeypatch):
        """Test warning for invalid DBT_PROD_TABLE_NAME."""
        monkeypatch.setenv('DBT_PROD_TABLE_NAME', 'invalid_strategy')

        config = Config.from_env()
        warnings = config.validate()

        assert len(warnings) >= 1
        assert any('DBT_PROD_TABLE_NAME' in w for w in warnings)
        assert any('invalid_strategy' in w for w in warnings)

        # Should fall back to default
        assert config.prod_table_name_strategy == 'alias_or_name'

    def test_validate_warns_invalid_schema_source(self, monkeypatch):
        """Test warning for invalid DBT_PROD_SCHEMA_SOURCE."""
        monkeypatch.setenv('DBT_PROD_SCHEMA_SOURCE', 'invalid_source')

        config = Config.from_env()
        warnings = config.validate()

        assert len(warnings) >= 1
        assert any('DBT_PROD_SCHEMA_SOURCE' in w for w in warnings)
        assert any('invalid_source' in w for w in warnings)

        # Should fall back to default
        assert config.prod_schema_source == 'config_or_model'

    def test_validate_warns_missing_prod_manifest(self, monkeypatch, tmp_path):
        """Test warning when production manifest doesn't exist."""
        non_existent = tmp_path / "missing" / "manifest.json"
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', str(non_existent))

        config = Config.from_env()
        warnings = config.validate()

        assert len(warnings) >= 1
        assert any('Production manifest not found' in w for w in warnings)
        assert any(str(non_existent) in w for w in warnings)

    def test_validate_handles_multiple_issues(self, monkeypatch):
        """Test that multiple validation issues are all reported."""
        monkeypatch.setenv('DBT_PROD_TABLE_NAME', 'bad_value')
        monkeypatch.setenv('DBT_PROD_SCHEMA_SOURCE', 'bad_source')

        config = Config.from_env()
        warnings = config.validate()

        # Should have warnings for both issues + missing manifest
        assert len(warnings) >= 2
        assert any('DBT_PROD_TABLE_NAME' in w for w in warnings)
        assert any('DBT_PROD_SCHEMA_SOURCE' in w for w in warnings)
