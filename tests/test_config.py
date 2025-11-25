"""Tests for configuration management module."""


import pytest

from dbt_meta.config import Config, _calculate_dev_schema, _parse_bool


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

    def test_sanitizes_username_dots_and_hyphens(self, monkeypatch):
        """Test that dots and hyphens are replaced with underscores."""
        monkeypatch.delenv('DBT_DEV_SCHEMA', raising=False)
        monkeypatch.setenv('USER', 'pavel.filianin')

        result = _calculate_dev_schema()

        assert result == 'personal_pavel_filianin'

    def test_sanitizes_username_with_hyphens(self, monkeypatch):
        """Test that hyphens are replaced with underscores."""
        monkeypatch.delenv('DBT_DEV_SCHEMA', raising=False)
        monkeypatch.setenv('USER', 'john-doe')

        result = _calculate_dev_schema()

        assert result == 'personal_john_doe'

    def test_sanitizes_all_special_characters(self, monkeypatch):
        """Test that all non-alphanumeric characters are sanitized."""
        monkeypatch.delenv('DBT_DEV_SCHEMA', raising=False)
        monkeypatch.setenv('USER', 'user@example.com-test_123')

        result = _calculate_dev_schema()

        # Only letters, numbers, and underscores allowed
        # @, ., - should be replaced with _
        assert result == 'personal_user_example_com_test_123'


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
        assert any('prod_table_name_strategy' in w for w in warnings)
        assert any('invalid_strategy' in w for w in warnings)

        # Should fall back to default
        assert config.prod_table_name_strategy == 'alias_or_name'

    def test_validate_warns_invalid_schema_source(self, monkeypatch):
        """Test warning for invalid DBT_PROD_SCHEMA_SOURCE."""
        monkeypatch.setenv('DBT_PROD_SCHEMA_SOURCE', 'invalid_source')

        config = Config.from_env()
        warnings = config.validate()

        assert len(warnings) >= 1
        assert any('prod_schema_source' in w for w in warnings)
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
        assert any('prod_table_name_strategy' in w for w in warnings)
        assert any('prod_schema_source' in w for w in warnings)


class TestConfigFindFile:
    """Test Config.find_config_file() discovery."""

    def test_finds_project_local_config(self, tmp_path, monkeypatch):
        """Test that ./.dbt-meta.toml is found first."""
        monkeypatch.chdir(tmp_path)

        # Create project-local config
        local_config = tmp_path / ".dbt-meta.toml"
        local_config.write_text("[manifest]\n")

        result = Config.find_config_file()

        assert result == local_config

    def test_finds_xdg_config(self, tmp_path, monkeypatch):
        """Test that ~/.config/dbt-meta/config.toml is found."""
        # Mock home directory
        fake_home = tmp_path / "home"
        fake_home.mkdir()
        monkeypatch.setenv('HOME', str(fake_home))

        # Change to different directory (no local config)
        work_dir = tmp_path / "work"
        work_dir.mkdir()
        monkeypatch.chdir(work_dir)

        # Create XDG config
        config_dir = fake_home / ".config" / "dbt-meta"
        config_dir.mkdir(parents=True)
        xdg_config = config_dir / "config.toml"
        xdg_config.write_text("[manifest]\n")

        result = Config.find_config_file()

        assert result == xdg_config

    def test_finds_home_fallback(self, tmp_path, monkeypatch):
        """Test that ~/.dbt-meta.toml is found as fallback."""
        fake_home = tmp_path / "home"
        fake_home.mkdir()
        monkeypatch.setenv('HOME', str(fake_home))

        work_dir = tmp_path / "work"
        work_dir.mkdir()
        monkeypatch.chdir(work_dir)

        # Create home fallback config (no XDG)
        home_config = fake_home / ".dbt-meta.toml"
        home_config.write_text("[manifest]\n")

        result = Config.find_config_file()

        assert result == home_config

    def test_returns_none_when_no_config(self, tmp_path, monkeypatch):
        """Test that None is returned when no config file exists."""
        fake_home = tmp_path / "home"
        fake_home.mkdir()
        monkeypatch.setenv('HOME', str(fake_home))

        work_dir = tmp_path / "work"
        work_dir.mkdir()
        monkeypatch.chdir(work_dir)

        result = Config.find_config_file()

        assert result is None


class TestConfigFromToml:
    """Test Config.from_toml() loading."""

    def test_loads_minimal_toml(self, tmp_path):
        """Test loading minimal TOML config."""
        config_file = tmp_path / "config.toml"
        config_file.write_text("""
[manifest]
prod_path = "~/custom/manifest.json"
""")

        config = Config.from_toml(config_file)

        assert config.prod_manifest_path.endswith("custom/manifest.json")
        # Defaults should be used for other fields
        assert config.fallback_dev_enabled is True

    def test_loads_full_toml(self, tmp_path):
        """Test loading comprehensive TOML config."""
        config_file = tmp_path / "config.toml"
        config_file.write_text("""
[manifest]
prod_path = "~/prod/manifest.json"
dev_path = "./dev/manifest.json"

[catalog]
prod_path = "~/prod/catalog.json"
dev_path = "./dev/catalog.json"

[fallback]
dev_enabled = false
catalog_enabled = true
bigquery_enabled = false

[dev]
schema = "custom_dev_schema"
user = "alice"

[production]
table_name_strategy = "name"
schema_source = "model"

[bigquery]
project_id = "my-project"
timeout = 30
retries = 5
location = "EU"

[database]
type = "postgresql"
host = "localhost"
port = 5432
name = "dbt_db"
username = "user"
password = "pass"

[output]
default_format = "json"
json_pretty = false
color = "never"
show_source = false

[defer]
auto_sync = false
sync_threshold = 7200
sync_command = "custom-sync.sh"
target = "prod"
""")

        config = Config.from_toml(config_file)

        # Manifest
        assert config.prod_manifest_path.endswith("prod/manifest.json")
        assert config.dev_manifest_path == "dev/manifest.json"

        # Catalog
        assert config.prod_catalog_path.endswith("prod/catalog.json")
        assert config.dev_catalog_path == "dev/catalog.json"

        # Fallback
        assert config.fallback_dev_enabled is False
        assert config.fallback_catalog_enabled is True
        assert config.fallback_bigquery_enabled is False

        # Dev
        assert config.dev_dataset == "custom_dev_schema"
        assert config.dev_user == "alice"

        # Production
        assert config.prod_table_name_strategy == "name"
        assert config.prod_schema_source == "model"

        # BigQuery
        assert config.bigquery_project_id == "my-project"
        assert config.bigquery_timeout == 30
        assert config.bigquery_retries == 5
        assert config.bigquery_location == "EU"

        # Database
        assert config.database_type == "postgresql"
        assert config.database_host == "localhost"
        assert config.database_port == 5432
        assert config.database_name == "dbt_db"
        assert config.database_username == "user"
        assert config.database_password == "pass"

        # Output
        assert config.output_default_format == "json"
        assert config.output_json_pretty is False
        assert config.output_color == "never"
        assert config.output_show_source is False

        # Defer
        assert config.defer_auto_sync is False
        assert config.defer_sync_threshold == 7200
        assert config.defer_sync_command == "custom-sync.sh"
        assert config.defer_target == "prod"

    def test_expands_tilde_in_toml_paths(self, tmp_path):
        """Test that ~ is expanded in TOML paths."""
        config_file = tmp_path / "config.toml"
        config_file.write_text("""
[manifest]
prod_path = "~/my-project/manifest.json"
""")

        config = Config.from_toml(config_file)

        assert '~' not in config.prod_manifest_path
        assert config.prod_manifest_path.startswith('/')

    def test_raises_when_explicit_path_not_found(self, tmp_path):
        """Test that error is raised when explicit path doesn't exist."""
        non_existent = tmp_path / "missing.toml"

        with pytest.raises(FileNotFoundError) as exc_info:
            Config.from_toml(non_existent)

        assert "Config file not found" in str(exc_info.value)

    def test_returns_defaults_when_no_config(self, tmp_path, monkeypatch):
        """Test that defaults are returned when no config file found (auto-detect)."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv('HOME', str(tmp_path))

        # Call without explicit path (auto-detect mode)
        config = Config.from_toml()

        # Should return defaults (no config file found)
        assert config.prod_manifest_path.endswith("dbt-state/manifest.json")
        assert config.fallback_dev_enabled is True

    def test_handles_invalid_toml(self, tmp_path):
        """Test error handling for invalid TOML."""
        config_file = tmp_path / "config.toml"
        config_file.write_text("invalid toml {{")

        with pytest.raises(ValueError) as exc_info:
            Config.from_toml(config_file)

        assert "Invalid TOML syntax" in str(exc_info.value)


class TestConfigFromConfigOrEnv:
    """Test Config.from_config_or_env() fallback logic."""

    def test_prefers_toml_over_env(self, tmp_path, monkeypatch):
        """Test that TOML config is preferred over env vars."""
        # Set env var
        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', '/env/manifest.json')

        # Create TOML config
        config_file = tmp_path / ".dbt-meta.toml"
        config_file.write_text("""
[manifest]
prod_path = "/toml/manifest.json"
""")

        monkeypatch.chdir(tmp_path)

        config = Config.from_config_or_env()

        # Should use TOML value
        assert config.prod_manifest_path == "/toml/manifest.json"

    def test_falls_back_to_env_when_no_toml(self, tmp_path, monkeypatch):
        """Test fallback to env vars when no TOML config."""
        from unittest.mock import patch

        monkeypatch.setenv('DBT_PROD_MANIFEST_PATH', '/env/manifest.json')
        monkeypatch.chdir(tmp_path)

        # Mock find_config_file to return None (no TOML file found)
        with patch.object(Config, 'find_config_file', return_value=None):
            config = Config.from_config_or_env()

            # Should use env var
            assert config.prod_manifest_path == "/env/manifest.json"

    def test_uses_defaults_when_no_config(self, tmp_path, monkeypatch):
        """Test defaults when neither TOML nor env vars."""
        from unittest.mock import patch

        # Clear env vars
        for var in ['DBT_PROD_MANIFEST_PATH', 'DBT_DEV_MANIFEST_PATH']:
            monkeypatch.delenv(var, raising=False)

        monkeypatch.setenv('USER', 'testuser')
        monkeypatch.chdir(tmp_path)

        # Mock find_config_file to return None
        with patch.object(Config, 'find_config_file', return_value=None):
            config = Config.from_config_or_env()

            # Should use defaults
            assert config.prod_manifest_path.endswith("dbt-state/manifest.json")
            assert config.dev_dataset == "personal_testuser"


class TestConfigToDict:
    """Test Config.to_dict() method."""

    def test_converts_to_dict(self, monkeypatch):
        """Test that config is properly converted to dict."""
        monkeypatch.delenv('DBT_DEV_SCHEMA', raising=False)
        monkeypatch.setenv('USER', 'alice')

        config = Config.from_env()
        result = config.to_dict()

        assert isinstance(result, dict)
        assert 'prod_manifest_path' in result
        assert 'dev_dataset' in result
        assert 'fallback_dev_enabled' in result

        # New fields should be present
        assert 'bigquery_timeout' in result
        assert 'output_default_format' in result
        assert 'database_type' in result

    def test_dict_contains_all_fields(self, tmp_path):
        """Test that all config fields are in dict."""
        config_file = tmp_path / "config.toml"
        config_file.write_text("""
[bigquery]
project_id = "test-project"

[output]
default_format = "json"
""")

        config = Config.from_toml(config_file)
        result = config.to_dict()

        # Check all major field groups are present
        assert result['bigquery_project_id'] == "test-project"
        assert result['output_default_format'] == "json"
